(ns afrolabs.components.kafka.consumer-registry
  "Registry of which process owns each [consumer-group-id topic partition].

  The component is a rebalance listener: pass it in a subscribe strategy's
  `:consumer-rebalance-listeners`. It advertises every partition assigned to
  those consumers into a compacted topic, keyed by
  [consumer-group-id topic partition], and holds all advertisements in a
  ktable.

  Advertisements are never tombstoned. A new owner overwrites its
  predecessor. Each process republishes its whole ownership every 50-60
  minutes; readers drop entries older than 90 minutes, so an entry not
  refreshed in that window belongs to a process that is gone.

  Handover fence: sends and ownership changes share one lock, and the revoke
  callback flushes the producer. On a revoke, every record a process sent for
  a partition is therefore written before the partition's next owner is
  assigned.

  Self-healing: a lost partition or a zombie can still write after the new
  owner. Each process watches the registry and republishes when an entry it
  owns was overwritten after its own acknowledged record by a writer of an
  equal or older generation. A newer generation is deferred to. Two live
  owners never share a generation, because static group membership is
  refused.

  Init blocks until the ktable has caught up once. Deref returns
  {[consumer-group-id topic partition] advertisement}, without entries older
  than 90 minutes."
  (:require [afrolabs.components :as -comp]
            [afrolabs.components.health :as -health]
            [afrolabs.components.kafka :as -kafka]
            [afrolabs.components.time :as -time]
            [afrolabs.prometheus :as -prom]
            [clojure.core.async :as csp]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [iapetos.core :as prom]
            [java-time.api :as t]
            [taoensso.timbre :as log])
  (:import [afrolabs.components IHaltable]
           [clojure.lang IDeref]
           [org.apache.kafka.clients.consumer Consumer ConsumerGroupMetadata]
           [org.apache.kafka.clients.producer Producer]
           [org.apache.kafka.common TopicPartition]))

(def ^:private registry-retention-ms (* 90 60 1000))
(def ^:private registry-segment-ms (* 60 60 1000))
(def ^:private registry-topic-retention-ms (* 24 60 60 1000))
(def ^:private catch-up-timeout-ms 10000)

(def ^:private producer-config
  ["acks"                "all"
   "enable.idempotence"  "true"
   "linger.ms"           "0"
   "request.timeout.ms"  "10000"
   "delivery.timeout.ms" "30000"
   ;; bounds how long `.send` can hold the fence lock
   "max.block.ms"        "10000"])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; metrics

(-prom/register-metric (prom/counter ::publish-attempts
                                     {:description "Publishes of this process's ownership snapshot, by outcome."
                                      :labels      [:outcome]}))

(-prom/register-metric (prom/gauge ::last-publish-success
                                   {:description "Epoch seconds of the last successful publish."}))

(-prom/register-metric (prom/counter ::corrections
                                     {:description "Owned registry entries found overwritten by another writer, by outcome."
                                      :labels      [:outcome]}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ownership

(defn- tp->vec
  [^TopicPartition tp]
  [(.topic tp) (.partition tp)])

(defn- assign
  "Return `ownership` with `partitions` added to `consumer`'s owned set, and
  its group metadata updated."
  [ownership consumer ^ConsumerGroupMetadata group-metadata partitions]
  (update ownership consumer
          (fn [o]
            (-> o
                (assoc :group-id   (.groupId group-metadata)
                       :member-id  (.memberId group-metadata)
                       :generation (.generationId group-metadata))
                (update :owned (fnil into #{}) (map tp->vec) partitions)))))

(defn- disown
  "Return `ownership` without `partitions` for `consumer`. Drop the consumer
  once it owns nothing."
  [ownership consumer partitions]
  (let [ownership (update-in ownership [consumer :owned]
                             #(set/difference (or % #{}) (into #{} (map tp->vec) partitions)))]
    (if (seq (get-in ownership [consumer :owned]))
      ownership
      (dissoc ownership consumer))))

(defn- ownership->records
  "Return one producer message per partition in `ownership`."
  [registry-topic advertisement ownership]
  (for [[_ {:keys [group-id member-id generation owned]}] ownership
        [topic partition] owned]
    {:topic registry-topic
     :key   [group-id topic partition]
     :value (assoc advertisement
                   :member-id  member-id
                   :generation generation)}))

(defn- corrections
  "Return {:correct [key ...] :defer [key ...]} for owned keys whose entry
  differs from what this process would write, and was written after this
  process's last acknowledged record for the key.

  A differing entry with a higher generation is deferred to; any other is to
  be corrected. `expected` is {key value} of the records this process would
  write now, `acked` is {key offset}, `entries` is the registry ktable value."
  [expected acked entries]
  (reduce-kv (fn [acc k want]
               (let [e            (get entries k)
                     acked-offset (get acked k)
                     offset       (:offset (meta e))]
                 (cond
                   (or (nil? acked-offset) (nil? e) (nil? offset)
                       (<= offset acked-offset) (= want e))
                   acc

                   (let [g (:generation e)]
                     (and (number? g) (> g (:generation want))))
                   (update acc :defer conj k)

                   :else
                   (update acc :correct conj k))))
             {:correct [] :defer []}
             expected))

(defn- live-advertisements
  "Return `advertisements` without entries whose record timestamp is more than
  `registry-retention-ms` before `now`. Entries without a timestamp are kept."
  [now advertisements]
  (let [cutoff (t/- now (t/millis registry-retention-ms))]
    (into {}
          (remove (fn [[_ v]]
                    (when-let [ts (:timestamp (meta v))]
                      (t/before? ts cutoff))))
          advertisements)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; publishing

(defn- refresh-delay-ms
  []
  (+ (* 50 60 1000)
     (rand-int (* 10 60 1000))))

(defn- retry-delay-ms
  [failures]
  (min 60000 (* 5000 failures)))

(defn- ensure-producer!
  "Return the registry's producer, creating it on first use. A failed creation
  is not cached; the next call tries again."
  [{:keys [producer make-producer]}]
  (or @producer
      (reset! producer (make-producer))))

(defn- publish-snapshot!
  "Send one record per owned partition and wait for every acknowledgement.

  The snapshot and its sends happen under the fence lock. Set `acked` to the
  offset of each acknowledged record, keeping only the snapshot's keys.
  Return true iff every record was acknowledged; an empty snapshot is a
  success and creates no producer."
  [{:keys [fence ownership registry-topic advertisement acked] :as registry}]
  (try
    (let [msgs     (locking fence
                     (let [msgs (mapv #(assoc % :delivered-ch (csp/promise-chan))
                                      (ownership->records registry-topic advertisement @ownership))]
                       (when (seq msgs)
                         (-kafka/produce! (ensure-producer! registry) msgs))
                       msgs))
          results  (mapv (fn [{:keys [delivered-ch key]}]
                           [key (csp/<!! delivered-ch)])
                         msgs)
          failures (filter (fn [[_ r]]
                             (or (nil? r)
                                 (instance? Throwable r)))
                           results)]
      (swap! acked #(into (select-keys % (map :key msgs))
                          (keep (fn [[k r]]
                                  (when-let [o (and (map? r) (:offset r))]
                                    [k o])))
                          results))
      (if (seq failures)
        (let [[k ex] (first failures)]
          (log/with-context+ {:failed-records (count failures)
                              :records        (count msgs)
                              :key            k}
            (log/warn ex "Consumer registry advertisement was not acknowledged."))
          false)
        true))
    (catch Throwable t
      (log/warn t "Consumer registry publish failed.")
      false)))

(defn- start-publish-loop!
  "Publish on every ownership change and every 50-60 minutes; after a
  failure, retry with backoff until a publish succeeds.

  Return a channel that closes when the loop exits, after `stop-ch` closes."
  [{:keys [changed-ch stop-ch] :as registry}]
  (csp/go-loop [wait     (refresh-delay-ms)
                failures 0]
    (let [[_ ch] (csp/alts! [stop-ch changed-ch (csp/timeout wait)] :priority true)]
      (when-not (= ch stop-ch)
        (if (csp/<! (csp/io-thread (publish-snapshot! registry)))
          (do (prom/inc (get-counter-publish-attempts {:outcome "ok"}))
              (prom/set (get-gauge-last-publish-success) (quot (System/currentTimeMillis) 1000))
              (recur (refresh-delay-ms) 0))
          (let [failures (inc failures)]
            (prom/inc (get-counter-publish-attempts {:outcome "failed"}))
            (recur (retry-delay-ms failures) failures)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; self-healing

(defn- heal!
  "Wait until the ktable has applied `batch`, then request a publish if an
  owned entry needs correcting. Log deferrals."
  [{:keys [ktable registry-topic advertisement ownership acked changed-ch]} batch]
  (try
    (if (= ::timeout (-kafka/ktable-wait-for-catchup ktable
                                                     (-kafka/msgs->topic-partition-maxoffsets batch)
                                                     (t/millis catch-up-timeout-ms)
                                                     ::timeout))
      (log/warn "Consumer registry ktable did not catch up; skipping correction check.")
      (let [expected                (into {}
                                          (map (juxt :key :value))
                                          (ownership->records registry-topic advertisement @ownership))
            {:keys [correct defer]} (corrections expected @acked (get @ktable registry-topic {}))]
        (when (seq defer)
          (prom/inc (get-counter-corrections {:outcome "deferred"}) (count defer))
          (log/with-context+ {:keys  (count defer)
                              :key   (first defer)
                              :entry (get-in @ktable [registry-topic (first defer)])}
            (log/warn "Consumer registry entries for owned partitions name a newer generation; this consumer may have been removed from its group.")))
        (when (seq correct)
          (prom/inc (get-counter-corrections {:outcome "corrected"}) (count correct))
          (log/with-context+ {:keys  (count correct)
                              :key   (first correct)
                              :entry (get-in @ktable [registry-topic (first correct)])}
            (log/info "Consumer registry entries for owned partitions were overwritten; republishing."))
          (csp/put! changed-ch true))))
    (catch Throwable t
      (log/warn t "Consumer registry correction check failed."))))

(defn- start-heal-loop!
  "Run `heal!` on every batch the registry ktable consumes.

  Return {:subscription handle, :done channel}. `:done` closes when the loop
  exits, after `stop-ch` closes."
  [{:keys [ktable stop-ch] :as registry}]
  (let [batches      (csp/chan)
        subscription (-kafka/ktable-subscribe ktable {:receiving-ch batches})]
    {:subscription subscription
     :done         (csp/go-loop []
                     (let [[batch ch] (csp/alts! [stop-ch batches] :priority true)]
                       (when (and (not= ch stop-ch) batch)
                         (csp/<! (csp/io-thread (heal! registry batch)))
                         (recur))))}))

(defn- refuse-static-membership!
  "Throw if `group-metadata` has a group instance id.

  Static members can share a generation with the instance they replace, so
  neither could win a correction."
  [^ConsumerGroupMetadata group-metadata]
  (when-let [instance-id (.orElse (.groupInstanceId group-metadata) nil)]
    (throw (ex-info "Consumer registry does not support static group membership (group.instance.id)."
                    {:group-id          (.groupId group-metadata)
                     :group-instance-id instance-id}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; component

(s/def ::registry-topic (s/and string? seq))
(s/def ::advertised-url (s/and string? seq))
(s/def ::registry-cfg (s/keys :req-un [::-kafka/bootstrap-server
                                       ::registry-topic
                                       ::advertised-url
                                       ::-time/clock
                                       ::-health/service-health-trip-switch]
                              :opt-un [::-kafka/strategies
                                       ::-kafka/admin-client]))

(defn- assert-registry-topic!
  [{:keys [admin-client bootstrap-server strategies registry-topic]}]
  (let [ac (or admin-client
               (-kafka/make-admin-client {:bootstrap-server bootstrap-server
                                          :strategies       strategies}))]
    (try
      (-kafka/init-fn-ktable-asserter ::registry-topic-asserter
                                      {:admin-client                  ac
                                       :topic-name-providers          [(reify -kafka/ITopicNameProvider
                                                                         (get-topic-names [_] [registry-topic]))]
                                       :nr-of-partitions              1
                                       :ktable-compaction-policy      "compact,delete"
                                       :topic-config                  {"retention.ms" registry-topic-retention-ms
                                                                       "segment.ms"   registry-segment-ms}
                                       :update-topics-with-bad-config true})
      (finally
        (when-not admin-client
          (-comp/halt ac))))))

(defn- make-registry-ktable
  "Return a ktable over `registry-topic`. Its `retention-ms` makes it seek
  back that far on start, and expire entries not refreshed within it."
  [{:keys [bootstrap-server strategies registry-topic clock service-health-trip-switch]}]
  (-kafka/init-fn-ktable ::registry-ktable
                         {:ktable-id                  (str "consumer-registry-" registry-topic)
                          :bootstrap-server           bootstrap-server
                          :service-health-trip-switch service-health-trip-switch
                          :clock                      clock
                          :retention-ms               registry-retention-ms
                          :caught-up-once?            true
                          :strategies                 (concat strategies
                                                              [(-kafka/SubscribeWithTopicsCollection [registry-topic])
                                                               (-kafka/EdnSerializer :consumer :both)])}))

(defn- flush-producer!
  [producer]
  (when-let [p @producer]
    (try (.flush ^Producer (-kafka/get-producer p))
         (catch Throwable t
           (log/warn t "Consumer registry producer flush failed during handover.")))))

(-comp/defcomponent {::-comp/config-spec ::registry-cfg
                     ::-comp/ig-kw       ::component}
  [{:as   cfg
    :keys [bootstrap-server registry-topic advertised-url clock]}]
  (let [cfg        (update cfg :strategies #(or % []))
        process-id (random-uuid)
        _          (assert-registry-topic! cfg)
        ktable     (make-registry-ktable cfg)
        _          @ktable
        registry   {:fence          (Object.)
                    :ktable         ktable
                    :ownership      (atom {})
                    :acked          (atom {})
                    :producer       (atom nil)
                    :make-producer  #(-kafka/make-producer
                                      {:bootstrap-server bootstrap-server
                                       :strategies       (concat (:strategies cfg)
                                                                 [(-kafka/EdnSerializer :producer :both)
                                                                  (-kafka/ClientId (str "consumer-registry-" process-id))
                                                                  (apply -kafka/AdhocConfig producer-config)])})
                    :registry-topic registry-topic
                    :advertisement  {:url        advertised-url
                                     :process-id process-id}
                    :changed-ch     (csp/chan (csp/sliding-buffer 1))
                    :stop-ch        (csp/chan)}
        {:keys [fence ownership producer changed-ch stop-ch]} registry
        heal-loop     (start-heal-loop! registry)
        publish-loop  (start-publish-loop! registry)]

    (log/with-context+ {:registry-topic registry-topic
                        :process-id     process-id
                        :advertised-url advertised-url}
      (log/info "Consumer registry started."))

    (reify
      -kafka/IConsumerAwareRebalanceListener
      (on-partitions-assigned [_ consumer partitions]
        (let [group-metadata (.groupMetadata ^Consumer consumer)]
          (refuse-static-membership! group-metadata)
          (locking fence
            (swap! ownership assign consumer group-metadata partitions)))
        (csp/put! changed-ch true))

      (on-partitions-revoked [_ consumer partitions]
        (locking fence
          (swap! ownership disown consumer partitions))
        (csp/put! changed-ch true)
        (flush-producer! producer))

      (on-partitions-lost [_ consumer partitions]
        (locking fence
          (swap! ownership disown consumer partitions))
        (csp/put! changed-ch true))

      IDeref
      (deref [_]
        (live-advertisements (-time/get-current-instant clock)
                             (get @ktable registry-topic {})))

      IHaltable
      (halt [_]
        (csp/close! stop-ch)
        (csp/<!! publish-loop)
        (csp/<!! (:done heal-loop))
        (-kafka/ktable-unsubscribe ktable (:subscription heal-loop))
        (-comp/halt ktable)
        (when-let [p @producer]
          (-comp/halt p))))))
