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
  than 90 minutes.

  Queries: `query` sends a nippy-encoded value over HTTP to every process
  advertising a consumer group, and returns the answers. The component is an
  `IHttpRequestHandler` serving `POST {query-path}/{group-id}`; it hands each
  query to every `IConsumerGroupQueryHandler` registered for that group, in
  this process. The `ConsumerRegistryQueryHandler` strategy registers a
  handler under its consumer's group id."
  (:require [afrolabs.components :as -comp]
            [afrolabs.components.health :as -health]
            [afrolabs.components.http :as -http]
            [afrolabs.components.kafka :as -kafka]
            [afrolabs.components.time :as -time]
            [afrolabs.prometheus :as -prom]
            [clojure.core.async :as csp]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [iapetos.core :as prom]
            [java-time.api :as t]
            [org.httpkit.client :as hk]
            [taoensso.nippy :as nippy]
            [taoensso.timbre :as log])
  (:import [afrolabs.components IHaltable]
           [clojure.lang IDeref]
           [java.io InputStream]
           [java.net URLDecoder URLEncoder]
           [java.nio.charset StandardCharsets]
           [java.security MessageDigest]
           [org.apache.kafka.clients.consumer Consumer ConsumerGroupMetadata]
           [org.apache.kafka.clients.producer Producer]
           [org.apache.kafka.common TopicPartition]))

(def ^:private registry-retention-ms (* 90 60 1000))
(def ^:private registry-segment-ms (* 60 60 1000))
(def ^:private registry-topic-retention-ms (* 24 60 60 1000))
(def ^:private catch-up-timeout-ms 10000)
(def ^:private default-query-timeout-ms 20000)
(def ^:private correlation-id-header "X-ConsumerGroupQuery-CorrelationId")
(def ^:private nippy-content-type "application/x-nippy")
(def ^:private no-answer {:status 404 :body nil})

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

(-prom/register-metric (prom/counter ::queries-sent
                                     {:description "Consumer group queries sent by this process."}))

(-prom/register-metric (prom/counter ::query-responses
                                     {:description "Responses to this process's consumer group queries, by HTTP status, `error` or `timeout`."
                                      :labels      [:status]}))

(-prom/register-metric (prom/counter ::queries-handled
                                     {:description "Consumer group queries received by this process, by outcome."
                                      :labels      [:outcome]}))

(-prom/register-metric (prom/counter ::query-handler-errors
                                     {:description "Query handler calls that threw."}))

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
;; queries

(defprotocol IConsumerGroupQueryHandler
  :extend-via-metadata true
  (handle-query [_ group-id query]
    "Return the answer to `query`, or nil when this handler has none."))

(defprotocol IConsumerGroupQueries
  (register-query-handler! [registry group-id handler]
    "Register `handler`, an `IConsumerGroupQueryHandler`, for queries to
    `group-id` received by this process. Return a handle for
    `deregister-query-handler!`. Any number of handlers may share a group.")
  (deregister-query-handler! [registry handle]
    "Remove the handler registered under `handle`.")
  (query [registry opts]
    "Send `(:query opts)` to every process advertising `(:group-id opts)` and
    wait up to `(:timeout-ms opts)`, default the registry's
    `:query-timeout-ms`.

    Return a vector of every answer received in time; [] if none. Blocks."))

(defn- query-handler?
  [x]
  (or (satisfies? IConsumerGroupQueryHandler x)
      (contains? (meta x) `handle-query)))

(defn- route-group-id
  "Return the group id `req` addresses, when it is a POST to
  {query-path}/{url-encoded group-id}; else nil."
  [query-path {:keys [uri request-method]}]
  (let [prefix (str query-path "/")]
    (when (and (= :post request-method)
               (string? uri)
               (str/starts-with? uri prefix))
      (let [encoded (subs uri (count prefix))]
        (when (and (seq encoded)
                   (not (str/includes? encoded "/")))
          (try (URLDecoder/decode encoded StandardCharsets/UTF_8)
               (catch IllegalArgumentException _ nil)))))))

(defn- authorization-problem
  "Return nil when `authorization` is `Bearer <token>`, else :missing or
  :mismatch."
  [token authorization]
  (cond
    (nil? authorization)
    :missing

    (MessageDigest/isEqual (.getBytes (str "Bearer " token) StandardCharsets/UTF_8)
                           (.getBytes ^String authorization StandardCharsets/UTF_8))
    nil

    :else
    :mismatch))

(defn- thaw-body
  "Return the value nippy-encoded in `body`, a byte array or InputStream.
  Throw when it is empty or not nippy."
  [body]
  (let [bs (cond
             (bytes? body)                body
             (instance? InputStream body) (.readAllBytes ^InputStream body))]
    (when (or (nil? bs) (zero? (alength ^bytes bs)))
      (throw (ex-info "Empty body." {})))
    (nippy/thaw bs)))

(defn- group-handlers
  [handlers group-id]
  (keep (fn [{g :group-id h :handler}]
          (when (= g group-id) h))
        (vals handlers)))

(defn- answers
  "Return the non-nil answers of `handlers` to `query`. Log and skip handlers
  that throw."
  [handlers group-id query]
  (into []
        (keep (fn [h]
                (try (handle-query h group-id query)
                     (catch Throwable t
                       (prom/inc (get-counter-query-handler-errors))
                       (log/error t "Consumer group query handler threw.")
                       nil))))
        handlers))

(defn- handled!
  [outcome]
  (prom/inc (get-counter-queries-handled {:outcome outcome})))

(defn- answer-query-request
  "Return the ring response to a query request, or nil when `req` is not one.

  `query-handlers` is an atom of {handle {:group-id :handler}}."
  [{:keys [query-path query-token query-handlers]} {:keys [headers body] :as req}]
  (when-let [group-id (route-group-id query-path req)]
    (log/with-context+ {:group-id group-id}
      (if-let [problem (authorization-problem query-token (get headers "authorization"))]
        (do (handled! "unauthorized")
            (log/warn (case problem
                        :missing  "Consumer group query without an Authorization header."
                        :mismatch "Consumer group query with a wrong token."))
            {:status 401 :body nil})
        (if-let [correlation-id (not-empty (get headers (str/lower-case correlation-id-header)))]
          (log/with-context+ {:correlation-id correlation-id}
            (let [query (try (thaw-body body)
                             (catch Throwable t
                               (log/warn t "Consumer group query body is not nippy.")
                               ::unreadable))]
              (if (= ::unreadable query)
                (do (handled! "bad-request")
                    {:status 400 :body nil})
                (let [handlers (group-handlers @query-handlers group-id)]
                  (if-not (seq handlers)
                    (do (handled! "no-handler")
                        (log/error "Consumer group query for a group without a registered query handler.")
                        no-answer)
                    (let [as (answers handlers group-id query)]
                      (if (seq as)
                        (do (handled! "answered")
                            {:status  200
                             :headers {"Content-Type" nippy-content-type}
                             :body    (nippy/freeze as)})
                        (do (handled! "no-answer")
                            no-answer))))))))
          (do (handled! "bad-request")
              (log/warn (str "Consumer group query without an " correlation-id-header " header."))
              {:status 400 :body nil}))))))

(defn- query-url
  [advertised-url query-path group-id]
  (str (str/replace advertised-url #"/+$" "")
       query-path
       "/"
       (URLEncoder/encode ^String group-id StandardCharsets/UTF_8)))

(defn- group-urls
  "Return the distinct advertised urls for `group-id`."
  [advertisements group-id]
  (into (sorted-set)
        (keep (fn [[[g] {:keys [url]}]]
                (when (= g group-id) url)))
        advertisements))

(defn- response-answers
  "Return the answers in an http-kit `response` from `url`, or nil. Log every
  response other than 200 and 404."
  [url {:keys [status body error]}]
  (prom/inc (get-counter-query-responses {:status (if error "error" (str status))}))
  (log/with-context+ {:url url :status status}
    (cond
      error
      (do (log/warn error "Consumer group query request failed.")
          nil)

      (= 200 status)
      (try (let [as (thaw-body body)]
             (if (sequential? as)
               as
               (log/warn "Consumer group query answer is not a vector.")))
           (catch Throwable t
             (log/warn t "Consumer group query answer is not nippy.")
             nil))

      (= 404 status)
      nil

      (<= 500 status 599)
      (log/error "Consumer group query failed on the receiving process.")

      :else
      (log/warn "Consumer group query was rejected."))))

(defn- send-query!
  "Send `query` to every url advertising `group-id`; return the answers
  received within the timeout.

  `advertisements` is a 0-arity fn returning the registry's live entries."
  [{:keys [query-path query-token query-timeout-ms advertisements]}
   {:keys [group-id query timeout-ms]}]
  (let [timeout-ms     (or timeout-ms query-timeout-ms)
        correlation-id (str (random-uuid))
        urls           (group-urls (advertisements) group-id)]
    (log/with-context+ {:group-id       group-id
                        :correlation-id correlation-id}
      (prom/inc (get-counter-queries-sent))
      (if-not (seq urls)
        []
        (let [body      (nippy/freeze query)
              responses (csp/chan (count urls))
              deadline  (csp/timeout timeout-ms)]
          (doseq [url urls]
            (hk/request {:method          :post
                         :url             (query-url url query-path group-id)
                         :headers         {"Authorization"       (str "Bearer " query-token)
                                           "Content-Type"        nippy-content-type
                                           correlation-id-header correlation-id}
                         :body            body
                         :as              :byte-array
                         :timeout         timeout-ms
                         :connect-timeout timeout-ms}
                        #(csp/put! responses [url %])))
          (loop [pending urls
                 acc     []]
            (if-not (seq pending)
              acc
              (let [[v ch] (csp/alts!! [responses deadline] :priority true)]
                (if (= ch deadline)
                  (do (prom/inc (get-counter-query-responses {:status "timeout"}) (count pending))
                      (log/with-context+ {:urls pending}
                        (log/warn "Consumer group query targets did not answer within the timeout."))
                      acc)
                  (let [[url response] v]
                    (recur (disj pending url)
                           (into acc (response-answers url response)))))))))))))

(-kafka/defstrategy ConsumerRegistryQueryHandler
  "Return a consumer strategy that registers `handler`, an
  `IConsumerGroupQueryHandler`, with `registry` under the consumer's group id
  before its first poll, and deregisters it when the consumer stops."
  [registry handler]
  (when-not (satisfies? IConsumerGroupQueries registry)
    (throw (ex-info "ConsumerRegistryQueryHandler requires a consumer registry component." {})))
  (when-not (query-handler? handler)
    (throw (ex-info "ConsumerRegistryQueryHandler requires a handler that implements IConsumerGroupQueryHandler." {})))
  (let [handle (atom nil)]
    (reify
      -kafka/IConsumerPostInitHook
      (post-init-hook [_ consumer]
        (reset! handle (register-query-handler! registry
                                                (.groupId (.groupMetadata ^Consumer consumer))
                                                handler)))

      -kafka/IShutdownHook
      (shutdown-hook [_ _]
        (when-let [h @handle]
          (deregister-query-handler! registry h)
          (reset! handle nil))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; component

(s/def ::registry-topic (s/and string? seq))
(s/def ::advertised-url (s/and string? seq))
(s/def ::query-path (s/and string?
                           #(str/starts-with? % "/")
                           #(not (str/ends-with? % "/"))))
(s/def ::query-token (s/and string? seq))
(s/def ::query-timeout-ms pos-int?)
(s/def ::registry-cfg (s/keys :req-un [::-kafka/bootstrap-server
                                       ::registry-topic
                                       ::advertised-url
                                       ::query-path
                                       ::query-token
                                       ::-time/clock
                                       ::-health/service-health-trip-switch]
                              :opt-un [::-kafka/strategies
                                       ::-kafka/admin-client
                                       ::query-timeout-ms]))

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
    :keys [bootstrap-server registry-topic advertised-url clock
           query-path query-token query-timeout-ms]
    :or   {query-timeout-ms default-query-timeout-ms}}]
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
        heal-loop      (start-heal-loop! registry)
        publish-loop   (start-publish-loop! registry)
        advertisements #(live-advertisements (-time/get-current-instant clock)
                                             (get @ktable registry-topic {}))
        query-handlers (atom {})
        query-ctx      {:query-path       query-path
                        :query-token      query-token
                        :query-timeout-ms query-timeout-ms
                        :query-handlers   query-handlers
                        :advertisements   advertisements}]

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
      (deref [_] (advertisements))

      IConsumerGroupQueries
      (register-query-handler! [_ group-id handler]
        (when-not (query-handler? handler)
          (throw (ex-info "Query handler must implement IConsumerGroupQueryHandler."
                          {:group-id group-id})))
        (let [handle (random-uuid)]
          (swap! query-handlers assoc handle {:group-id group-id :handler handler})
          handle))
      (deregister-query-handler! [_ handle]
        (swap! query-handlers dissoc handle)
        nil)
      (query [_ opts]
        (send-query! query-ctx opts))

      -http/IHttpRequestHandler
      (handle-http-request [_ req]
        (answer-query-request query-ctx req))

      IHaltable
      (halt [_]
        (csp/close! stop-ch)
        (csp/<!! publish-loop)
        (csp/<!! (:done heal-loop))
        (-kafka/ktable-unsubscribe ktable (:subscription heal-loop))
        (-comp/halt ktable)
        (when-let [p @producer]
          (-comp/halt p))))))
