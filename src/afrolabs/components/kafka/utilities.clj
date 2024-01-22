(ns afrolabs.components.kafka.utilities
  (:require [afrolabs.components :as -comp]
            [afrolabs.components.kafka :as k]
            [afrolabs.components.confluent :as -confluent]
            [afrolabs.components.kafka.utilities.topic-forwarder :as -topic-forwarder]
            [afrolabs.components.confluent.schema-registry]
            [afrolabs.components.health :as -health]
            [integrant.core :as ig]
            [clojure.data.json :as json]
            [clojure.core.async :as csp]
            [taoensso.timbre :as log
             :refer [log  trace  debug  info  warn  error  fatal  report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]]
            [java-time :as time]
            [afrolabs.csp :as -csp]
            [net.cgrand.xforms :as x]
            [clojure.spec.alpha :as s]
            [afrolabs.components.kafka :as -kafka]
            [clojure.string :as str]
            [clojure.set :as set]
            [afrolabs.components.kafka.utilities.healthcheck :as -healthcheck]
            [afrolabs.components.kafka.bytes-serdes :as -bytes-serdes])
  (:import [afrolabs.components.health IServiceHealthTripSwitch]
           [afrolabs.components IHaltable]
           [java.util UUID]
           [afrolabs.components.kafka IPostConsumeHook IConsumerClient]
           [clojure.lang IDeref]))

(defn load-messages-from-confluent-topic
  "Loads a collection of messages from confluent kafka topics. Will stop consuming when the consumer has reached the very latest offsets.

  Intended for interactive use. Returns items in a list in reverse order (oldest at the end, newest at the beginning)

  - Specify :topics (a collection of strings) for which topics to subscribe to.
  - If you want to limit the number of messages you load, use :nr-msgs to specify a lower bound of loaded messages. You will get at least this many, unless there are too few messages in the topic, in which case you will get everything in the topic.
  - Use the :msg-filter to keep a subset of messages of the ones in the topic(s)
  - Optionally specify consumer-group-id, otherwise a fresh and unique consumer-group-id will be used.
  - Optionally specify :offset-reset (one of #{\"earliest\" \"latest\"}. Default \"earliest\".)
  - ConfluentCloud specific: when :api-key and :api-secret are both nil, the ConfluentCloud strategy will not be used.

  - use :extra-strategies is useful for specifying deserialization settings.


  eg (def invalid-msgs
       (afrolabs.config/with-config \".env\"
         #(-kafka-utils/load-messages-from-confluent-topic :bootstrap-server (:kafka-bootstrap-server %)
                                                           :topics           [\"test-topic\"]
                                                           :api-key          (:kafka-api-key %)
                                                           :api-secret       (:kafka-api-secret %)
                                                           :extra-strategies [[:strategy/StringSerializer :consumer :key]
                                                                              [:strategy/JsonSerializer :consumer :value]]
                                                           )))

  "
  [& {:keys [bootstrap-server
             topics
             nr-msgs
             api-key api-secret
             extra-strategies
             msg-filter
             consumer-group-id
             offset-reset]
      :or {nr-msgs          :all
           msg-filter       identity
           extra-strategies []
           offset-reset     "earliest"}}]
  (let [loaded-msgs (atom nil)
        loaded-enough-msgs (promise)

        last-progress-update (atom (time/instant))

        caught-up-ch (csp/chan)
        _ (csp/go (csp/<! caught-up-ch)
                  (info "Caught up to the end of the subscribed topics, closing...")
                  (deliver loaded-enough-msgs true))

        health-trip-switch (-healthcheck/make-fake-health-trip-switch loaded-enough-msgs)

        consumer-client
        (reify
          k/IConsumerClient
          (consume-messages
              [_ msgs]
            (let [msgs (filter msg-filter msgs)
                  new-state (swap! loaded-msgs (partial apply conj) msgs)
                  how-many (count new-state)]

              ;; do we have enough yet? is anything ever enough?
              (when (and (not= nr-msgs :all)
                         (< nr-msgs how-many))
                (info "Indicating that we've received enough messages...")
                (deliver loaded-enough-msgs true))

              ;; give progress indicators
              (let [t-now (time/instant)]
                (when (time/after? t-now
                                   (time/plus @last-progress-update
                                              (time/seconds 30)))
                  (info (str "Loaded " how-many " messages ..."))
                  (reset! last-progress-update t-now))))
            nil))

        ig-cfg
        (let [strategies-result (concat (keep identity
                                              [(when (and api-key api-secret)
                                                 (-confluent/ConfluentCloud :api-key api-key :api-secret api-secret))
                                               (when topics
                                                 (k/SubscribeWithTopicsCollection topics))
                                               (if-not consumer-group-id
                                                 (k/FreshConsumerGroup)
                                                 (k/ConsumerGroup consumer-group-id))
                                               (k/OffsetReset offset-reset)
                                               (k/CaughtUpNotifications caught-up-ch)])
                                        extra-strategies)]
          (log/debug (with-out-str (clojure.pprint/pprint strategies-result)))
          {::k/kafka-consumer
           {:bootstrap-server           bootstrap-server
            :consumer/client            consumer-client
            :service-health-trip-switch health-trip-switch
            :strategies                 strategies-result}})

        system (ig/init ig-cfg)]

    (try
      @loaded-enough-msgs
      (infof "Done waiting, received a total of '%d' messages." (count @loaded-msgs))
      (ig/halt! system)
      (info "System done shutting down.")

      ;; return value
      (or (when (and nr-msgs
                     (number? nr-msgs))
            (vec (take nr-msgs @loaded-msgs)))
          @loaded-msgs)

      (catch Throwable t
        (warn t "Caught a throwable while waiting for messages to load. Stopping the system...")
        (ig/halt! system)
        (info "Extraordinary system stop completed.")
        @loaded-msgs))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn forward-topics-between-clusters-2
  "This fn will forward messages between different kafka clusters. The return value is an IHaltable (afrolabs.components/halt <return-value) which will terminate the process.

  This is version 2 of this fn that changes the parameter interface to allow use as a component."
  ([src-cluster-cfg dest-cluster-cfg {:keys [consumer-group-id
                                             health-component]}]
   (let [src-cluster-cfg (cond-> src-cluster-cfg
                           consumer-group-id
                           (update :strategies concat [[:strategy/AutoCommitOffsets]
                                                       [:strategy/ConsumerGroup consumer-group-id]]))
         log-metrics-input-ch (csp/chan)
         log-metrics-output-ch (csp/chan)
         _ (-csp/partition-by-interval log-metrics-input-ch
                                       (x/reduce (fn
                                                   ([x] x)
                                                   ([acc x]
                                                    (merge-with + acc x)))
                                                 {})
                                       log-metrics-output-ch
                                       30000)
         _ (csp/go-loop [stats (csp/<! log-metrics-output-ch)]
             (when stats
               (log/info (str "Forwarded messages to these topics: " stats))
               (recur (csp/<! log-metrics-output-ch))))

         system (atom (ig/init (-topic-forwarder/create-system-config
                                (-> src-cluster-cfg
                                    (update :strategies concat [(reify
                                                                  IPostConsumeHook
                                                                  (post-consume-hook [_ _ msgs]
                                                                    (when (seq msgs)
                                                                      (csp/go (csp/>! log-metrics-input-ch
                                                                                      (into {}
                                                                                            (x/by-key :topic x/count)
                                                                                            msgs))))))]))
                                dest-cluster-cfg
                                (cond-> {}
                                  health-component (assoc :health-component health-component)))))
         halted? (promise)
         do-halt (fn []
                   (swap! system
                          (fn [old-system]
                            (when old-system
                              (ig/halt! old-system))
                            nil))
                   (csp/close! log-metrics-input-ch)
                   (csp/close! log-metrics-output-ch)
                   (deliver halted? true))]

     (csp/thread
       (-health/wait-while-healthy (or health-component
                                       (-> system deref :afrolabs.components.health/component)))
       (do-halt))
     (reify
       IHaltable
       (halt [_]
         (do-halt)
         @halted?)))))

;;;;;;;;;;

(s/def ::src-cluster-cfg :topic-forwarder-cfg/src)
(s/def ::dest-cluster-cfg :topic-forwarder-cfg/dest)
(s/def ::consumer-group-id #(and (string? %)
                                 (seq %)))
(s/def ::health-component :topic-forwarder-cfg/health-component)

(s/def ::topic-forwarder-cfg (s/keys :req-un [::src-cluster-cfg
                                              ::dest-cluster-cfg
                                              ::consumer-group-id
                                              ::health-component]))
(-comp/defcomponent {::-comp/config-spec  ::topic-forwarder-cfg
                     ::-comp/ig-kw        ::topic-forwarder}
  [{:keys [src-cluster-cfg
           dest-cluster-cfg
           consumer-group-id
           health-component]}]
  (forward-topics-between-clusters-2 src-cluster-cfg
                                     dest-cluster-cfg
                                     {:consumer-group-id consumer-group-id
                                      :health-component  health-component}))

;;;;;;;;;;;;;;;;;;;;

(defn forward-topics-between-clusters
  "This fn will forward messages between different kafka clusters. It is indended for interactive use. The return value is an IHaltable (afrolabs.components/halt <return-value) which will terminate the process."
  ([consumer-group-id src-cluster-cfg dest-cluster-cfg]
   (forward-topics-between-clusters-2 src-cluster-cfg
                                      dest-cluster-cfg
                                      {:consumer-group-id consumer-group-id}))
  ([src-cluster-cfg dest-cluster-cfg]
   (forward-topics-between-clusters-2 src-cluster-cfg
                                      dest-cluster-cfg
                                      {})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn delete-some-topics-on-cluster!
  "This code will delete _some_ topics, based on a predicate."
  [& {:keys [bootstrap-server
             confluent-api-key confluent-api-secret ;; confluent
             extra-strategies
             topic-predicate
             preserve-internal-and-confluent-topics]
      :or {extra-strategies                       []
           preserve-internal-and-confluent-topics true}}]
  (let [admin-client-strategies (concat (keep identity
                                              [(when (and confluent-api-key confluent-api-secret)
                                                 (-confluent/ConfluentCloud :api-key confluent-api-key :api-secret confluent-api-secret))])
                                        extra-strategies)
        admin-client (k/make-admin-client {:bootstrap-server bootstrap-server
                                           :strategies       admin-client-strategies})
        extra-topic-predicate (if preserve-internal-and-confluent-topics
                                (comp (filter (complement #(str/starts-with? % "_")))
                                      (filter (complement #(str/index-of % "ksql")))
                                      (filter (complement #(str/index-of % "connect"))))
                                (constantly true))
        topics-to-be-deleted (into #{}
                                   (comp (filter topic-predicate)
                                         extra-topic-predicate)
                                   (-> @admin-client
                                       (.listTopics)
                                       (.names)
                                       (.get)))]

    (when (seq topics-to-be-deleted)
      (log/infof "Deleting these topics:\n%s"
                 (str topics-to-be-deleted))
      (.all (.deleteTopics ^org.apache.kafka.clients.admin.AdminClient @admin-client
                           topics-to-be-deleted)))

    ;; to release the resources of the admin-client
    (-comp/halt admin-client)))

(defn delete-all-topics-on-cluster!
  "Deletes ALL of the topics on a kafka cluster."
  [& {:as cfg}]
  (apply delete-some-topics-on-cluster! (->> (assoc cfg :topic-predicate (constantly true))
                                             (mapcat identity))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn list-all-topics
  [& {:keys [bootstrap-server
             confluent-api-key confluent-api-secret ;; confluent
             extra-strategies
             admin-client]
      :or {extra-strategies []}}]
  (let [admin-client-strategies (concat (keep identity
                                              [(when (and confluent-api-key confluent-api-secret)
                                                 (-confluent/ConfluentCloud :api-key confluent-api-key :api-secret confluent-api-secret))])
                                        extra-strategies)
        admin-client' (or admin-client
                          (k/make-admin-client {:bootstrap-server bootstrap-server
                                                :strategies       admin-client-strategies}))

        topics-result (set (-> @admin-client'
                               (.listTopics)
                               (.names)
                               (.get)))]

    ;; to release the resources of the admin-client
    ;; but only if it was not provided as a parameter
    (when-not admin-client
      (-comp/halt admin-client'))

    topics-result))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn produce-and-wait!
  "Intended for interactive use. Produces all of the records and wait for their delivery ack before returning."
  [producer msgs]
  (let [msgs (into []
                   (map #(assoc % :delivered-ch (csp/chan)))
                   msgs)]
    (k/produce! producer msgs)
    (doseq [{ch :delivered-ch} msgs] (csp/<!! ch))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn alter-topic-config
  [& {:as   alter-cfg
      :keys [topic-name
             partition-count
             confluent-api-key confluent-api-secret
             bootstrap-server
             extra-strategies]}]
  (let [admin-client-strategies (concat (keep identity
                                              [(when (and confluent-api-key confluent-api-secret)
                                                 (-confluent/ConfluentCloud :api-key confluent-api-key :api-secret confluent-api-secret))])
                                        extra-strategies)
        admin-client (k/make-admin-client {:bootstrap-server bootstrap-server
                                           :strategies       admin-client-strategies})]

    (when partition-count
      (-> @admin-client
          (.createPartitions {topic-name (org.apache.kafka.clients.admin.NewPartitions/increaseTo partition-count)})
          (.all)))

    ;; to release the resources of the admin-client
    (-comp/halt admin-client)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::load-ktable-cfg (s/keys :req-un [::-kafka/bootstrap-server
                                          ::-kafka/topics]
                                 :opt-un [::-kafka/strategies]))

(defn load-ktable
  [& {:as cfg
      :keys [api-key api-secret
             topics]}]
  (let [extra-strategies (->> [(when (and (seq (:api-key cfg))
                                          (seq (:api-secret cfg)))
                                 [(-confluent/ConfluentCloud cfg)])
                               (when topics
                                 [(-kafka/SubscribeWithTopicsCollection topics)])]
                              (keep identity)
                              flatten)
        system-cfg {:afrolabs.components.health/component
                    {:intercept-signals                   false
                     :intercept-uncaught-exceptions       false
                     :trigger-self-destruct-timer-seconds nil}

                    ::-kafka/ktable (-> cfg
                                        (update :strategies
                                                concat extra-strategies)
                                        (assoc :service-health-trip-switch
                                                (ig/ref :afrolabs.components.health/component)))}
        system (ig/init system-cfg)
        health-component (:afrolabs.components.health/component system)
        ktable (::-kafka/ktable system)]

    (reify
      IDeref
      (deref [_]
        (when (not (-health/healthy? health-component))
          (log/warn "The ktable utility is no longer healthy."))
        @ktable)

      IHaltable
      (halt [_]
        (ig/halt! system)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; load partition data

(defn node->data
  [node]
  (cond-> {:id        (.id node)
           :id-str    (.idString node)
           :is-empty  (.isEmpty node)
           :port      (.port node)
           :host      (.host node)}
    (.hasRack node) (assoc :rack (.rack node))))

(defn describe-topics
  [admin-client topics]
  (let [partitions-info (->> (let [topics (-> @admin-client
                                              (.describeTopics topics)
                                              (.allTopicNames)
                                              (.get))]
                               (->> topics
                                    (mapcat (fn [[topic description]]
                                              (map (fn [topic-partition-info]
                                                     {:topic topic
                                                      :is-internal (.isInternal description)
                                                      :topic-id (.toString (.topicId description))
                                                      :in-sync-replicas (apply list (map node->data (.isr topic-partition-info)))
                                                      :leader (node->data (.leader topic-partition-info))
                                                      :partition (.partition topic-partition-info)
                                                      :replicas (apply list (map node->data (.replicas topic-partition-info)))})
                                                   (.partitions description))))
                                    (into [])))
                             (reduce (fn [{:as acc
                                           :keys [nodes]}
                                          item]
                                       (-> acc
                                           (update :nodes set/union
                                                   (->> [(:leader item)]
                                                        (concat (:replicas item))
                                                        (concat (:in-sync-replicas item))
                                                        (into #{})))
                                           (update :data conj
                                                   (-> item
                                                       (update :leader :id)
                                                       (update :replicas #(map :id %))
                                                       (update :in-sync-replicas #(map :id %))))))
                                     {:nodes #{}
                                      :data  []}))]
    (-> partitions-info
        (update :nodes #(apply sorted-set-by
                               (fn [a b] (< (:id a) (:id b)))
                               %))
        (update :data #(sort-by :partition %)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; load consumer group info

(defn describe-consumer-groups
  [admin-client-component consumer-groups]
  (let [r (-> @admin-client-component
              (.describeConsumerGroups consumer-groups)
              (.all)
              (.get))]
    (map (fn [[_ cgdesc]]
           {:coordinator              (node->data (.coordinator cgdesc))
            :group-id                 (.groupId cgdesc)
            :is-simple-consumer-group (.isSimpleConsumerGroup cgdesc)
            :members                  (->> (.members cgdesc)
                                           (map (fn [membdesc]
                                                  (let [gid (.groupInstanceId membdesc)]
                                                    (cond-> {:client-id (.clientId membdesc)
                                                             :consumer-id (.consumerId membdesc)
                                                             :host (.host membdesc)
                                                             }
                                                      (.isPresent gid) (assoc :group-instance-id (.get gid)))))))
            :partition-assignor       (.partitionAssignor cgdesc)
            :state                    (.toString (.state cgdesc))})
         r)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; backup topics

(defn rename-topic:add-backup
  "Used as a default argument to `rename-topic-strategy` param in `backup-topic`.

  Creates a new topic by prepending \"backup-\" to the passed topic name."
  [topic]
  (str "backup-" topic))

(comment

  (rename-topic:add-backup "test") ;; "backup-test"

  )

(defn copy-topic
  "Makes a \"copy\" of a topic by copying data from one topic, to a newly created topic.
  This fn assumes both src and dest topics are on the same kafka cluster.

  This fn blocks the calling thread.

  Intended for interactive use.

  This will keep on copying messages from the src topic until it reaches the current offset, and then stop.
  This process /might/ therefore not stop if the src-topic is continually receiving new data, faster than it can be forwarded.

  This is a dumb backup strategy as only the :key and the :value will be \"backed-up\" to a new topic.
  Other data like partition, record-date etc will get the default treatment for produced records."
  [& {:keys [src-topic dest-topic
             bootstrap-server
             confluent-api-key confluent-api-secret
             consumer-group-id
             nr-of-partitions]
      :or   {dest-topic        (rename-topic:add-backup src-topic)
             consumer-group-id (str "copy-topic-consumer-" (UUID/randomUUID))
             nr-of-partitions     2}}]

  (let [common-strategies (keep identity
                                [(when (and confluent-api-key confluent-api-secret)
                                   (-confluent/ConfluentCloud :api-key confluent-api-key
                                                              :api-secret confluent-api-secret))])
        admin-client-strategies common-strategies
        src-topic-consumer-strategies (concat [(-bytes-serdes/ByteArraySerializer :consumer :both)
                                               (-kafka/OffsetReset "earliest")
                                               (-kafka/ConsumerGroup consumer-group-id)
                                               (-kafka/SubscribeWithTopicsCollection [src-topic])
                                               (-kafka/AutoCommitOffsets)]
                                              common-strategies)
        dest-topic-producer-strategies (concat [(-bytes-serdes/ByteArraySerializer :producer :both)
                                                (-kafka/HighThroughput)]
                                               common-strategies)

        admin-client (-kafka/make-admin-client {:bootstrap-server bootstrap-server
                                                :strategies       admin-client-strategies})
        all-server-topics (list-all-topics :admin-client admin-client)]

    (try
      (when-not (all-server-topics src-topic)
        (throw (ex-info "The src-topic does not exist and thus cannot be backed up." {:src-topic src-topic})))
      ;; on reflection, when restoring data to a topic, failing because it already exists is silly...
      #_(when (all-server-topics dest-topic)
        (throw (ex-info "The dest-topic exists already and cannot be used as a backup topic." {:dest-topic dest-topic})))

      ;; create the backup topic if it does not exist
      (when-not (all-server-topics dest-topic)
        (-kafka/assert-topics @admin-client [dest-topic]
                              :nr-of-partitions nr-of-partitions))
      (finally
        (-comp/halt admin-client)))

    (let [system-must-stop (promise)

          ;; provides log feedback while the backup-system is running
          log-metrics-input-ch (csp/chan)
          _ (csp/go-loop [total-msgs 0]
              (let [timeout (csp/timeout 30000)
                    [v ch] (csp/alts! [log-metrics-input-ch
                                       timeout])

                    new-total
                    (cond
                      (= ch timeout)
                      (do (log/info (format "Backed up a total of '%d' messages so far." total-msgs))
                          total-msgs)

                      (and (= ch log-metrics-input-ch)
                           v)
                      (+ total-msgs v)

                      :else
                      (do (log/info (format "Backed up a total of '%d' messages." total-msgs))
                          nil))]
                (when new-total
                  (recur new-total))))

          caught-up-ch (csp/chan)
          _ (csp/go (csp/<! caught-up-ch)
                    (info "Caught up to the end of the subscribed topics, closing...")
                    (deliver system-must-stop true))

          health-tripswitch (-healthcheck/make-fake-health-trip-switch system-must-stop)

          system (atom (ig/init {::-kafka/kafka-producer {:bootstrap-server bootstrap-server
                                                          :strategies       dest-topic-producer-strategies}
                                 ::-kafka/kafka-consumer {:bootstrap-server bootstrap-server
                                                          :strategies       (concat src-topic-consumer-strategies
                                                                                    [(ig/ref ::-kafka/kafka-producer)
                                                                                     (-kafka/CaughtUpNotifications caught-up-ch)])
                                                          :service-health-trip-switch health-tripswitch
                                                          :consumer/client            (reify
                                                                                        IConsumerClient
                                                                                        (consume-messages [_ msgs]
                                                                                          (when (seq msgs)
                                                                                            (csp/go (csp/>! log-metrics-input-ch
                                                                                                            (count msgs))))
                                                                                          (into [] (comp
                                                                                                    (map #(select-keys % [:key :value]))
                                                                                                    (map #(assoc % :topic dest-topic)))
                                                                                                msgs)))}}))

          halted? (promise)
          do-halt (fn []
                    (swap! system
                           (fn [old-system]
                             (when old-system
                               (ig/halt! old-system))
                             nil))
                    (csp/close! log-metrics-input-ch)
                    (csp/close! caught-up-ch)
                    (deliver halted? true))]

      ;; wait for the consumer to catch up to the current offset (or other exception)
      ;; blocks the calling thread
      @system-must-stop
      (do-halt)
      @halted?)))
