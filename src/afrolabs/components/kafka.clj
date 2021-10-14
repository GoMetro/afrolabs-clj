(ns afrolabs.components.kafka
  (:require [afrolabs.components.internal-include :as -inc]
            [afrolabs.components :as -comp]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as sgen]
            [integrant.core :as ig]
            [clojure.core.async :as csp]
            [afrolabs.components.health :as -health]
            [clojure.set :as set]
            [taoensso.timbre :as timbre
             :refer [log  trace  debug  info  warn  error  fatal  report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]])
  (:import [org.apache.kafka.clients.producer ProducerConfig ProducerRecord KafkaProducer Producer Callback]
           [org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer MockConsumer OffsetResetStrategy ConsumerRecord Consumer]
           [org.apache.kafka.clients.admin AdminClient AdminClientConfig NewTopic DescribeConfigsResult Config]
           [org.apache.kafka.common.header Header]
           [org.apache.kafka.common.config ConfigResource ConfigResource$Type]
           [java.util.concurrent Future]
           [java.util Map Collection UUID Optional]
           [afrolabs.components IHaltable]
           [clojure.lang IDeref IRef]))


(comment

  (set! *warn-on-reflection* true)

  )
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol ITopicNameProvider
  "A protocol for providing topic names. Could be used for subscribing to topics or for asserting them."
  (get-topic-names [_] "Returns a seq of names for kafka topics."))

(s/def ::topic-name-provider #(satisfies? ITopicNameProvider %))
(s/def ::topic-name-providers (s/coll-of ::topic-name-provider))

;;;;;;;;;;;;;;;;;;;;

(defprotocol IProducer
  "Can produce records. To kafka, probably."

  (get-producer [_] "Returns the actual kafka API producer object")
  (produce! [_ records]
    "Produces a collection of record maps. This is a side-effect!"))

(s/def ::kafka-producer #(satisfies? IProducer %))

(defprotocol IConsumer
  "The client interface for the kafka consumer component"
  (get-consumer [_] "Returns the actual kafka API consumer object (or mock)"))

(defprotocol IConsumerClient
  "Given to a consumer (thread), invoked with received messages, returns messages to be produced."
  (consume-messages [_ msgs] "Consumes a collection of messages from subscribed topics. Returns a collection of messages to be produced."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def :producer.msg/topic (s/and string?
                                  (comp pos-int? count)))
(s/def :producer.msg/key any?)
(s/def :producer.msg/value any?)
(s/def :producer.msg/delivered-ch any?) ;; if csp/chan? existed, we'd have used that
(s/def :producer.msg/headers (s/map-of (s/and string?
                                              (comp pos-int? count))
                                       any?))
(s/def :producer/msg (s/keys :req-un [:producer.msg/topic
                                      :producer.msg/value]
                             :opt-un [:producer.msg/delivered-ch
                                      :producer.msg/key
                                      :producer.msg/headers]))
(s/def :producer/msgs (s/coll-of :producer/msg))

(defmulti serialize-producer-record-header
  "Knowledge of how to serialize header values into byte[] values."
  type)

(defmethod serialize-producer-record-header java.lang.String
  [x] (.getBytes x "UTF-8"))

(defonce default-serialize-producer-record-header-used (atom #{}))

(defmethod serialize-producer-record-header :default
  [x]
  (let [t (type x)]
    (when-not (@default-serialize-producer-record-header-used t)
      (warnf "Using the default Kafka header value serializer for type: '%s'. defmethod on '%s' to silence this warning and provide better serialization."
             (str t)
             (str `serialize-producer-record-header))
      (swap! default-serialize-producer-record-header-used conj t)))
  (serialize-producer-record-header (str x)))

(defn- producer-produce
  [^Producer producer msgs]
  (s/assert :producer/msgs msgs)
  (->> msgs
       (map (juxt identity
                  #(let [hs (:headers %)]
                     (if hs
                       (ProducerRecord. (:topic %)
                                        (when-let [p (:partition %)] p)
                                        (:key %)
                                        (:value %)
                                        (map (fn [[hn hv]]
                                               (reify
                                                 Header
                                                 (^String key [_] hn)
                                                 (^bytes value [_] (serialize-producer-record-header hv))))
                                             hs))
                       (ProducerRecord. (:topic %)
                                        (:key %)
                                        (:value %))))))
       (map (fn [[{:keys [delivered-ch]
                   :as   msg}
                  producer-record]]
              (if delivered-ch
                (.send producer producer-record
                       (reify
                         Callback
                         (onCompletion [_ _ ex]
                           (trace (str "Firing onCompletion for msg. "))
                           ;; TODO ex may contain non-retriable exceptions, which must be used to indicate this component is not healthy
                           (when ex
                             (warn ex "Error while getting confirmation of producing record."))
                           (when-not ex
                             (trace "Forwarding delivery notification...")
                             (csp/go
                               (csp/>! delivered-ch msg)
                               (csp/close! delivered-ch)
                               (trace "onCompletion done."))))))
                (.send producer producer-record))))
       ;; doall means evaluate the lazy list (do side-effcts) but keep the sequence
       ;; this forces the send before the call returns
       (doall))
  ;; return value
  nil)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Strategies

(defprotocol IUpdateProducerConfigHook
  "A Strategy protocol for modifying the configuration properties before the Producer object is created. Useful for things like serdes."
  (update-producer-cfg-hook [this cfg] "Takes a config map and returns a (modified version of the) map."))

(defprotocol IUpdateConsumerConfigHook
  "A Strategy protocol for modifying the configuration properties before the Consumer objects is created. Useful for things like serdes."
  (update-consumer-cfg-hook [this cfg] "Takes a config map and returns a (modified version of the) map."))

(defprotocol IUpdateAdminClientConfigHook
  "A Strategy protocol for modifying the configuration properties before the AdminClient is created. Useful for Confluent connections."
  (update-admin-client-cfg-hook [this cfg] "Takes a config map and returns a modified version of the same map."))

(s/def ::update-admin-client-config-hook #(satisfies? IUpdateAdminClientConfigHook %))

(defprotocol IConsumerInitHook
  "A Strategy protocol for Kafka Object initialization. This hooks is called before the first .poll. Useful for assigning of or subscribing to partitions for consumers."
  (consumer-init-hook [this ^Consumer consumer] "Takes a Consumer or Producer Object. Results are ignored."))

(defprotocol IPostConsumeHook
  "A Strategy protocol for getting access to the Consumer Object and the consumed records, after the consume logic has been invoked. Useful for things like detecting when the consumer has caught up to the end of the partitions."
  (post-consume-hook [this consumer consumed-records] "Receives the consumer and the consumed records."))

(defprotocol IShutdownHook
  "A Strategy protocol for getting access to the kafka Object, eg after the .poll loop has been terminated, before .close is invoked. Useful for cleanup/.close type logic."
  (shutdown-hook [this consumer] "Receives the consumer."))

(defprotocol IConsumerMiddleware
  "A strategy protocol to intercept messages between the consumer object thread and the consumer-client.

  All the middlewares will form a chain, succesively intercepting and modifying messages one after the other, with later interceptors having access to the changes made be earlier interceptors.

  It is assumed that the result of any consumer-client is again messages that must be produced, so in reality this middleware accepts kafka messages (in map format) both from and to the consumer thread."
  (consumer-middleware-hook [this msgs] "Accepts a collection of messages and returns a collection of messages."))

(defprotocol IConsumedResultsHandler
  "A strategy protocol for dealing with the result of the IConsumerClient's consume-messages call. This is useful when for example you want to set up consumer-producer unit, where the results of consuming messages are messages that has to be produced."
  (handle-consumption-results [this xs] "Accepts whatever the (consume-messages ...) from the consumer-client returned."))


(def satisfies-some-of-the-strategy-protocols (->> [IShutdownHook
                                                    IPostConsumeHook
                                                    IConsumerInitHook
                                                    IUpdateConsumerConfigHook
                                                    IUpdateProducerConfigHook
                                                    IConsumerMiddleware
                                                    IConsumedResultsHandler
                                                    IUpdateAdminClientConfigHook]
                                                   (map #(partial satisfies? %))
                                                   (apply some-fn)))

(comment

  (satisfies-some-of-the-strategy-protocols (ConfluentJSONSchema {:schema-registry-url ""}))
  ;; true

  (satisfies-some-of-the-strategy-protocols (StringSerializer))
  ;; true

  (satisfies-some-of-the-strategy-protocols (SubscribeWithTopicsCollection ["topic-name"]))
  ;; true

  (satisfies-some-of-the-strategy-protocols "oeu") ;; nil
  (satisfies-some-of-the-strategy-protocols 123) ;; nil

  )


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti create-strategy first)

(defmacro defstrategy
  "Takes the paramaters and body of a pure function that creates a new component. Creates a top-level fn with the name (n) and declares a multi-method implementation that can create the same component.

  This is useful to declare component instances in config files by keyword."
  [n & body]
  `(do
     (defn ~n ~@body)
     (defmethod create-strategy ~(keyword "strategy" (name n))
       [strategy-specs#]
       (apply ~n (rest strategy-specs#)))))


;;;;;;;;;;;;;;;;;;;;

(defstrategy ConfluentJSONSchema
  [& {:keys [schema-registry-url
             sr-api-key
             sr-api-secret]
      producer-option :producer
      consumer-option :consumer
      :or {producer-option :value
           consumer-option :value}}]

  (when-not schema-registry-url
    (throw (ex-info "ConfluentJsonSchema strategy requires the schema-registry-url to be set." {})))

  (let [allowed-values #{:key :value :both}]
    (when-not (or (allowed-values producer-option)
                  (allowed-values consumer-option))
      (throw (ex-info "ConfluentJSONSchema expects one of #{:key :value :both} for each of :producer or :consumer, eg (ConfluentJSONSchema :schema-registry-url \"...\" :producer :both :consumer :key)"
                      {::allowed-values  allowed-values
                       ::consumer-option consumer-option
                       ::producer-option producer-option}))))

  (letfn [(update-with-credentials
            [cfg]
            (if-not (and sr-api-key sr-api-secret)
              (do
                (debug "Setting up schema registry config without api-key and api-secret.")
                cfg)
              (assoc cfg
                     "basic.auth.credentials.source"        "USER_INFO"
                     "schema.registry.basic.auth.user.info" (str sr-api-key ":" sr-api-secret))))]
    (reify
      IUpdateProducerConfigHook
      (update-producer-cfg-hook
          [_ cfg]
        (cond-> (assoc cfg "schema.registry.url" schema-registry-url)
          true                               update-with-credentials
          (#{:both :key}   producer-option)  (assoc ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG   "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer")
          (#{:both :value} producer-option)  (assoc ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer")))

      IUpdateConsumerConfigHook
      (update-consumer-cfg-hook
          [_ cfg]
        (cond-> (assoc cfg "schema.registry.url" schema-registry-url)
          true                               update-with-credentials
          (#{:both :key}   consumer-option)  (assoc ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG  "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer")
          (#{:both :value} consumer-option)  (assoc ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG    "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer"))))))


(defstrategy StringSerializer
  [& {producer-option :producer
      consumer-option :consumer
      :or {producer-option :none
           consumer-option :none}}]

  ;; validation of arguments, fail early
  (let [allowed-values #{:key :value :both :none}]
    (when-not (or (allowed-values producer-option)
                  (allowed-values consumer-option))
      (throw (ex-info "StringSerializer expects one of #{:key :value :both} for each of :producer or :consumer, eg (StringSerializery :producer :both :consumer :key)"
                      {::allowed-values  allowed-values
                       ::consumer-option consumer-option
                       ::producer-option producer-option}))))

  (reify
    IUpdateProducerConfigHook
    (update-producer-cfg-hook
        [_ cfg]
      (cond-> cfg
        (#{:both :key}   producer-option)  (assoc ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG   "org.apache.kafka.common.serialization.StringSerializer")
        (#{:both :value} producer-option)  (assoc ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer")))

    IUpdateConsumerConfigHook
    (update-consumer-cfg-hook
        [_ cfg]
      (cond-> cfg
        (#{:both :key}   consumer-option)  (assoc ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG  "org.apache.kafka.common.serialization.StringDeserializer")
        (#{:both :value} consumer-option)  (assoc ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG    "org.apache.kafka.common.serialization.StringDeserializer")))))

(defstrategy JsonSerializer
  [& {producer-option :producer
      consumer-option :consumer
      :or {producer-option :none
           consumer-option :none}}]

  ;; validation of arguments, fail early
  (let [allowed-values #{:key :value :both :none}]
    (when-not (or (allowed-values producer-option)
                  (allowed-values consumer-option))
      (throw (ex-info "StringSerializer expects one of #{:key :value :both} for each of :producer or :consumer, eg (StringSerializery :producer :both :consumer :key)"
                      {::allowed-values  allowed-values
                       ::consumer-option consumer-option
                       ::producer-option producer-option}))))

  (reify
    IUpdateProducerConfigHook
    (update-producer-cfg-hook
        [_ cfg]
      (cond-> cfg
        (#{:both :key}   producer-option)  (assoc ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG   "afrolabs.components.kafka.json_serdes.Serializer")
        (#{:both :value} producer-option)  (assoc ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "afrolabs.components.kafka.json_serdes.Serializer")))

    IUpdateConsumerConfigHook
    (update-consumer-cfg-hook
        [_ cfg]
      (cond-> cfg
        (#{:both :key}   consumer-option)  (assoc ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG  "afrolabs.components.kafka.json_serdes.Deserializer")
        (#{:both :value} consumer-option)  (assoc ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG    "afrolabs.components.kafka.json_serdes.Deserializer")))))

(defstrategy SubscribeWithTopicNameProvider
  [& topic-name-providers]
  (let [incorrect-type (into []
                             (filter #(not (satisfies? ITopicNameProvider %)))
                             topic-name-providers)]
    (when (seq incorrect-type)
      (throw (ex-info "The parameters to SubscribeWithTopicNameProvider must implement ITopicNameProvider protocol."
                      {::non-complient-providers incorrect-type}))))

  (reify
    IConsumerInitHook
    (consumer-init-hook
        [_ consumer]
      (.subscribe ^Consumer consumer
                  (into []
                        (comp
                         (mapcat #(get-topic-names %))
                         (distinct))
                        topic-name-providers)))))

(defstrategy SubscribeWithTopicsCollection
  [^Collection topics]
  (when-not (seq topics)
    (throw (ex-info "Specify a collaction of topics to subscribe to." {})))
  (reify
    IConsumerInitHook
    (consumer-init-hook
        [_ consumer]
      (.subscribe ^Consumer consumer topics))))


(defstrategy SubscribeWithTopicsRegex
  [^java.util.regex.Pattern regex]
  (reify
    IConsumerInitHook
    (consumer-init-hook
        [_ consumer]
      (.subscribe ^Consumer consumer regex))))

(defstrategy OffsetReset
  [strategy]
  (when-not (#{"earliest" "latest"} strategy)
    (throw (ex-info "Offset reset strategy has to be either 'latest' or 'earlient'"
                    {:given-strategy strategy})))
  (reify
    IUpdateConsumerConfigHook
    (update-consumer-cfg-hook
        [_ cfg]

      ;; validate that we're not trying to specify conflicting strategies here
      (let [previous-offset-reset-strategy (get cfg ConsumerConfig/AUTO_OFFSET_RESET_CONFIG)]
        (when (and previous-offset-reset-strategy
                   (not= previous-offset-reset-strategy
                         strategy))
          (throw (ex-info "Conflicting offset-reset-strategy specified."
                          {:previous previous-offset-reset-strategy
                           :this     strategy}))))

      ;; this is the meat of the fn
      (assoc cfg ConsumerConfig/AUTO_OFFSET_RESET_CONFIG strategy))))

(defstrategy AutoCommitOffsets
  [& {:keys [commit-interval-ms]
      :or {commit-interval-ms (* 30 1000)}}]
  (let [commit-interval-ms (int commit-interval-ms)]
    (reify

      IUpdateConsumerConfigHook
      (update-consumer-cfg-hook
          [_ cfg]
        (assoc cfg
               ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG true
               ConsumerConfig/AUTO_COMMIT_INTERVAL_MS_CONFIG commit-interval-ms)))))

(defstrategy ClientId
  [client-id]
  (reify
    IUpdateConsumerConfigHook
    (update-consumer-cfg-hook
        [_ cfg]
      (assoc cfg ProducerConfig/CLIENT_ID_CONFIG client-id))

    IUpdateProducerConfigHook
    (update-producer-cfg-hook
        [_ cfg]
      (assoc cfg ConsumerConfig/CLIENT_ID_CONFIG client-id))))

(defstrategy CaughtUpNotifications
  [& chs]
  (let [caught-up-ch (csp/chan)
        caught-up-mult (csp/mult caught-up-ch)]

    ;; tap all of the provided channels into the mult/ch that we notify on
    (doseq [ch chs] (csp/tap caught-up-mult ch))

    (reify
      IShutdownHook
      (shutdown-hook
          [_ _]
        (csp/close! caught-up-ch))

      IPostConsumeHook
      (post-consume-hook
          [_ consumer consumed-records]
        ;; Test if we've caught up to the last offset for every topic-partition we're consuming from
        (let [topic-partition-assignment (.assignment consumer)

              current-offsets
              (into #{}
                    (map (fn [tp] {:topic (.topic tp)
                                   :partition (.partition tp)
                                   :offset (.position consumer tp)}))
                    topic-partition-assignment)

              end-offsets
              (into #{}
                    (map (fn [[tp o]] {:topic (.topic tp)
                                       :partition (.partition tp)
                                       :offset o}))
                    (.endOffsets consumer
                                 topic-partition-assignment))
              ]
          (when (= end-offsets current-offsets)
            (csp/>!! caught-up-ch
                     current-offsets)))))))

(defstrategy FreshConsumerGroup
  [& {:keys [group-id-prepend]}]
  (reify
    IUpdateConsumerConfigHook
   (update-consumer-cfg-hook
       [_ cfg]
     (let [group-id (str (when group-id-prepend (str group-id-prepend "-"))
                         (UUID/randomUUID))]
       (info (format "Creating consumer group-id: %s"
                     group-id))
       (assoc cfg ConsumerConfig/GROUP_ID_CONFIG group-id)))))

(defstrategy ConsumerGroup
  [consumer-group-id]
  (reify IUpdateConsumerConfigHook
    (update-consumer-cfg-hook
        [_ cfg]
      (assoc cfg ConsumerConfig/GROUP_ID_CONFIG consumer-group-id))))


(defn normalize-strategies
  "Takes a collection of strategy-protocol-implementing-objects or strategy-keyword-declaration-vectors and turns the strategy-keyword-declaration-vectors into strategy-protocol-implementing-objects."
  [ss]
  (vec (concat (filter satisfies-some-of-the-strategy-protocols ss)
               (->> ss
                    (filter (complement satisfies-some-of-the-strategy-protocols))
                    (map (partial create-strategy))))))

(comment

  (normalize-strategies [(StringSerializer)
                         (AutoCommitOffsets)
                         [:strategy/FreshConsumerGroup]
                         [:strategy/SubscribeWithTopicsCollection ["test"]]])
  ;; [
  ;;  #object[afrolabs.components.kafka$StringSerializer$reify__692 0x264ce7ec "afrolabs.components.kafka$StringSerializer$reify__692@264ce7ec"]
  ;;  #object[afrolabs.components.kafka$AutoCommitOffsets$reify__751 0x29b9be3b "afrolabs.components.kafka$AutoCommitOffsets$reify__751@29b9be3b"]
  ;;  #object[afrolabs.components.kafka$FreshConsumerGroup$reify__781 0x75b65fb "afrolabs.components.kafka$FreshConsumerGroup$reify__781@75b65fb"]
  ;;  #object[afrolabs.components.kafka$SubscribeWithTopicsCollection$reify__727 0x44362059 "afrolabs.components.kafka$SubscribeWithTopicsCollection$reify__727@44362059"]
  ;; ]

  )


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::bootstrap-server string?)

(s/def ::update-producer-cfg-fn (s/fspec :args (s/cat :m map?)
                                         :ret map?))
(s/def ::update-consumer-cfg-fn (s/fspec :args (s/cat :m map?)
                                         :ret map?))
(s/def ::consumer (s/with-gen #(instance? Consumer %)
                    (constantly (sgen/return (MockConsumer. OffsetResetStrategy/EARLIEST)))))

(s/def ::strategy (s/or :protocol satisfies-some-of-the-strategy-protocols
                        :vector #(and (seq %1)
                                      (keyword? (first %1))
                                      (= "strategy" (namespace (first %1))))))
(s/def ::strategies (s/coll-of ::strategy))

;;;;;;;;;;;;;;;;;;;;

(s/def ::producer-config (s/keys :req-un [::bootstrap-server]
                                 :opt-un [::strategies]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::producer-consumption-results-xs-spec (s/or :single :producer/msg
                                                    :more   :producer/msgs))

(defn make-producer
  [{:keys [bootstrap-server
           strategies]
    :as producer-config}]
  (s/assert ::producer-config producer-config)
  (let [strategies (normalize-strategies strategies)
        starting-cfg {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-server}
        props ((->> strategies
                    (filter #(satisfies? IUpdateProducerConfigHook %))
                    (map #(partial update-producer-cfg-hook %))
                    (apply comp)) starting-cfg)
        producer (KafkaProducer. ^Map props)]
    (reify
      IProducer
      (produce! [_ msgs]
        (producer-produce producer msgs))
      (get-producer [_] producer)

      IConsumedResultsHandler
      (handle-consumption-results
          [_ xs]

        (let [xs (s/conform ::producer-consumption-results-xs-spec xs)]
          (when (= ::s/invalid xs)
            ;; horrible place to throw, but this is the best we've got.
            (throw (ex-info "The producer can only handle the result of consume-messages, if the value is either a single message to be produced, or a collection of messages to be produced."
                            {::explain-str (s/explain-str ::producer-consumption-results-xs-spec xs)
                             ::explain-data (s/explain-data ::producer-consumption-results-xs-spec xs)})))
          (let [[xs-type xs-value] xs]
            (producer-produce producer
                              (condp = xs-type
                                :single [xs-value]
                                :more    xs-value)))))

      IHaltable
      (halt [_]
        (.close ^Producer producer)

        (doseq [s strategies]
          (when (satisfies? -comp/IHaltable s)
            (-comp/halt s)))))))

;;;;;;;;;;;;;;;;;;;;

(-comp/defcomponent {::-comp/ig-kw       ::kafka-producer
                     ::-comp/config-spec ::producer-config}
  [cfg] (make-producer cfg))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; CONSUMER

(s/def :consumer/client #(satisfies? IConsumerClient %))
(s/def :consumer.poll/timeout pos-int?)
(s/def :consumer/mock #(= MockConsumer (class %)))

(s/def ::clientless-consumer (s/keys :req-un [::bootstrap-server
                                              ::-health/service-health-trip-switch]
                                     :opt-un [::strategies]
                                     :opt    [:consumer.poll/timeout
                                              :consumer/mock]))

(s/def ::consumer-config (s/and ::clientless-consumer
                                (s/keys :req    [:consumer/client])))

;;;;;;;;;;;;;;;;;;;;

(defn -consumer-main
  [^Consumer consumer
   must-stop
   & {{:consumer/keys        [client]
       :keys                 [strategies]
       poll-timeout          :consumer.poll/timeout
       :as                   consumer-config
       :or                   {poll-timeout 1000}} :consumer-config}]

  (s/assert ::consumer-config consumer-config)

  (let [post-consume-hooks
        (into []
              (filter (partial satisfies? IPostConsumeHook))
              strategies)

        combined-post-consume-hook
        (fn [& args]
          (doseq [s post-consume-hooks]
            (apply post-consume-hook s args)))

        consumed-results-handlers
        (into []
              (filter (partial satisfies? IConsumedResultsHandler))
              strategies)

        combined-consumed-results-handler
        (fn [xs]
          (doseq [h consumed-results-handlers]
            (handle-consumption-results h xs)))]
    (try

      (while (not @must-stop)
        (let [consumed-records           (into []
                                               (map (fn [^ConsumerRecord r]
                                                      {:topic     (.topic r)
                                                       :partition (.partition r)
                                                       :offset    (.offset r)
                                                       :value     (.value r)
                                                       :key       (.key r)}))
                                               (.poll consumer ^long poll-timeout))
              consumption-results        (consume-messages client consumed-records)]

          (when consumption-results
            (combined-consumed-results-handler consumption-results))

          (combined-post-consume-hook consumer consumed-records)))

      ;; we're done poll'ing and shutting down
      ;; give shutdown hooks a chance
      (doseq [s (filter (partial satisfies? IShutdownHook)
                        strategies)]
        (shutdown-hook s consumer))

      ;; the value we are returning
      [:done true]
      (catch Throwable t
        [:error t]))))

(defn- background-wait-on-stop-signal
  "Extracted only because the debugger chokes on core.async."
  [consumer must-stop
   {:as   consumer-config
    :keys [service-health-trip-switch]}
   consumer-properties has-stopped]
  (csp/go
    (let [[status xtra :as thread-result]
          (csp/<! (csp/thread (-consumer-main consumer
                                              must-stop
                                              :consumer-config consumer-config
                                              :consumer-properties consumer-properties)))]
      (when (= :error status)
        (error xtra ;; hopefully, an exception packaged with try/catch in -consumer-main
               "Kafka consumer main thread finished with exception. Tripping the health switch...")
        (-health/indicate-unhealthy! service-health-trip-switch ::kafka-consumer))

      ;; Anyway deliver the value into the promise.
      ;; At the time this code is written, the return value is not used.
      (deliver has-stopped
               (or xtra status thread-result)))))

(defn make-consumer
  [{:as            consumer-config
    :keys          [bootstrap-server
                    strategies]
    :consumer/keys [mock]}]
  (s/assert ::consumer-config consumer-config)
  (let [strategies          (normalize-strategies strategies)
        consumer-properties {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-server}
        consumer-properties ((->> strategies
                                  (filter #(satisfies? IUpdateConsumerConfigHook %))
                                  (map #(partial update-consumer-cfg-hook %))
                                  (apply comp)) consumer-properties)
        has-stopped         (promise)
        must-stop           (atom nil)
        consumer            (or mock (KafkaConsumer. ^Map consumer-properties))]

    ;; call all the consumer init strategies
    (doseq [strat (filter #(satisfies? IConsumerInitHook %)
                          strategies)]
      (consumer-init-hook strat consumer))

    ;; wait for the main consumer thread to exit, and deliver that result value to the promise
    ;; to indicate that the process has concluded shutting down
    (background-wait-on-stop-signal consumer must-stop consumer-config consumer-properties has-stopped)

    (reify

      IConsumer
      (get-consumer [_] consumer)

      IHaltable
      (halt [_]
        (reset! must-stop true)
        @has-stopped

        (doseq [s strategies]
          (when (satisfies? -comp/IHaltable s)
            (-comp/halt s)))

        (.close ^Consumer consumer)))))

(-comp/defcomponent {::-comp/ig-kw       ::kafka-consumer
                     ::-comp/config-spec ::consumer-config}
  [cfg] (make-consumer cfg))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(s/def ::admin-client-cfg (s/keys :req-un [::bootstrap-server
                                           ::strategies]))

(defn make-admin-client
  [{:keys [bootstrap-server
           strategies]
    :as   cfg}]

  (s/assert ::admin-client-cfg cfg)

  (let [strategies (normalize-strategies strategies)
        admin-client-cfg {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-server}
        admin-client-cfg ((->> strategies
                               (filter #(satisfies? IUpdateAdminClientConfigHook %))
                               (map (fn [strat]
                                      (fn [cfg]
                                        (update-admin-client-cfg-hook strat cfg))))
                               (apply comp)) admin-client-cfg)
        ac (AdminClient/create admin-client-cfg)]
    (reify
      clojure.lang.IDeref
      (deref [_] ac)

      IHaltable
      (halt [_] (.close ac)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::nr-of-partitions (s/or :nil nil?
                                :i   pos-int?
                                :s   #(try (Integer/parseInt %)
                                           (catch NumberFormatException _ false))))
(s/def ::topic-asserter-cfg (s/and ::admin-client-cfg
                                   (s/keys :req-un [::topic-name-providers]
                                           :opt-un [::nr-of-partitions])))

(-comp/defcomponent {::-comp/ig-kw       ::topic-asserter
                     ::-comp/config-spec ::topic-asserter-cfg}
  [{:as cfg
    :keys [topic-name-providers
           nr-of-partitions]}]

  (let [nr-of-partitions (or (when (and nr-of-partitions
                                        (string? nr-of-partitions))
                               (Integer/parseInt nr-of-partitions))
                             nr-of-partitions)
        ac (make-admin-client cfg)
        existing-topics (-> @ac
                            (.listTopics)
                            (.names)
                            (.get)
                            (set))
        new-topics (into []
                         (comp
                          (mapcat #(get-topic-names %))
                          (distinct)
                          (filter #(not (existing-topics %)))
                          (map (fn [topic-name]
                                 (info (format "Creating topic '%s' with nr-partitions '%s' and replication-factor '%s'."
                                               topic-name
                                               (str (or nr-of-partitions "CLUSTER_DEFAULT"))
                                               "CLUSTER_DEFAULT"))
                                 (NewTopic. topic-name
                                            (if nr-of-partitions
                                              (Optional/of nr-of-partitions)
                                              (Optional/empty))
                                            (Optional/empty)))))
                         topic-name-providers)
        topic-create-result (.createTopics @ac new-topics)]

    ;; wait for complete success
    (-> topic-create-result
        (.all)
        (.get))

    ;; stop the admin client
    (-comp/halt ac)

    ;; we need to return something for the component value, let's just use the config until something better comes about
    cfg))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::topics (s/coll-of (s/and string?
                                  #(pos-int? (count %)))))
(s/def ::list-of-topics-cfg (s/keys :req-un [::topics]))

(-comp/defcomponent {::-comp/config-spec ::list-of-topics-cfg
                     ::-comp/ig-kw       ::list-of-topics}
  [{:keys [topics]}]
  (reify
    ITopicNameProvider
    (get-topic-names [_] topics)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::topic-delete-retention-ms (s/or :n nil?
                                         :s (s/and string?
                                                   #(pos-int? (count %))
                                                   #(try (-> (Integer/parseInt %)
                                                             (pos-int?))
                                                         (catch NumberFormatException _ false)))
                                         :i pos-int?))
(s/def ::ktable-segment-ms ::topic-delete-retention-ms)
(s/def ::ktable-min-cleanable-dirty-ratio (s/or :n nil?
                                                :s (s/and string?
                                                          #(pos-int? (count %))
                                                          #(try (let [d (Double/parseDouble %)]
                                                                  (< 0.0 d 1.0))
                                                                (catch NumberFormatException _ false)))
                                                :d (s/and double?
                                                          #(< 0.0 % 1.0))))
(s/def ::ktable-asserter-cfg (s/and ::admin-client-cfg
                                    (s/keys :req-un [::topic-name-providers]
                                            :opt-un [::nr-of-partitions
                                                     ::topic-delete-retention-ms
                                                     ::ktable-segment-ms
                                                     ::ktable-min-cleanable-dirty-ratio])))

(-comp/defcomponent {::-comp/ig-kw       ::ktable-asserter
                     ::-comp/config-spec ::ktable-asserter-cfg}
  [{:as cfg
    :keys [topic-name-providers
           nr-of-partitions
           topic-delete-retention-ms
           ktable-segment-ms
           ktable-min-cleanable-dirty-ratio]}]

  (let [nr-of-partitions (or (when (and nr-of-partitions
                                        (string? nr-of-partitions))
                               (Integer/parseInt nr-of-partitions))
                             nr-of-partitions)
        ac (make-admin-client cfg)
        existing-topics (-> ^AdminClient @ac
                            (.listTopics)
                            (.names)
                            (.get)
                            (set))

        ;; we can't CHANGE a topic that has been created the wrong way
        ;; and neither can we let it be.
        ;; The app is in a broken state if a topic that must be compacted is not.
        topics-with-wrong-config (let [existing-topics (->> (mapcat #(get-topic-names %) topic-name-providers)
                                                            (distinct)
                                                            (filter existing-topics))
                                       topic-to-cleanup-policies (->> existing-topics
                                                                      (map #(ConfigResource. ConfigResource$Type/TOPIC %))
                                                                      (.describeConfigs ^AdminClient @ac)
                                                                      (.all)
                                                                      (.get)
                                                                      (map (fn [[^ConfigResource resource ^Config cfg]]
                                                                             [(.name resource)
                                                                              (.value (.get cfg "cleanup.policy"))]))
                                                                      (into {}))]
                                   (->> topic-to-cleanup-policies
                                        (filter (fn [[_ compaction-strategy]] (not= "compact" compaction-strategy)))
                                        (map first)))
        _ (when (pos-int? (count topics-with-wrong-config))
            (throw (ex-info (format "These topics must be created with topic config 'cleanup.policy' == 'compact', but they are not. Cannot continue.\n%s"
                                    (str topics-with-wrong-config))
                            {:topics topics-with-wrong-config})))

        new-topics (set/difference (set (mapcat #(get-topic-names %) topic-name-providers))
                                   existing-topics)
        topic-create-result (->> new-topics
                                 (map (fn [topic-name]
                                        (info (format "Creating log-compacted topic '%s' with nr-partitions '%s' and replication-factor '%s'."
                                                      topic-name
                                                      (str (or nr-of-partitions "CLUSTER_DEFAULT"))
                                                      "CLUSTER_DEFAULT"))
                                        (let [new-topic (NewTopic. ^String   topic-name
                                                                   ^Optional (if nr-of-partitions
                                                                               (Optional/of nr-of-partitions)
                                                                               (Optional/empty))
                                                                   ^Optional (Optional/empty))
                                              _ (.configs new-topic
                                                          (cond-> {"cleanup.policy" "compact"}
                                                            topic-delete-retention-ms (assoc "delete.retention.ms" (str topic-delete-retention-ms))
                                                            ktable-segment-ms (assoc "segment.ms" (str ktable-segment-ms))
                                                            ktable-min-cleanable-dirty-ratio (assoc "min.cleanable.dirty-ratio" (str ktable-min-cleanable-dirty-ratio))
                                                            ))]
                                          new-topic)))
                                 (.createTopics ^AdminClient @ac))]

    ;; wait for complete success
    (-> topic-create-result
        (.all)
        (.get))

    ;; stop the admin client
    (-comp/halt ac)

    ;; we need to return something for the component value, let's just use the config until something better comes about
    cfg))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn merge-updates-with-ktable
  [old-ktable new-msgs]
  (loop [old                      old-ktable
         [{:as head
           k :key
           v :value
           t :topic} & rest-msgs] new-msgs]
    (let [next-old (cond
                     ;; nothing is nil, save the value
                     (not (or (nil? k)
                              (nil? v)
                              (nil? t)))
                     (assoc-in old [t k] v)

                     ;; value (only) is nil, remove the value (tombstone)
                     (and (nil? v)
                          (not (or (nil? t)
                                   (nil? k))))
                     (update old t #(dissoc % k))

                     ;; default, return old value
                     :else
                     (do
                       (warn (format "merge-update-with-ktable does not have logic for this case: topic='%s', key='%s', value='%s'"
                                     (str t) (str k) (str v)))
                       old))]
      (if (not rest-msgs)
        next-old
        (recur next-old
               rest-msgs)))))

(s/def ::ktable-id (s/and string?
                          #(pos-int? (count %))))

(s/def ::ktable-cfg (s/and ::clientless-consumer
                           (s/keys :req-un [::ktable-id])))

(-comp/defcomponent {::-comp/config-spec ::ktable-cfg
                     ::-comp/ig-kw       ::ktable}
  [{:as cfg
    :keys [ktable-id]}]

  (let [consumer-group-id (str  ktable-id
                                "-"
                                (UUID/randomUUID))
        _ (println consumer-group-id)

        caught-up-ch (csp/chan)
        has-caught-up-once (promise)
        _ (csp/go (csp/<! caught-up-ch)
                  (deliver has-caught-up-once true)
                  (csp/close! caught-up-ch))

        ktable-state (atom {})
        consumer-client (reify
                          IConsumerClient
                          (consume-messages
                              [_ msgs]
                            (when (seq msgs)
                              (swap! ktable-state
                                     #(merge-updates-with-ktable % msgs)))))

        cfg (-> cfg
                (update-in [:strategies] concat [(OffsetReset "earliest")
                                                 (ConsumerGroup consumer-group-id)
                                                 (CaughtUpNotifications caught-up-ch)
                                                 ])
                (assoc :consumer/client consumer-client))

        consumer (make-consumer cfg)]

    (reify
      IHaltable
      (halt [_] (-comp/halt consumer))

      IDeref
      (deref [_]
        @has-caught-up-once
        @ktable-state)

      IRef
      (getValidator [this]  (.getValidator ktable-state))
      (getWatches [this]    (.getWatches ktable-state))
      (addWatch [this k cb] (.addWatch ktable-state k cb))
      (removeWatch [this k] (.removeWatch ktable-state k)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; create a forwarder; a forwarder is configured with:
;; - a producer
;; - a fn
;; It implements the IConsumedResultsHandler
;; by applying the fn to the consumed results, and forwarding it to the producer

(s/def :consumed-result-forwarder/producer ::kafka-producer)

;; we will allow the app developer to specify a function literal in the edn
;; and eval it to transform it into an actual fn?.
;; this is incredibly dangerous...
;; also note the literal will be eval'd more than once
(s/def :consumed-result-forwarder.transformer/literal-fn
  #(try (let [fn (eval %)]
          (fn? fn))
        (catch Throwable _ false)))
(s/def ::consumed-result-forwarder-cfg
  (s/and (s/keys :req-un [:consumed-result-forwarder/producer]
                 :opt-un [:consumed-result-forwarder.transformer/literal-fn])
         #(or (:literal-fn %))))

(-comp/defcomponent {::-comp/config-spec ::consumed-result-forwarder-cfg
                     ::-comp/ig-kw       ::consumed-result-forwarder}
  [{:as cfg
    :keys [producer
           literal-fn]}]
  (let [transformer-actual (cond
                             literal-fn
                             (do
                               (warn (str "The " ::consumed-result-forwarder " component is using eval to create a transformer: "
                                          literal-fn))
                               (eval literal-fn)))]
    (reify
      IConsumedResultsHandler
      (handle-consumption-results [_ xs]
        (when-let [forwarded (transformer-actual xs)]
          (produce! producer
                    forwarded))))))

