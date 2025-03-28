(ns afrolabs.components.kafka
  (:require
   [afrolabs.components :as -comp]
   [afrolabs.components.health :as -health]
   [afrolabs.components.internal-include :as -inc]
   [afrolabs.components.kafka.edn-serdes :as -edn-serdes]
   [afrolabs.components.kafka.json-serdes :as -json-serdes]
   [afrolabs.components.kafka.transit-serdes :as -transit-serdes]
   [afrolabs.components.kafka.checkpoint-storage :as -ktable-checkpoints]
   [afrolabs.components.time :as -time]
   [afrolabs.prometheus :as -prom]
   [clojure.core.async :as csp]
   [clojure.set :as set]
   [clojure.spec.alpha :as s]
   [clojure.spec.gen.alpha :as sgen]
   [clojure.string :as str]
   [com.rpl.specter :as specter]
   [iapetos.core :as prom]
   [integrant.core :as ig]
   [java-time.api :as t]
   [net.cgrand.xforms :as x]
   [taoensso.timbre :as log]
   [taoensso.timbre :as timbre :refer [log  trace  debug  info  warn  error  fatal  report logf tracef debugf infof warnf errorf fatalf reportf spy get-env]]
   )
  (:import [org.apache.kafka.clients.producer
            ProducerConfig ProducerRecord KafkaProducer Producer Callback RecordMetadata]

           [org.apache.kafka.clients.consumer
            ConsumerConfig KafkaConsumer MockConsumer OffsetResetStrategy ConsumerRecord Consumer
            ConsumerRebalanceListener OffsetAndTimestamp]

           [org.apache.kafka.clients.admin
            AdminClient AdminClientConfig NewTopic DescribeConfigsResult Config AlterConfigOp
            AlterConfigOp$OpType ConfigEntry TopicDescription NewPartitions]

           [org.apache.kafka.common.header
            Header Headers]

           [org.apache.kafka.common.config
            ConfigResource ConfigResource$Type]

           [org.apache.kafka.common
            TopicPartition TopicPartitionInfo]

           [java.util.concurrent
            Future]

           [java.util
            Map Collection UUID Optional]

           [afrolabs.components
            IHaltable]

           [clojure.lang
            IDeref IRef IBlockingDeref]))


(comment

  (set! *warn-on-reflection* true)

  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol ITopicNameProvider
  :extend-via-metadata true
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

(defprotocol IProducerPreProduceMiddleware
  "Can be applied to a producer. Will pre-process a message before it is handed off to the producer client.

  If multiple `IProducerPreProduceMiddleware` are applied to a producer, they will all be applied to the messages as if
  they are middleware."
  (pre-produce-hook [this msgs] "Accepts a sequence of messages and returns a (possibly) modified sequence of messages."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def :producer.msg/topic (s/and string?
                                  (comp pos-int? count)))
(s/def :producer.msg/key any?)
(s/def :producer.msg/value any?)
(s/def :producer.msg/delivered-ch any?) ;; if csp/chan? existed, we'd have used that
(s/def :producer.msg/headers (s/or :map (s/map-of (s/and string?
                                                         (comp pos-int? count))
                                                  any?)

                                   :tuples (s/coll-of (s/and (s/coll-of any? :count 2)
                                                             (comp string? first)))))
(s/def :producer/msg (s/keys :req-un [:producer.msg/topic
                                      :producer.msg/value]
                             :opt-un [:producer.msg/delivered-ch
                                      :producer.msg/key
                                      :producer.msg/headers]))
(s/def :producer/msgs (s/coll-of :producer/msg))

(defmulti deserialize-consumer-record-header
  "Provides a way to deserialize header values, based on the name of the header.
  Default deserializer is to keep the value as a byte-array.

  Returns the deserialized value itself."
  (fn [header-name _header-value-bytes-array] header-name))

(defmethod deserialize-consumer-record-header :default
  [_ value-byte-array] value-byte-array)

(defn- deserialize-consumer-record-header*
  [[header-name header-value]]
  [header-name (deserialize-consumer-record-header header-name header-value)])

(comment

  (def serialize-producer-record-header-by-name nil)
  )
(defmulti serialize-producer-record-header-by-name
  "Serializes a header by name of the header, rather than by type of the value.
  The producer will attempt to use this multimethod first, and next try serialize-producer-record-header
  to serialize by value type.

  This MUST return a byte array."
  (fn [header-name _header-value] header-name))

(defmethod serialize-producer-record-header-by-name :default
  [_ _])

(defmulti serialize-producer-record-header
  "Knowledge of how to serialize header values into byte[] values."
  type)

(defmethod serialize-producer-record-header java.lang.String
  [x] (.getBytes ^java.lang.String x "UTF-8"))

(defmethod serialize-producer-record-header (type (byte-array 0))
  [x] x)

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

(deftype KafkaProducingHeader [key value]
  Header
  (^String key [_] key)
  (^bytes value [_]
   (when value
     (or (serialize-producer-record-header-by-name key value)
         (serialize-producer-record-header value)))))

(deftype KafkaProducingCompletionCallback [msg delivered-ch]
  Callback
  (onCompletion [_ meta-data ex]
    (trace (str "Firing onCompletion for msg. "))
    ;; TODO ex may contain non-retriable exceptions, which must be used to indicate this component is not healthy
    (when ex
      (trace "Forwarding delivery exception...")
      (csp/go
        (csp/>!! delivered-ch ex)
        (csp/close! delivered-ch)
        (trace "when exception onCompletion done.")))
    (when-not ex
      (trace "Forwarding delivery notification...")
      (csp/go
        (csp/>! delivered-ch (merge msg
                                    (cond-> {:topic     (.topic ^RecordMetadata meta-data)
                                             :partition (.partition ^RecordMetadata meta-data)}

                                      (.hasOffset ^RecordMetadata meta-data)
                                      (assoc :offset (.offset ^RecordMetadata meta-data))

                                      (.hasTimestamp ^RecordMetadata meta-data)
                                      (assoc :timestamp (t/instant (.timestamp ^RecordMetadata meta-data))))))
        (csp/close! delivered-ch)
        (trace "onCompletion delivered result.")))))

(-prom/register-metric (prom/counter ::producer-msgs-produced
                                     {:description "How many messages are being produce via producer-produce."
                                      :labels [:topic]}))

(defn- producer-produce
  [^Producer producer msgs]
  (s/assert :producer/msgs msgs)
  (doseq [{:as msg
           :keys [delivered-ch]} msgs]
    (prom/inc (get-counter-producer-msgs-produced {:topic (:topic msg)})
              1)

    (let [producer-record (ProducerRecord. (:topic msg)
                                           (when-let [p (:partition msg)] p)
                                           nil ;; timestamp, Long
                                           (:key msg)
                                           (:value msg)
                                           (when-let [hs (:headers msg)]
                                             (map (fn [[hn hv]] (->KafkaProducingHeader hn hv))
                                                  hs)))]

      (if delivered-ch
        (.send producer
               producer-record
               (->KafkaProducingCompletionCallback msg delivered-ch))
        (.send producer producer-record)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Strategies

(defprotocol IUpdateProducerConfigHook
  "A Strategy protocol for modifying the configuration properties before the Producer object is created. Useful for
  things like serdes."
  (update-producer-cfg-hook [this cfg] "Takes a config map and returns a (modified version of the) map."))

(defprotocol IUpdateConsumerConfigHook
  "A Strategy protocol for modifying the configuration properties before the Consumer objects is created. Useful for
  things like serdes."
  (update-consumer-cfg-hook [this cfg] "Takes a config map and returns a (modified version of the) map."))

(defprotocol IUpdateAdminClientConfigHook
  "A Strategy protocol for modifying the configuration properties before the AdminClient is created. Useful for
  Confluent connections."
  (update-admin-client-cfg-hook [this cfg] "Takes a config map and returns a modified version of the same map."))

(s/def ::update-admin-client-config-hook #(satisfies? IUpdateAdminClientConfigHook %))

(defprotocol IConsumerInitHook
  "A Strategy protocol for Kafka Object initialization. This hooks is called before the first .poll.
  Useful for assigning of or subscribing to partitions for consumers."
  (consumer-init-hook [this ^Consumer consumer] "Takes a Consumer or Producer Object. Results are ignored."))

(defprotocol IConsumerPostInitHook
  "A strategy protocol for the kafka consumer object. This is called after IConsumerInitHook (after subscription) but
  before the first .poll. Useful for getting info about partitions or topics before consumption starts."
  (post-init-hook [this consumer] "Receives the consumer before the first call to .poll."))

(defprotocol IPostConsumeHook
  "A Strategy protocol for getting access to the Consumer Object and the consumed records, after the consume logic has
  been invoked. Useful for things like detecting when the consumer has caught up to the end of the partitions."
  (post-consume-hook [this consumer consumed-records] "Receives the consumer and the consumed records."))

(defprotocol IShutdownHook
  "A Strategy protocol for getting access to the kafka Object, eg after the .poll loop has been terminated, before
  .close is invoked. Useful for cleanup/.close type logic."
  (shutdown-hook [this consumer] "Receives the consumer."))

(defprotocol IConsumerMiddleware
  "A strategy protocol to intercept messages between the consumer object thread and the consumer-client.

  All the middlewares will form a chain, succesively intercepting and modifying messages one after the other, with later
  interceptors having access to the changes made be earlier interceptors.

  It is assumed that the result of any consumer-client is again messages that must be produced, so in reality this
  middleware accepts kafka messages (in map format) both from and to the consumer thread."
  (consumer-middleware-hook [this msgs] "Accepts a collection of messages and returns a collection of messages."))

(defprotocol IConsumedResultsHandler
  "A strategy protocol for dealing with the result of the IConsumerClient's consume-messages call. This is useful when
  for example you want to set up consumer-producer unit, where the results of consuming messages are messages that has
  to be produced."
  (handle-consumption-results [this xs] "Accepts whatever the (consume-messages ...) from the consumer-client returned."))

(defprotocol IConsumerMessagesPreProcessor
  "A strategy for intercepting messages between the Consumer and the consumer-client. This strategy may modify
  messages in flight."
  (pre-process-messages [this msgs] "Accepts kafka records and returns (possibly modified) kafka records."))


(def satisfies-some-of-the-strategy-protocols (->> [IShutdownHook
                                                    IPostConsumeHook
                                                    IConsumerInitHook
                                                    IUpdateConsumerConfigHook
                                                    IUpdateProducerConfigHook
                                                    IConsumerMiddleware
                                                    IConsumedResultsHandler
                                                    IUpdateAdminClientConfigHook
                                                    IConsumerPostInitHook
                                                    IProducerPreProduceMiddleware
                                                    IConsumerMessagesPreProcessor]
                                                   (map #(partial satisfies? %))
                                                   (apply some-fn)))

(comment

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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defstrategy RenameTopicsForProducer
  [& {:keys [rename-fn]}]
  (when-not rename-fn
    (throw (ex-info (str "strategy RenameTopicsForProducer requires the `rename-fn` to be specified. This function "
                         "accepts a topic name for every message, and must return a modified version of the topic, "
                         "OR nil, in which case the original topic value will be kept.")
                    {})))
  (reify
    IProducerPreProduceMiddleware
    (pre-produce-hook [_ msgs]
      (map (fn [{:as   msg
                 :keys [topic]}]
             (assoc msg :topic (or (rename-fn topic)
                                   topic)))
           msgs))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- resolve-fn
  "Resolves a configuration value that semantically points to an fn, to the actual fn.
  `the-fn` might be a symbol, in which case the namespace will be recquired and the symbol resolved
  or an actual `IFn`."
  [the-fn]
  (when the-fn
    (cond
      (fn? the-fn)       the-fn
      (symbol? the-fn)   (let [the-fn-ref (var-get (requiring-resolve the-fn))]
                           (if-not (fn? the-fn-ref)
                             (throw (ex-info "`the-fn` must be an fn or a symbol that points to an fn."
                                             {:the-fn the-fn}))
                             the-fn-ref))
      :else
      (throw (ex-info "Unable to resolve `the-fn` to a function." {:the-fn the-fn})))))

(defstrategy UpdateConsumedRecordKey
  [& {:keys [key-fn] :as params}]
  (when-not key-fn
    (throw (ex-info "In `UpdateConsumedRecordKey` strategy, `key-fn` parameter must have a value."
                    {:key-fn key-fn
                     :params params})))
  (let [key-fn' (resolve-fn key-fn)]
    (reify
      IConsumerMessagesPreProcessor
      (pre-process-messages [_ msgs]
        (map #(update % :key key-fn') msgs)))))

(defstrategy UpdateConsumedRecordValue
  [& {:keys [value-fn] :as params}]
  (when-not value-fn
    (throw (ex-info "`value-fn` must have a value."
                    {:value-fn value-fn
                     :params   params})))
  (let [value-fn' (resolve-fn value-fn)]
    (reify
      IConsumerMessagesPreProcessor
      (pre-process-messages [_ msgs]
        (map #(update % :value value-fn') msgs)))))

;; We have this "wrapper" for a producer because the strategy system
;; attempts to call -comp/halt on any strategies. If this is called on
;; the producer itself, it is closed/halted BEFORE it has finished producing
;; records that are in flight. By wrapping it like this, we are shielding it
;; from that -comp/halt call, and instead integrant will call halt on the producer
;; at the right time, depending on the dependency chain.
(defstrategy ProduceConsumerResultsWithProducer
  [producer]
  (when-not (s/valid? ::kafka-producer producer)
    (throw (ex-info "`ProduceConsumerResultsWithProducer` requires a producer component."
                    {:producer-value producer})))
  (reify
    IConsumedResultsHandler
    (handle-consumption-results [_ msgs]
      (let [xs (s/conform ::producer-consumption-results-xs-spec msgs)]
        (when (= ::s/invalid xs)
          ;; horrible place to throw, but this is the best we've got.
          (throw (ex-info "The producer can only handle the result of consume-messages, if the value is either a single message to be produced, or a collection of messages to be produced."
                          {::explain-str (s/explain-str ::producer-consumption-results-xs-spec msgs)
                           ::explain-data (s/explain-data ::producer-consumption-results-xs-spec msgs)})))
        (let [[xs-type _] xs]
          (produce! producer
                    (condp = xs-type
                      :single [msgs]
                      :more   msgs)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- get-allowed-config-keys
    [config-class]
  (->> (.getDeclaredFields ^Class config-class)
       ;; looking for public final fields static
       (filter (fn [field]
                 (let [mods (.getModifiers ^java.lang.reflect.Field field)]
                   (and (java.lang.reflect.Modifier/isStatic mods)
                        (java.lang.reflect.Modifier/isFinal mods)
                        (java.lang.reflect.Modifier/isPublic mods)))))
       ;; looking only for members that end with _CONFIG
       (filter (fn [field]
                 (-> (.getName ^java.lang.reflect.Field field)
                     (str/split #"_")
                     (last)
                     (str/lower-case)
                     (= "config"))))
       (map (fn [field]
              (.get ^java.lang.reflect.Field field
                    config-class)))))

(let [producer-cfg-keys (set (get-allowed-config-keys ProducerConfig))
      consumer-cfg-keys (set (get-allowed-config-keys ConsumerConfig))
      admin-client-cfg-keys (set (get-allowed-config-keys AdminClientConfig))]

  ;; AdhocConfig allows the user to specify a strategy for any allowed config keys.
  ;; The strategy is somewhat smart, in that it "knows" which parameters apply to
  ;; either Producer, Consumer or AdminClient config maps.

  (defstrategy AdhocConfig
    [& config-pairs]

    (when-not (even? (count config-pairs))
      (throw (ex-info "Specify pairs of parameters for AdhocConfig (ie groups of [config-key config-value])"
                      {:config config-pairs})))

    (let [config-pairs (partition 2 config-pairs)
          invalid-config-keys (set/difference (->> config-pairs
                                                   (map first)
                                                   (set))
                                              (set/union producer-cfg-keys
                                                         consumer-cfg-keys
                                                         admin-client-cfg-keys))]
      (when (seq invalid-config-keys)
        (log/warn (str "Found invalid config keys in AdhocConfig: " invalid-config-keys)))

      (reify
        IUpdateProducerConfigHook
        (update-producer-cfg-hook
            [_ cfg]
          (->> config-pairs
               (filter (fn [[config-key _]] (producer-cfg-keys config-key)))
               (reduce (fn [acc [k v]] (assoc acc k v))
                       cfg)))


        IUpdateConsumerConfigHook
        (update-consumer-cfg-hook
            [_ cfg]
          (->> config-pairs
               (filter (fn [[config-key _]] (consumer-cfg-keys config-key)))
               (reduce (fn [acc [k v]] (assoc acc k v))
                       cfg)))

        IUpdateAdminClientConfigHook
        (update-admin-client-cfg-hook
            [_ cfg]
          (->> config-pairs
               (filter (fn [[config-key _]] (admin-client-cfg-keys config-key)))
               (reduce (fn [acc [k v]] (assoc acc k v))
                       cfg)))))))

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
      :keys [deserialize-keys-as-keyword?]
      :or {producer-option              :none
           consumer-option              :none
           deserialize-keys-as-keyword? false}}]

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
        (#{:both :key}   consumer-option)  (assoc ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG  "afrolabs.components.kafka.json_serdes.Deserializer"
                                                  -json-serdes/json-deserializer-keyfn-option-name (if deserialize-keys-as-keyword? "keyword" "identity"))
        (#{:both :value} consumer-option)  (assoc ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG    "afrolabs.components.kafka.json_serdes.Deserializer"
                                                  -json-serdes/json-deserializer-keyfn-option-name (if deserialize-keys-as-keyword? "keyword" "identity"))))))

(defstrategy EdnSerializer
  [& {producer-option :producer
      consumer-option :consumer

      :keys [parse-inst-as-java-time]
      :or   {producer-option         :none
             consumer-option         :none
             parse-inst-as-java-time false}}]

  (let [allowed-values #{:key :value :both :none}]
    (when-not (or (allowed-values producer-option)
                  (allowed-values consumer-option))
      (throw (ex-info "EdnSerializer expects one of #{:key :value :both} for each of :producer or :consumer, eg (EdnSerializer :producer :both :consumer :key)"
                      {::allowed-values  allowed-values
                       ::consumer-option consumer-option
                       ::producer-option producer-option}))))

  (reify
    IUpdateProducerConfigHook
    (update-producer-cfg-hook
        [_ cfg]
      (cond-> cfg
        (#{:both :key}   producer-option)  (assoc ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG   "afrolabs.components.kafka.edn_serdes.Serializer")
        (#{:both :value} producer-option)  (assoc ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "afrolabs.components.kafka.edn_serdes.Serializer")))

    IUpdateConsumerConfigHook
    (update-consumer-cfg-hook
        [_ cfg]
      (cond-> cfg
        (#{:both :key}   consumer-option)  (assoc ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG    "afrolabs.components.kafka.edn_serdes.Deserializer"
                                                  (:parse-inst-as-java-time -edn-serdes/config-keys) parse-inst-as-java-time)
        (#{:both :value} consumer-option)  (assoc ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG  "afrolabs.components.kafka.edn_serdes.Deserializer"
                                                  (:parse-inst-as-java-time -edn-serdes/config-keys) parse-inst-as-java-time)))))

(defstrategy TransitSerializer
  [& {producer-option :producer
      consumer-option :consumer

      :or   {producer-option         :none
             consumer-option         :none}}]

  (let [allowed-values #{:key :value :both :none}]
    (when-not (or (allowed-values producer-option)
                  (allowed-values consumer-option))
      (throw (ex-info "TransitSerializer expects one of #{:key :value :both} for each of :producer or :consumer, eg (TransitSerializer :producer :both :consumer :key)"
                      {::allowed-values  allowed-values
                       ::consumer-option consumer-option
                       ::producer-option producer-option}))))

  (reify
    IUpdateProducerConfigHook
    (update-producer-cfg-hook
        [_ cfg]
      (cond-> cfg
        (#{:both :key}   producer-option)  (assoc ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG   "afrolabs.components.kafka.transit_serdes.Serializer")
        (#{:both :value} producer-option)  (assoc ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "afrolabs.components.kafka.transit_serdes.Serializer")))

    IUpdateConsumerConfigHook
    (update-consumer-cfg-hook
        [_ cfg]
      (cond-> cfg
        (#{:both :key}   consumer-option)  (assoc ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG    "afrolabs.components.kafka.transit_serdes.Deserializer")
        (#{:both :value} consumer-option)  (assoc ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG  "afrolabs.components.kafka.transit_serdes.Deserializer")))))

(defprotocol IConsumerAwareRebalanceListener
  "Wrapping ConsumerRebalanceListener; adds the consumer to the parameter list"
  (on-partitions-revoked  [_ consumer partitions])
  (on-partitions-lost     [_ consumer partitions])
  (on-partitions-assigned [_ consumer partitions]))

(defn combine-all-rebalance-listeners
  [rebalance-listeners]
  (let [;; wrap (kafka) ConsumerRebalanceListeners into IConsumerAwareRebalanceListeners
        kafka-rebalance-listeners (into #{}
                                  (comp (filter #(instance? ConsumerRebalanceListener %))
                                        (map (fn [crbl]
                                               (reify
                                                 IConsumerAwareRebalanceListener
                                                 (on-partitions-revoked  [_ _ p] (.onPartitionsRevoked  ^ConsumerRebalanceListener crbl p))
                                                 (on-partitions-lost     [_ _ p] (.onPartitionsLost     ^ConsumerRebalanceListener crbl p))
                                                 (on-partitions-assigned [_ _ p] (.onPartitionsAssigned ^ConsumerRebalanceListener crbl p))))))
                                  rebalance-listeners)
        consumer-aware-rebalance-listeners (into #{}
                                                 (filter #(satisfies? IConsumerAwareRebalanceListener %))
                                                 rebalance-listeners)
        combined-listeners (set/union kafka-rebalance-listeners
                                      consumer-aware-rebalance-listeners)]
    (fn [consumer]
      (reify
        ConsumerRebalanceListener
        (onPartitionsRevoked  [_ partitions] (doseq [l combined-listeners] (on-partitions-revoked  l consumer partitions)))
        (onPartitionsLost     [_ partitions] (doseq [l combined-listeners] (on-partitions-lost     l consumer partitions)))
        (onPartitionsAssigned [_ partitions] (doseq [l combined-listeners] (on-partitions-assigned l consumer partitions)))))))

(defstrategy SubscribeWithTopicNameProvider
  [& {:keys [topic-name-providers
             consumer-rebalance-listeners]}]
  (when (and topic-name-providers (not (coll? topic-name-providers)))
    (throw (ex-info "topic-name-providers must be a collection." {})))

  (when (and consumer-rebalance-listeners (not (coll? consumer-rebalance-listeners)))
    (throw (ex-info "consumer-rebalance-listeners must be a collection." {})))

  (let [with-consumer-rebalance-listeners (combine-all-rebalance-listeners consumer-rebalance-listeners)]
    (when (or (some #(and (not (satisfies? IConsumerAwareRebalanceListener %))
                          (not (instance? ConsumerRebalanceListener %)))
                    consumer-rebalance-listeners)
              (some #(not (satisfies? ITopicNameProvider %))
                    topic-name-providers))
      (throw (ex-info "The parameters to SubscribeWithTopicNameProvider must for :topic-name-providers (list-of) implement ITopicNameProvider AND for :consumer-rebalance-listener (list-of) IConsumerAwareRebalanceListener protocol or ConsumerRebalanceListener interface." {})))

    (reify
      IConsumerInitHook
      (consumer-init-hook
          [_ consumer]
        (.subscribe ^Consumer consumer
                    ^Collection
                    (into []
                          (comp
                           (mapcat #(get-topic-names %))
                           (distinct))
                          topic-name-providers)
                    ^ConsumerRebalanceListener (with-consumer-rebalance-listeners consumer))))))

(defn resolve-topic-like
  "Accepts something that must resolve into a string.
  - a string literal
  - an fn, in which case it will be called without args, and expects a string back
  - a symbol, in which case it will be resolved, and
    - check if it has a string value, in which case that is used
    - check if it is an fn, in which case it will be called, and a string is expected"
  [topic-like]
  (cond
    (string? topic-like)
    topic-like

    (fn? topic-like)
    (try (let [x (topic-like)]
           (if-not (string? x)
             (throw (ex-info "topic-like is fn?, but when called does not return a string, which it must."
                             {:topic-like   topic-like
                              :return-value x}))
             x))
         (catch Throwable t nil
                (log/warn t "Unable to call topic-like, which is fn?")
                nil))

    ;; it's a symbol? resolve it, and recur on the value
    (symbol? topic-like)
    (resolve-topic-like (var-get (requiring-resolve topic-like)))

    :else
    (do (log/with-context+ {:topic-like topic-like}
          (log/warn "Don't know what to do with topic-like."))
        nil)))

(defn resolve-topic-like-seq
  "Accepts a sequence of topic-likes, resolving all of them, keeping non-nil values."
  [xs]
  (->> xs
       (map resolve-topic-like)
       (remove nil?)))

(defstrategy SubscribeWithTopicsCollection
  ([topics]
   (when-not (seq topics)
     (throw (ex-info "Specify a collection of topics to subscribe to." {})))
   (reify
     IConsumerInitHook
     (consumer-init-hook
         [_ consumer]
       (.subscribe ^Consumer consumer
                   ^Collection (resolve-topic-like-seq topics)))))
  ([topics
    rebalance-listeners]
   (when-not (seq topics)
     (throw (ex-info "Specify a collection of topics to subscribe to." {})))
   (when-not (seq rebalance-listeners)
     (throw (ex-info "Specify a collection of at least one element for the rebalance-listeners" {})))
   (let [with-consumer-rebalance-listener (combine-all-rebalance-listeners rebalance-listeners)]
     (reify
       IConsumerInitHook
       (consumer-init-hook
           [_ consumer]
         (.subscribe ^Consumer consumer
                     ^Collection (resolve-topic-like-seq topics)
                     ^ConsumerRebalanceListener (with-consumer-rebalance-listener consumer)))))))


;; TODO - Add an overload for ConsumerRebalanceListener
(defstrategy SubscribeWithTopicsRegex
  [^java.util.regex.Pattern regex]
  (reify
    IConsumerInitHook
    (consumer-init-hook
        [_ consumer]
      (.subscribe ^Consumer consumer regex))))

(defstrategy SeekToPartitionOffset
  [topic-partition-offsets]
  (reify
    IConsumerPostInitHook
    (post-init-hook
        [_ consumer]
      (let [assignment
            (loop []
              (let [assignment (.assignment ^KafkaConsumer consumer)]
                (if (seq assignment)
                  assignment
                  (do
                    (log/debug "Polling once for topic-partition assignment to happen, before seeking to offsets.")
                    (.poll ^Consumer consumer 5000)
                    (recur)))))]
        (doseq [[topic partition] (map (fn [topic-partition]
                                         [(.topic ^TopicPartition topic-partition)
                                          (.partition ^TopicPartition topic-partition)])
                                       assignment)]
          (when-let [offset (get-in topic-partition-offsets [topic partition])]
            (.seek ^KafkaConsumer consumer
                   (TopicPartition. topic partition)
                   ^long offset)))))))

(defn ->millis-from-epoch
  "A utility function that can take something representing an instant
  and convert it to millis-since-the-epoch.

  Inputs:
  - integer : returns itself. An integer value is considered to be the right thing already
  - string : parsed as an instant, and converted to the instant's millis-since-epoch value
  - java.time.Instant or ZonedDateTime : converted to instant's millis-since-epoch value"
  [x]
  (cond
    (integer? x)
    x

    (string? x)
    (-> x
        (t/instant)
        (t/to-millis-from-epoch))

    (#{java.time.Instant
       java.time.ZonedDateTime} (type x))
    (t/to-millis-from-epoch x)))

(defstrategy SeekToTimestampOffset
  [offset]
  (let [offset (->millis-from-epoch offset)]
    (reify
      IConsumerPostInitHook
      (post-init-hook
          [_ consumer]
        (let [assignment
              (loop [remaining 2]
                (when (zero? remaining)
                  (throw (ex-info "SeekToTimestampOffset Cannot find topic assignment to seek to" {:remaining remaining})))
                (let [assignment (.assignment ^KafkaConsumer consumer)]
                  (if (seq assignment)
                    assignment
                    (do
                      (log/debug "Polling once for topic-partition assignment to happen, before seeking to offsets.")
                      (.poll ^Consumer consumer 5000)
                      (recur (dec remaining))))))]
          (doseq [[^TopicPartition topic-partition
                   ^OffsetAndTimestamp offset-and-timestamp]
                  (.offsetsForTimes ^KafkaConsumer consumer
                                    (into {}
                                          (map #(vector % offset))
                                          assignment))]
            (when offset-and-timestamp
              (.seek ^KafkaConsumer consumer
                     topic-partition
                     (.offset offset-and-timestamp)))))))))

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
  [& {:keys [commit-interval-ms
             disabled]
      :or {commit-interval-ms (* 30 1000)
           disabled           false}}]
  (let [commit-interval-ms (int commit-interval-ms)]
    (reify

      IUpdateConsumerConfigHook
      (update-consumer-cfg-hook
          [_ cfg]
        (if disabled
          (assoc cfg ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG false)
          (assoc cfg
                 ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG true
                 ConsumerConfig/AUTO_COMMIT_INTERVAL_MS_CONFIG commit-interval-ms))))))

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

(defn consumer-end-offsets
  "Gets the (.endOffsets ...) of the consumer in nice data format.
  Pass a duration in `timeout-duration` to use non-default timeout value.
  When a timeout exception is encountered, the value of `timeout-value` will be returned.
  If this value is `:throw`, the TimeoutException will be re-thrown."
  [^Consumer consumer & {:keys [timeout-duration
                                timeout-value]
                         :or   {timeout-value :timeout}}]
  (try
    (let [topic-partition-assignment (.assignment consumer)]
      (into #{}
            (map (fn [[tp o]]
                   {:topic     (.topic ^TopicPartition tp)
                    :partition (.partition ^TopicPartition tp)
                    :offset    o}))
            (if timeout-duration
              (.endOffsets consumer topic-partition-assignment timeout-duration)
              (.endOffsets consumer topic-partition-assignment))))
    (catch org.apache.kafka.common.errors.TimeoutException to
      (log/warn to "Timeout fetching consumer end offsets.")
      (if (= :throw timeout-value)
        (throw to)
        timeout-value))))

(defn consumer-current-offsets
  "Gets the (.position Consumer) of the consumer in nice format.
  Pass a duration in `timeout-duration` to use non-default timeout value.
  When a timeout exception is encountered, the value of `timeout-value` will be returned.
  If this value is set to `:throw`, the TimeoutException will be re-thrown."
  [^Consumer consumer & {:keys [timeout-duration
                                timeout-value]
                         :or   {timeout-value :timeout}}]
  (try
    (let [topic-partition-assignment (.assignment consumer)]
      (into #{}
            (map (fn [tp]
                   {:topic     (.topic ^TopicPartition tp)
                    :partition (.partition ^TopicPartition tp)
                    :offset    (if timeout-duration
                                 (.position consumer ^TopicPartition tp timeout-duration)
                                 (.position consumer ^TopicPartition tp))}))
            topic-partition-assignment))
    (catch org.apache.kafka.common.errors.TimeoutException to
      (log/warn to "Timeout waiting for consumer position.")
      (if (= :throw timeout-value)
        (throw to)
        timeout-value))))

;; Once the consumer loads, fetches the endOffsets. Once the consumer reaches those offsets
;; will send this value to the channel. This is distinct from `CaughtUpNotifications` in that
;; the latter only fires when the consumer is up-to-date, whereas this strategy fires
;; once the consumer reaches the offsets that were current when it started up.
(defstrategy CaughtUpOnceNotifications
  [& chs]
  (let [caught-up-ch (csp/chan)
        caught-up-mult (csp/mult caught-up-ch)
        end-offsets-at-start (atom nil)]

    ;; tap all of the provided channels into the mult/ch that we notify on
    (doseq [ch chs] (csp/tap caught-up-mult ch))

    (reify
      IShutdownHook
      (shutdown-hook
          [_ _]
        (csp/close! caught-up-ch))

      IPostConsumeHook
      (post-consume-hook
          [_ consumer _consumed-records]
        ;; we have to store the endOffsets, after the first consume (ie after partition assignment)
        ;; if we have a value in this atom, just keep it
        ;; otherwise query the end offsets and store it
        (swap! end-offsets-at-start
               (fn [old-value]
                 (or old-value
                     (consumer-end-offsets consumer :timeout-value :throw))))

        (let [current-offsets (consumer-current-offsets consumer :timeout-value :throw)]
          #_(log/spy :debug "Start offsets & current offsets "
                   [@end-offsets-at-start current-offsets])
          ;; when the end offsets at the start are less than the current offsets
          ;; we have caught up once
          (csp/go (when (every? (fn [{:keys [topic partition offset]}]
                                  (<= offset
                                      (->> current-offsets
                                           (filter (fn [co]
                                                     (and (= (:partition co) partition)
                                                          (= (:topic co) topic))))
                                           (first)
                                           :offset)))
                                @end-offsets-at-start)
                    (csp/>!! caught-up-ch
                             current-offsets))))))))

(defn consumer:fully-current?
  "Checks if the consumer's current offsets is equal to the end offsets.

  This means there is nothing (right now) to consume in the topic.

  Specify `timeout` as a java.time.Duration and `timeout-value` as a constant
  so that you don't hang on this forever. `timeout-value` may be `:throw`, in which
  case an exception will be thrown if the timeout expires before an offset value is
  returned from the consumer.

  Returns the current-offsets as the truthy value."
  [consumer timeout timeout-value]
  (let [current-offsets (consumer-current-offsets consumer :timeout-value :throw)
        end-offsets (consumer-end-offsets consumer :timeout-value :throw)]
    (when (and current-offsets
               end-offsets
               (= end-offsets current-offsets))
      end-offsets)))

(defstrategy CaughtUpNotifications
  [& chs]
  (let [caught-up-ch (csp/chan)
        caught-up-mult (csp/mult caught-up-ch)]

    ;; tap all of the provided channels into the mult/ch that we notify on
    (doseq [ch chs]
      (csp/tap caught-up-mult ch))

    (reify
      IShutdownHook
      (shutdown-hook
          [_ _]
        (csp/close! caught-up-ch))

      IPostConsumeHook
      (post-consume-hook
          [_ consumer _consumed-records]
        ;; Test if we've caught up to the last offset for every topic-partition we're consuming from
        (let [current-offsets? (consumer:fully-current? consumer (t/duration 10 :seconds) :timeout)]
          (when (and current-offsets?
                     (not= current-offsets? :timeout))
            (csp/>!! caught-up-ch
                     current-offsets?)))))))

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

(defstrategy RequestTimeout
  [request-timeout-ms]
  (reify
    IUpdateConsumerConfigHook
    (update-consumer-cfg-hook
        [_ cfg]
      (assoc cfg ConsumerConfig/REQUEST_TIMEOUT_MS_CONFIG (str request-timeout-ms)))

    IUpdateProducerConfigHook
    (update-producer-cfg-hook
        [_ cfg]
      (assoc cfg ProducerConfig/REQUEST_TIMEOUT_MS_CONFIG (str request-timeout-ms)))))

;; use :default to avoid setting this value in the ConsumerConfig
(defstrategy ConsumerMaxPollRecords
  [max-poll-records]
  (reify
    IUpdateConsumerConfigHook
    (update-consumer-cfg-hook
        [_ cfg]
      (log/debug (str "ConsumerMaxPollRecords: " max-poll-records))
      (cond-> cfg
        (not= max-poll-records :default)
        (assoc ConsumerConfig/MAX_POLL_RECORDS_CONFIG (str max-poll-records))))))

;; high default values
(defstrategy HighThroughput
  [& {:keys [consumer-fetch-min-bytes
             consumer-fetch-max-wait-ms
             consumer-max-poll-records
             consumer-max-poll-interval

             producer-batch-size
             producer-linger-ms
             producer-acks
             producer-buffer-memory
             producer-compression-type]
      :or   {consumer-fetch-min-bytes   50000   ;; default is 1...
             consumer-fetch-max-wait-ms 1000    ;; default is 500
             consumer-max-poll-records  5000    ;; default is 500
             consumer-max-poll-interval 300000  ;; default, 5 minutes

             producer-batch-size       131072    ;; ¯\_(ツ)_/¯
             producer-linger-ms        100       ;; default 0
             producer-acks             1         ;; this is probably less than the server's default
             producer-buffer-memory    134217728 ;; 8 times more than default value
             producer-compression-type "lz4"}}]  ;; lz4 is for "performance" ¯\_(ツ)_/¯
  (reify
    IUpdateConsumerConfigHook
    (update-consumer-cfg-hook
        [_ cfg]
      (-> cfg
          (assoc ConsumerConfig/FETCH_MIN_BYTES_CONFIG (int consumer-fetch-min-bytes))
          (assoc ConsumerConfig/FETCH_MAX_WAIT_MS_CONFIG (int consumer-fetch-max-wait-ms))
          (assoc ConsumerConfig/MAX_POLL_RECORDS_CONFIG (int consumer-max-poll-records))
          (assoc ConsumerConfig/MAX_POLL_INTERVAL_MS_CONFIG (int consumer-max-poll-interval))))

    IUpdateProducerConfigHook
    (update-producer-cfg-hook
        [_ cfg]
      (-> cfg
          (assoc ProducerConfig/BATCH_SIZE_CONFIG (int producer-batch-size))
          (assoc ProducerConfig/LINGER_MS_CONFIG (int producer-linger-ms))
          (assoc ProducerConfig/ACKS_CONFIG (str producer-acks))
          (assoc ProducerConfig/BUFFER_MEMORY_CONFIG (int producer-buffer-memory))
          (assoc ProducerConfig/COMPRESSION_TYPE_CONFIG producer-compression-type)))))


(defstrategy ProducerCompression
  [& compression-type-first-arg]
  (let [compression-type (or (first compression-type-first-arg)
                             "lz4")]
    (reify
      IUpdateProducerConfigHook
      (update-producer-cfg-hook
          [_ cfg]
        (assoc cfg ProducerConfig/COMPRESSION_TYPE_CONFIG compression-type)))))

(defstrategy ProducerBatching
  [& {:keys [batch-size
             linger-ms]
      :or {batch-size 131072
           linger-ms  150}}]
  (reify
    IUpdateProducerConfigHook
    (update-producer-cfg-hook
        [_ cfg]
      (assoc cfg
             ProducerConfig/BATCH_SIZE_CONFIG (int batch-size)
             ProducerConfig/LINGER_MS_CONFIG (int linger-ms)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; after suggestions from confluent, use with care

(defstrategy ReduceRebalanceFrequency
  [& {:keys [request-timeout-ms
             heartbeat-interval-ms]
      :or {request-timeout-ms 30000}}]
  (let [heartbeat-interval-ms (or heartbeat-interval-ms
                                  (-> request-timeout-ms
                                      (/ 3)
                                      (int)))]
    (reify
      IUpdateConsumerConfigHook
      (update-consumer-cfg-hook
          [_ cfg]
        (-> cfg
            (assoc ConsumerConfig/REQUEST_TIMEOUT_MS_CONFIG (str request-timeout-ms))
            (assoc ConsumerConfig/HEARTBEAT_INTERVAL_MS_CONFIG (str heartbeat-interval-ms)))))))

(defn normalize-strategies
  "Takes a collection of strategy-protocol-implementing-objects or strategy-keyword-declaration-vectors and turns the strategy-keyword-declaration-vectors into strategy-protocol-implementing-objects."
  [ss]
  (into []
        (map (fn [s]
               (if (satisfies-some-of-the-strategy-protocols s) s
                   (create-strategy s))))
        ss))

(comment

  (normalize-strategies [(StringSerializer)
                         (AutoCommitOffsets)
                         [:strategy/FreshConsumerGroup]
                         [:strategy/SubscribeWithTopicsCollection ["test"]]
                         (JsonSerializer :subscriber :both)])

  
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
                        :strategy-keyword
                        #(try (and (seq %1)
                                   (keyword? (first %1))
                                   (= "strategy" (namespace (first %1))))
                              (catch Throwable _ nil))))
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
                    (reverse)
                    (apply comp)) starting-cfg)

        producer (KafkaProducer. ^Map props)

        ;; turn the pre-produce middleware strategies into lambdas
        ;; then compose them in reverse order, so that strategies defined
        ;; first, gets called first.
        pre-produce-middleware (->> strategies
                                    (filter #(satisfies? IProducerPreProduceMiddleware %))
                                    (map #(partial pre-produce-hook %))
                                    (reverse)
                                    (apply comp))]
    (reify
      IProducer
      (produce! [_ msgs]
        (producer-produce producer
                          (pre-produce-middleware msgs)))
      (get-producer [_] producer)

      IConsumedResultsHandler
      (handle-consumption-results
          [this msgs]

        (log/warn "Using a producer component's `IConsumedResultsHandler` implementation is now considered harmful.")

        (let [xs (s/conform ::producer-consumption-results-xs-spec msgs)]
          (when (= ::s/invalid xs)
            ;; horrible place to throw, but this is the best we've got.
            (throw (ex-info "The producer can only handle the result of consume-messages, if the value is either a single message to be produced, or a collection of messages to be produced."
                            {::explain-str (s/explain-str ::producer-consumption-results-xs-spec msgs)
                             ::explain-data (s/explain-data ::producer-consumption-results-xs-spec msgs)})))
          (let [[xs-type _] xs]
            (produce! this
                      (condp = xs-type
                        :single [msgs]
                        :more   msgs)))))

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

(-prom/register-metric (prom/counter ::consumer-main-msgs-consumed
                                     {:description "All messages consumed by kafka consumer-main."
                                      :labels [:topic]}))
(-prom/register-metric (prom/counter ::consumer-poll
                                     {:description "Incemented every time a poll call is completed on the consumer."
                                      :labels [:consumer-group-id]}))

(-prom/register-metric (prom/summary ::consumer-lag:measure-cost
                                     {:description "Measurement of how many milliseconds it takes to measure the consumer lag and report it."
                                      :labels [:consumer-group-id]}))
;; part of commented-out code below.
;; This technique of determining lag on the consumer is not smart.
;; It should probably be a strategy.
(-prom/register-metric (prom/gauge ::consumer-partition-lag
                                   {:description "The lag in offsets per consumer-group-id and partition."
                                    :labels [:consumer-group-id
                                             :topic
                                             :partition]}))

(defn -consumer-main
  [^Consumer consumer
   must-stop
   & {{:consumer/keys        [client]
       :keys                 [strategies]
       poll-timeout          :consumer.poll/timeout
       :as                   consumer-config
       :or                   {poll-timeout 1000}} :consumer-config}]

  (s/assert ::consumer-config consumer-config)

  (let [post-init-hooks
        (into []
              (filter (partial satisfies? IConsumerPostInitHook))
              strategies)

        combined-post-init-hooks
        (fn [consumer]
          (doseq [s post-init-hooks]
            (post-init-hook s consumer)))

        post-consume-hooks
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
            (handle-consumption-results h xs)))


        ;; compose up all of the interceptors into one fn chain
        consumer-messages-pre-processor-chain
        (->> strategies
             (filter (partial satisfies? IConsumerMessagesPreProcessor))
             (map #(partial pre-process-messages %))
             (reverse)
             (apply comp))]
    (try

      (combined-post-init-hooks consumer)

      (let [consumer-group-id (.groupId (.groupMetadata ^Consumer consumer))]

        (while (not @must-stop)
          (let [consume-id                 (random-uuid)]
            (log/with-context+ {:consume-id consume-id}
              (let [consumed-records           (into []
                                                     (map (fn [^ConsumerRecord r]
                                                            (let [hdrs (into []
                                                                             (comp (map (juxt #(.key ^Header %) #(.value ^Header %)))
                                                                                   (map deserialize-consumer-record-header*))
                                                                             (-> r (.headers) (.toArray)))]
                                                              (cond->
                                                                  {:topic     (.topic r)
                                                                   :partition (.partition r)
                                                                   :offset    (.offset r)
                                                                   :value     (.value r)
                                                                   :key       (.key r)
                                                                   :timestamp (t/instant (.timestamp r))}
                                                                (seq hdrs) (assoc :headers hdrs)))))
                                                     (.poll consumer ^long poll-timeout))

                    ;; request the consumer lag on this consumer's topic-partition assignment
                    ;; and record it as a guage, per topic and per partition
                    ;; ALSO - measure how long it takes to make this measurement
                    _ (let [start-millis (System/currentTimeMillis)]
                        (doseq [^TopicPartition assignment (.assignment consumer)
                                :let [assignment-lag (.currentLag consumer assignment)]
                                :when (not (.isEmpty assignment-lag))]
                          (prom/set (get-gauge-consumer-partition-lag {:consumer-group-id consumer-group-id
                                                                       :topic             (.topic assignment)
                                                                       :partition         (.partition assignment)})
                                    (.getAsLong assignment-lag)))
                        (let [end-millis (System/currentTimeMillis)
                              duration (t/duration (- end-millis start-millis))]
                          ;; (log/debug (str "Measuring consumer lag and setting gauge took " duration))
                          (prom/observe (get-summary-consumer-lag:measure-cost {:consumer-group-id consumer-group-id})
                                        (- end-millis start-millis))))


                    _ (prom/inc (get-counter-consumer-poll {:consumer-group-id consumer-group-id}))
                    ;; register prometheus metrics for msgs consumed per topic
                    _ (doseq [[topic msgs-count] (into {}
                                                       (x/by-key :topic x/count)
                                                       consumed-records)]
                        (prom/inc (get-counter-consumer-main-msgs-consumed {:topic topic})
                                  msgs-count))
                    consumption-results        (->> consumed-records
                                                    (consumer-messages-pre-processor-chain)
                                                    (consume-messages client))]

                (when consumption-results
                  (combined-consumed-results-handler consumption-results))

                (combined-post-consume-hook consumer consumed-records))))))

      ;; the value we are returning
      [:done true]
      (catch Throwable t
        [:error t])
      (finally
        ;; we're done poll'ing and shutting down
        ;; give shutdown hooks a chance
        (doseq [s (filter (partial satisfies? IShutdownHook)
                          strategies)]
          (try (shutdown-hook s consumer)
               (catch Throwable t
                 (error t (str "Exception while calling shutdown-hook.")))))

        ;; close the consumer. this commits and exits cleanly
        (.close consumer)))))

(defn- background-wait-on-stop-signal
  "Extracted only because the debugger chokes on core.async."
  [consumer must-stop
   {:as   consumer-config
    :keys [service-health-trip-switch]}
   consumer-properties has-stopped]
  (let [consumer-group-id (get consumer-properties ConsumerConfig/GROUP_ID_CONFIG "<UNAVAILABLE>")]
    (csp/go
      (let [[status xtra :as thread-result]
            (csp/<! (csp/thread (log/with-context+ {:consumer-group-id consumer-group-id}
                                  (-consumer-main consumer
                                                  must-stop
                                                  :consumer-config consumer-config
                                                  :consumer-properties consumer-properties))))]
        (when (= :error status)
          (error xtra ;; hopefully, an exception packaged with try/catch in -consumer-main
                 (format "Kafka consumer main thread finished with exception. [consumer-group-id '%s'] Tripping the health switch..."
                         consumer-group-id))
          (-health/indicate-unhealthy! service-health-trip-switch ::kafka-consumer))

        ;; Anyway deliver the value into the promise.
        ;; At the time this code is written, the return value is not used.
        (deliver has-stopped
                 (or xtra status thread-result))))))

(defn make-consumer
  [{:as            consumer-config
    :keys          [bootstrap-server
                    strategies]
    :consumer/keys [mock]}]
  (s/assert ::consumer-config consumer-config)
  (let [strategies          (normalize-strategies strategies)
        consumer-config     (assoc consumer-config :strategies strategies)
        consumer-properties {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-server}
        consumer-properties ((->> strategies
                                  (filter #(satisfies? IUpdateConsumerConfigHook %))
                                  (map #(partial update-consumer-cfg-hook %))
                                  (reverse) ;; we need to preserve the order the strategies were specified in (apply comp reverses the order)
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

        ;; this is throwing a new exception in some cases
        ;; part of an experiment to close the consumer in the main consumer's thread
        #_(.close ^Consumer consumer)

        ))))

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
                               (reverse)
                               (apply comp)) admin-client-cfg)
        ac (AdminClient/create ^java.util.Map admin-client-cfg)]
    (reify
      IDeref
      (deref [_] ac)

      IHaltable
      (halt [_] (.close ac)))))

(-comp/defcomponent {::-comp/ig-kw ::admin-client
                     ::-comp/config-spec  ::admin-client-cfg}
  [cfg] (make-admin-client cfg))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn increase-topic-partitions!
  "A common utility that takes a collection of topic names and a desired partition count.
  Will query the topics to find topics with too few partitions and then increase them in-place.
  Throws for topics that does not exist."
  [admin-client topic-names partition-count]

  (let [describe-result (-> (.describeTopics admin-client
                                             topic-names)
                            (.allTopicNames)
                            (.get))

        topics-createPartitions-parameter
        (into {}
              (keep (fn [topic-name]
                      (let [^TopicDescription topic-describe-result
                            (get describe-result topic-name)

                            max-partition
                            (apply max (map #(.partition ^TopicPartitionInfo %)
                                            (.partitions topic-describe-result)))]
                        (when (< max-partition (dec partition-count))
                          [topic-name (NewPartitions/increaseTo partition-count)]))))
              topic-names)]

    (when (seq topics-createPartitions-parameter)
      (-> admin-client
          (.createPartitions topics-createPartitions-parameter)
          (.all)))))

(defn ^{:deprecated    true
        :superseded-by "assert-topics!"} assert-topics
  "SUPERSETED-BY `assert-topics!`
  Makes sure topics exist with the given number of partitions.
  If a topic exists and has fewer partitions than specified with `nr-of-partitions`, the partitions will be
  increased to match (when `increase-existing-partitions?` is `true`)."
  [^AdminClient admin-client
   desired-topics
   & {:keys [nr-of-partitions
             increase-existing-partitions?]
      :or   {increase-existing-partitions? false}}]
  (let [existing-topics (-> admin-client
                            (.listTopics)
                            (.names)
                            (.get)
                            (set))
        new-topics (into []
                         (comp
                          (distinct)
                          (filter #(not (existing-topics %)))
                          (map (fn [topic-name]
                                 (info (format "Creating topic '%s' with nr-partitions '%s' and replication-factor '%s'."
                                               topic-name
                                               (str (or nr-of-partitions "CLUSTER_DEFAULT"))
                                               "CLUSTER_DEFAULT"))
                                 (NewTopic. ^String topic-name
                                            ^java.util.Optional
                                            (if nr-of-partitions
                                              (Optional/of (Integer. ^long nr-of-partitions))
                                              (Optional/empty))
                                            ^java.util.Optional
                                            (Optional/empty)))))
                         desired-topics)
        topic-create-result (.createTopics admin-client new-topics)

        ^java.util.Collection existing-topics-to-check
        (set/intersection existing-topics
                          (set desired-topics))]

    (when increase-existing-partitions?
      (increase-topic-partitions! admin-client
                                  existing-topics-to-check
                                  nr-of-partitions))

    ;; wait for complete success
    (-> topic-create-result
        (.all)
        (.get))
    nil))

;; this is the new preferred name
(defn assert-topics!
  "Makes sure topics exist with the given number of partitions.
  If a topic exists and has fewer partitions than specified with `nr-of-partitions`, the partitions will be
  increased to match (when `increase-existing-partitions?` is `true`)."
  [^AdminClient admin-client
   desired-topics
   & {:keys [nr-of-partitions
             increase-existing-partitions?]
      :or   {increase-existing-partitions? false}
      :as   opts}]
  (assert-topics admin-client desired-topics opts))

(defn delete-topics!
  [^AdminClient admin-client
   topics-to-be-deleted]
  (let [existing-topics (-> admin-client
                            (.listTopics)
                            (.names)
                            (.get)
                            (set))
        deletable-topics (set/intersection (set topics-to-be-deleted)
                                           (set existing-topics))

        topic-delete-result (.deleteTopics admin-client deletable-topics)]

    ;; wait for complete success
    (-> topic-delete-result
        (.all)
        (.get))))

(s/def ::increase-existing-partitions? (s/or :b boolean?))
(s/def ::nr-of-partitions (s/or :nil nil?
                                :i   pos-int?
                                :s   #(try (Integer/parseInt %)
                                           (catch NumberFormatException _ false))))
(s/def ::topic-asserter-cfg (s/and ::admin-client-cfg
                                   (s/keys :req-un [::topic-name-providers]
                                           :opt-un [::nr-of-partitions
                                                    ::increase-existing-partitions?])))

(-comp/defcomponent {::-comp/ig-kw       ::topic-asserter
                     ::-comp/config-spec ::topic-asserter-cfg}
  [{:as   cfg
    :keys [topic-name-providers
           nr-of-partitions
           increase-existing-partitions?]
    :or   {increase-existing-partitions? false}}]

  (let [^java.lang.Integer
        nr-of-partitions (or (when (and nr-of-partitions
                                        (string? nr-of-partitions))
                               (Integer/parseInt nr-of-partitions))
                             (when (number? nr-of-partitions)
                               (Integer. nr-of-partitions))
                             nr-of-partitions)
        ac (make-admin-client cfg)]

    (assert-topics! @ac
                    (mapcat #(get-topic-names %) topic-name-providers)
                    :nr-of-partitions              nr-of-partitions
                    :increase-existing-partitions? increase-existing-partitions?)

    ;; stop the admin client
    (-comp/halt ac)

    ;; we need to return something for the component value, let's just use the config until something better comes about
    cfg))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::topic (s/and string?
                      (comp pos-int? count)))
(s/def ::topics (s/coll-of ::topic))
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
(s/def ::ktable-max-compaction-lag-ms ::topic-delete-retention-ms)
(s/def ::ktable-min-cleanable-dirty-ratio (s/or :n nil?
                                                :s (s/and string?
                                                          #(pos-int? (count %))
                                                          #(try (let [d (Double/parseDouble %)]
                                                                  (< 0.0 d 1.0))
                                                                (catch NumberFormatException _ false)))
                                                :d (s/and double?
                                                          #(< 0.0 % 1.0))))
(s/def ::recreate-topics-with-bad-config boolean?)
(s/def ::update-topics-with-bad-config boolean?)
(s/def ::ktable-compaction-policy #{;; normal compaction, keeps the last value of records by key
                                    "compact"
                                    ;; keeps the last value of a record by key, for the retention period
                                    ;; If no updates are received, the record is deleted
                                    "delete,compact"
                                    ;; same as `delete,compact`
                                    "compact,delete"})
(s/def ::ktable-asserter-cfg (s/and ::admin-client-cfg
                                    (s/keys :req-un [::topic-name-providers]
                                            :opt-un [::nr-of-partitions
                                                     ::topic-delete-retention-ms
                                                     ::ktable-segment-ms
                                                     ::ktable-min-cleanable-dirty-ratio
                                                     ::recreate-topics-with-bad-config
                                                     ::update-topics-with-bad-config
                                                     ::ktable-max-compaction-lag-ms
                                                     ::ktable-compaction-policy
                                                     ::increase-existing-partitions?])))

(-comp/defcomponent {::-comp/ig-kw       ::ktable-asserter
                     ::-comp/config-spec ::ktable-asserter-cfg}
  [{:as   cfg
    :keys [topic-name-providers
           nr-of-partitions
           topic-delete-retention-ms
           ktable-segment-ms
           ktable-min-cleanable-dirty-ratio
           ktable-max-compaction-lag-ms
           update-topics-with-bad-config
           ktable-compaction-policy
           increase-existing-partitions?]
    :or   {ktable-compaction-policy        "compact"}}]

  (when-not (nil? (:recreate-topics-with-bad-config cfg))
    (log/info "KTable asserter option `recreate-topics-with-bad-config` has been deprecated in favour of `update-topics-with-bad-config`."))

  (let [update-topics-with-bad-config (or (when-not (nil? update-topics-with-bad-config)
                                            update-topics-with-bad-config)
                                          (let [recreate-topics-with-bad-config (:recreate-topics-with-bad-config cfg)]
                                            (when-not (nil? recreate-topics-with-bad-config)
                                              recreate-topics-with-bad-config)))
        ktable-compaction-policy (if (= ktable-compaction-policy "delete,compact")
                                   "compact,delete" ;; normalize on this order
                                   ktable-compaction-policy)
        nr-of-partitions (or (when (and nr-of-partitions
                                        (string? nr-of-partitions))
                               (Integer/parseInt nr-of-partitions))
                             (when (number? nr-of-partitions)
                               (Integer. nr-of-partitions))
                             nr-of-partitions)
        ac (make-admin-client cfg)
        existing-topics (-> ^AdminClient @ac
                            (.listTopics)
                            (.names)
                            (.get)
                            (set))

        desired-topics (set (mapcat #(get-topic-names %) topic-name-providers))

        ;; make sure topics have the correct number of partitions
        _ (when increase-existing-partitions?
            (increase-topic-partitions! @ac
                                        (set/intersection desired-topics
                                                          existing-topics)
                                        nr-of-partitions))

        ;; we can't CHANGE a topic that has been created the wrong way
        ;; and neither can we let it be.
        ;; The app is in a broken state if a topic that must be compacted is not.
        topics-with-wrong-config
        (->> (set/intersection desired-topics
                               existing-topics)
             (map #(ConfigResource. ConfigResource$Type/TOPIC %))
             (.describeConfigs ^AdminClient @ac)
             (.all)
             (.get)
             (map (fn [[^ConfigResource resource ^Config cfg]]
                    [(.name resource)
                     (.value (.get cfg "cleanup.policy"))]))
             (filter (fn [[_ topic-compaction-strategy]]
                       (let [topic-compaction-strategy (if (= topic-compaction-strategy
                                                              "delete,compact")
                                                         "compact,delete" ;; normalize
                                                         topic-compaction-strategy)]
                         (not= ktable-compaction-policy
                               topic-compaction-strategy))))
             (map first)
             vec)

        ;; we want to either alert (by throwing) that a topic is misconfigured
        ;; or we will modify the topic in-place. governed by `update-topics-with-bad-config` flag.
        _ (when (pos-int? (count topics-with-wrong-config))
            (if-not update-topics-with-bad-config
              (throw (ex-info (format "These topics must be created with topic config 'cleanup.policy' == '%s', but they are not. Cannot continue.\n%s"
                                      ktable-compaction-policy
                                      (str topics-with-wrong-config))
                              {:topics         topics-with-wrong-config
                               :desired-policy ktable-compaction-policy}))

              ;; here the topics will be modified
              (do
                (warn (format "These topics [%s] were created incorrectly ('cleanup.policy' != '%s'). They will now be modified. (Control this behaviour with component setting 'recreate-topics-with-bad-config'.)"
                              (str/join "," (map #(str "'" % "'") topics-with-wrong-config))
                              ktable-compaction-policy))

                (let [change-spec [(AlterConfigOp. (ConfigEntry. "cleanup.policy"
                                                                 ktable-compaction-policy)
                                                   AlterConfigOp$OpType/SET)]]
                  (-> (.incrementalAlterConfigs ^AdminClient @ac
                                                (into {}
                                                      (map (fn [topic-name]
                                                             [(ConfigResource. ConfigResource$Type/TOPIC
                                                                               topic-name)
                                                              change-spec]))
                                                      topics-with-wrong-config))
                      (.all))))))



        new-topics (set/difference (set desired-topics)
                                   existing-topics)
        topic-create-result (->> new-topics
                                 (map (fn [topic-name]
                                        (info (format "Creating log-compacted topic '%s' with nr-partitions '%s', replication-factor '%s' & cleanup.policy = '%s'."
                                                      topic-name
                                                      (str (or nr-of-partitions "CLUSTER_DEFAULT"))
                                                      "CLUSTER_DEFAULT"
                                                      ktable-compaction-policy))
                                        (let [new-topic (NewTopic. ^String   topic-name
                                                                   ^Optional (if nr-of-partitions
                                                                               (Optional/of nr-of-partitions)
                                                                               (Optional/empty))
                                                                   ^Optional (Optional/empty))
                                              _ (.configs new-topic
                                                          (cond-> {"cleanup.policy" ktable-compaction-policy}
                                                            topic-delete-retention-ms        (assoc "delete.retention.ms" (str topic-delete-retention-ms))
                                                            ktable-segment-ms                (assoc "segment.ms" (str ktable-segment-ms))
                                                            ktable-min-cleanable-dirty-ratio (assoc "min.cleanable.dirty.ratio" (str ktable-min-cleanable-dirty-ratio))
                                                            ktable-max-compaction-lag-ms     (assoc "max.compaction.lag.ms" (str ktable-max-compaction-lag-ms))
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

(defn msgs->topic-partition-maxoffsets
  "Accepts a sequence of kafka messages, and works out the max offset for every topic and partition.
  Result is map of string (topic) to map of int (partition) to int (max offset)."
  [msgs]
  (into {}
        (comp (map #(dissoc % :value))
              (x/by-key :topic
                        (comp (x/by-key :partition
                                        (comp (map :offset)
                                              x/max))
                              (x/into {}))))
        msgs))

(defn combine-topic-partition-maxoffsets
  "Accepts to topic-partition-maxoffset values, and combines them by keeping the max offset for each topic-partition."
  [topic-partition-offset-acc topic-partition-offset-x]
  (merge-with (fn [r l] (merge-with max r l))
              topic-partition-offset-acc
              topic-partition-offset-x))

(comment

  (def x1 (msgs->topic-partition-maxoffsets [{:topic "a"
                                              :partition 1
                                              :offset 2}
                                             {:topic "a"
                                              :partition 1
                                              :offset 4}
                                             {:topic "b"
                                              :partition 1
                                              :offset 5}
                                             {:topic "b"
                                              :partition 1
                                              :offset 6}
                                             {:topic "c"
                                              :partition 1
                                              :offset 7}]))
  (def x2 (msgs->topic-partition-maxoffsets [{:topic "a"
                                              :partition 1
                                              :offset 11}
                                             {:topic "a"
                                              :partition 1
                                              :offset 12}
                                             {:topic "b"
                                              :partition 1
                                              :offset 15}
                                             {:topic "b"
                                              :partition 1
                                              :offset 16}
                                             {:topic "c"
                                              :partition 1
                                              :offset 17}]))

  (combine-topic-partition-maxoffsets x1 x2)


  )

(defn max-instant
  "Returns the more-recent of two java.time.<moment-in-time> values."
  [a b]
  (cond
    (not (or a b)) nil
    (and a (not b)) a
    (and (not a) b) b

    (t/after? a b) a
    :else          b))

(comment

  (max-instant nil nil) ;; nil
  (def a (t/instant "2024-09-27T12:00:00.00-00:00"))
  (def b (t/instant "2024-09-27T13:00:00.00-00:00"))

  (max-instant a b) ;; #inst "2024-09-27T13:00:00.000000000-00:00"
  (max-instant b a) ;; #inst "2024-09-27T13:00:00.000000000-00:00"
  (max-instant nil a) ;; #inst "2024-09-27T12:00:00.000000000-00:00"
  (max-instant b nil) ;; #inst "2024-09-27T13:00:00.000000000-00:00"

  ;; the pramaters don't HAVE to be instants, but they have to be the same
  (max-instant (t/zoned-date-time a "UTC") ;; #object[java.time.ZonedDateTime 0x23bea94 "2024-09-27T12:00Z[UTC]"]
               (t/zoned-date-time b "UTC") ;; #object[java.time.ZonedDateTime 0x47c8930b "2024-09-27T13:00Z[UTC]"]
               ) ;; #object[java.time.ZonedDateTime 0x3f360e49 "2024-09-27T13:00Z[UTC]"]


  )

(defn merge-updates-with-ktable
  "Acts like a reduce function.
  Accepts a starting-state (`old-ktable`) and a collection of kafka-records (`new-msgs`),
  Merges the `new-messages` into a map, which is returned.
  The state has the shape {topic-name -> {record-key latest-record-value}}.
  Each `latest-record-value` has meta-data including `:headers` & `:timestamp`.
  The result value (state) has meta-data
  - `:ktable/topic-partition-offsets`
  - `:ktable/record-headers`
  that records the max offset, per [topic partition] encountered while building the ktable value.
  Accepts an optional `opts` parameter:
  - :retention-ms -- If a `latest-record-value` was created from a kafka-record wit a timestamp
  thas is older than latest-received-kafka-record-timestamp, then this record will be removed from the ktable."
  ([old-ktable new-msgs]
   (merge-updates-with-ktable old-ktable new-msgs {}))
  ([old-ktable new-msgs {:as   _opts
                         :keys [retention-ms]}]
   (let [[result max-timestamp]
         (loop [old                           old-ktable
                [{:as  head
                  k    :key
                  v    :value
                  t    :topic
                  p    :partition
                  o    :offset
                  ts   :timestamp
                  hdrs :headers} & rest-msgs] new-msgs
                max-timestamp                 ts]
           (let [new-max-timestamp (max-instant max-timestamp ts)]
             (if-not head
               [old max-timestamp]
               (recur (vary-meta (let [v-meta-data (cond-> {}
                                                     hdrs (assoc :headers hdrs)
                                                     ts   (assoc :timestamp ts)
                                                     p    (assoc :partition p)
                                                     o    (assoc :offset o))
                                       v (if (and v (instance? clojure.lang.IMeta v))
                                           (with-meta v v-meta-data)
                                           v)]
                                   (cond
                                     ;; nothing is nil, save the value
                                     (not (or (nil? k)
                                              (nil? v)
                                              (nil? t)))
                                     (assoc-in old [t k] v)

                                     ;; value (only) is nil, remove the value (tombstone)
                                     (and (nil? v)
                                          (not (or (nil? t)
                                                   (nil? k))))
                                     (update old t (fnil #(dissoc % k) {}))

                                     ;; default, return old value
                                     :else
                                     (do
                                       (warn (format "merge-update-with-ktable does not have logic for this case: topic='%s', key='%s', value='%s'"
                                                     (str t) (str k) (str v)))
                                       old)))
                                 (fn [old-meta]
                                   (cond-> old-meta

                                     ;; If we have an offset and a partition (int) value we will store it,
                                     ;; keeping the max of all offsets per partition we've encountered.
                                     ;; `max` throws with `nil` so we need a non-nil value to compare
                                     ;; the encountered offset with. Choose `-1` because it is less than
                                     ;; any offset we will encounter (ie invalid offset value) but still
                                     ;; an int that will not throw inside max.
                                     (and o p)
                                     (update-in [:ktable/topic-partition-offsets t p]
                                                (fnil #(max % o)
                                                      -1))

                                     ;; in retrospect, `:ktable/recerd-headers` was not a great idea, as it's too limiting
                                     ;; `:ktable/record-data` is better since it captures all of the kafka record data,
                                     ;; instead of just some of it.
                                     (or o p ts hdrs)
                                     (assoc-in [:ktable/record-data t k] (->> {:offset    o
                                                                               :partition p
                                                                               :timestamp ts
                                                                               :headers   hdrs}
                                                                              (remove (comp nil? second))
                                                                              (into {})))

                                     hdrs
                                     (assoc-in [:ktable/record-headers t k]
                                               hdrs)

                                     (not hdrs)
                                     (update-in [:ktable/record-headers t]
                                                #(dissoc % k)))))
                      rest-msgs
                      new-max-timestamp))))]

     ;; if the user specified `retention-ms` AND we have `max-timestamp`
     ;; we may now expunge data that is older than `retention-ms`
     (if (or (not retention-ms)
             (not max-timestamp))
       result
       ;; NOTE: strategy is to search through kafka records' meta-data for `:timestamp` that is long enough ago
       ;; if we find such that qualify, we want to remove that entry and also the meta-data on the ktable
       ;; object itself, related to that entry `[:ktable-record-headers t k]`
       (let [cutoff-timestamp (t/- max-timestamp
                                   (t/duration retention-ms))
             ;; We need the path [topic record-key] to the records we will expunge
             ;; so we can expunge table-level meta-data too
             topic-partition-keys (map (juxt first second)
                                       (specter/select [:ktable/record-data
                                                        specter/ALL (specter/collect-one specter/FIRST) specter/LAST ;; collect topic name, continue to topic value-map
                                                        specter/ALL (specter/collect-one specter/FIRST) specter/LAST ;; collect record key, continue to record value
                                                        :timestamp                                                   ;; navigate to :timestamp
                                                        #(t/before? % cutoff-timestamp)                              ;; match only when :timestamp is before cutoff
                                                        ]
                                                       (meta result)))
             result-without-records (reduce (fn [acc [topic record-key]]
                                              (update acc topic dissoc record-key))
                                            result
                                            topic-partition-keys)
             result-with-fixed-meta-data (vary-meta result-without-records
                                                    (fn [old-meta-data]
                                                      (assoc old-meta-data
                                                             :ktable/record-headers
                                                             (reduce (fn [record-headers [topic record-key]]
                                                                       (update record-headers topic dissoc record-key))
                                                                     (:ktable/record-headers old-meta-data)
                                                                     topic-partition-keys)

                                                             :ktable/record-data
                                                             (reduce (fn [record-data [topic record-key]]
                                                                       (update record-data topic dissoc record-key))
                                                                     (:ktable/record-data old-meta-data)
                                                                     topic-partition-keys))))]
         result-with-fixed-meta-data)))))

(comment

  (def data {"one"   {"1" {:a 1}
                      "2" {:a 2}
                      "3" {:a 3}}
             "two"   {"1" {:a 1}
                      "2" {:a 2}
                      "5" {:a 5}}
             "three" {"1" {:a 1}
                      "7" {:a 7}}})

  (specter/select [specter/ALL (specter/collect-one specter/FIRST) specter/LAST
                   specter/ALL (specter/collect-one specter/FIRST) specter/LAST
                   :a odd?]
                  data)

  [["one" "1" 1] ["one" "3" 3] ["two" "1" 1] ["two" "5" 5] ["three" "1" 1] ["three" "7" 7]]


  )

(defn ktable-checkpoint-seek-strategy
  "Implements a kafka strategy specifically for seeking to partition-offsets
  so that the ktable can resume from a checkpoint."
  [checkpoint-value]
  (let [topic-partition-offsets (-> checkpoint-value
                                    (meta)
                                    :ktable/topic-partition-offsets)]
    (reify
      IConsumerPostInitHook
      (post-init-hook [_ consumer]
        ;; poll once, to achieve a topic-partition assignment
        ;; This _is_ a ktable, so the assumption is that this is for _all_ partitions...
        (.poll ^Consumer consumer 1000)
        (doseq [^TopicPartition tp (.assignment ^Consumer consumer)
                :let [t                     (.topic tp)
                      p                     (.partition tp)
                      offset-to-resume-from (get-in topic-partition-offsets [t p])]
                :when offset-to-resume-from]
          (log/with-context+ {:topic     t
                              :partition p
                              :offset    offset-to-resume-from}
            (log/info "KTable checkpoint loading is resuming ktable consumption from offset."))
          (.seek ^Consumer consumer
                 tp
                 ;; we seek to at least one higher offset than what we are aware of, so we can start resuming from the new data
                 (inc offset-to-resume-from)))))))

(s/def ::ktable-id (s/and string?
                          #(pos-int? (count %))))

(s/def ::caught-up-once? boolean?)
(s/def ::retention-ms pos-int?)
(s/def ::ktable-checkpoint-storage #(satisfies? -ktable-checkpoints/IKTableCheckpointStorage %))
(s/def ::ktable-cfg (s/and ::clientless-consumer
                           (s/keys :req-un [::ktable-id]
                                   :opt-un [::caught-up-once?
                                            ::retention-ms
                                            ::-time/clock
                                            ::ktable-checkpoint-storage])
                           ;; when retention-ms is specified, clock must be too
                           (fn [{:keys [retention-ms clock]}]
                             (or (and retention-ms clock)
                                 (not retention-ms)))))

(defprotocol IKTable
  (ktable-wait-for-catchup
    [_this topic-partition-offset timeout-duration timeout-value]
    "Waits until a ktable has consumed messages from its source topics, at least up to the topic-partition-offset data parameter.
Supports a timeout operation. `timeout-duration` must be a java.time.Duration.")
  (ktable-wait-until-fully-current
    [_this timeout-duration timeout-value]
    "Waits until the current offset is also the latest offset. This might time out if there are still active producers to the ktable, and the consumer struggle to fully catch up as a result."))

(s/def ::ktable #(satisfies? IKTable %))

(defn ktable-atom-wait-for-catchup
  "Will use csp to actually wait up to timeout-duration for the ktable value to reflect changes up to this level."
  [ktable-atom topic-partition-offset timeout-duration timeout-value]
  (let [timeout-ms (.toMillis ^java.time.Duration timeout-duration)
        timeout-chan (csp/timeout timeout-ms)

        initial-ktable-value @ktable-atom

        ktable->topic-partition-offsets
        (fn [ktable-value] (:ktable/topic-partition-offsets (meta ktable-value)))

        caught-up-already? (fn [ktable-value]
                             (let [ktable-topic-partition-offsets
                                   (ktable->topic-partition-offsets ktable-value)]

                               (->> topic-partition-offset
                                    (mapcat (fn [[topic partition-offset]]
                                              (map (fn [[partition offset]] [topic partition offset])
                                                   partition-offset)))
                                    (keep (fn [[topic partition offset]]
                                            (let [ktable-progress-offset (get-in ktable-topic-partition-offsets
                                                                                 [topic partition])]
                                              (when (and ktable-progress-offset
                                                         (< ktable-progress-offset offset))
                                                :not-caught-up-yet))))
                                    (count)
                                    (zero?))))]
    (cond
      (caught-up-already? initial-ktable-value)
      (ktable->topic-partition-offsets initial-ktable-value)

      :else
      (let [watch-id (keyword "ktable-atom-wait-for-catchup" (str (random-uuid)))
            caught-up?-chan (csp/chan)
            new-ktable-ch (csp/chan (csp/sliding-buffer 1))]

        ;; notice changes when they happen
        (add-watch ktable-atom watch-id (fn [_ _ _ _] (csp/>!! new-ktable-ch true)))

        ;; do the job of waiting on a background thread
        (csp/go
          (loop [[_v ch] (csp/alts! [new-ktable-ch
                                     timeout-chan])]
            (if (= ch timeout-chan)
              (do (csp/>! caught-up?-chan timeout-value)
                  (csp/close! caught-up?-chan)
                  timeout-value)
              (let [ktable-value @ktable-atom]
                (if (caught-up-already? ktable-value)
                  (do (csp/>! caught-up?-chan (ktable->topic-partition-offsets ktable-value))
                      (csp/close! caught-up?-chan))
                  (recur (csp/alts! [new-ktable-ch
                                     timeout-chan
                                     (csp/timeout (long (/ timeout-ms 10)))]))))))
          ;; cleanup with the watch
          (remove-watch ktable-atom watch-id))

        ;; kick of the process
        (csp/go (csp/>! new-ktable-ch true))

        ;; wait for the result or the timeout-value to arrive
        (csp/<!! caught-up?-chan)))))

(-comp/defcomponent {::-comp/config-spec ::ktable-cfg
                     ::-comp/ig-kw       ::ktable}
  [{:as cfg
    :keys [ktable-id
           caught-up-once?
           retention-ms
           clock
           ktable-checkpoint-storage]
    :or   {caught-up-once? false}}]

  (let [consumer-group-id (str  ktable-id
                                "-"
                                (UUID/randomUUID))
        caught-up-ch (csp/chan)
        has-caught-up-once (promise)
        _ (csp/go (csp/<! caught-up-ch)
                  (deliver has-caught-up-once true)
                  (csp/close! caught-up-ch))

        ;; `caught-up-once?` mechanism is a way to block the initial ktable (deref) call until the ktable consumer
        ;; was current at least once, relative to when the consumer started.
        ;; latest-offsets are checked initially, and when the consumer's current-offset surpasses this, the
        ;; check is passed. It is special because it does not check if the consumer is fully
        ;; current all the time, but instead checks if the consumer caught up to where the offsets were
        ;; at the time of starting. It is a type of "caught up enough to be useful" check.

        ;; If `caught-up-once?` is false, then the first deref will only return if the consumer's
        ;; current-offset is the same as latest-offset, which could be tricky of a producer is busy producing
        ;; records to this topic while the first deref is waiting.

        fully-current-notification-ch (csp/chan (csp/sliding-buffer 1))
        caught-up-notifications-strategy (apply CaughtUpNotifications
                                                (remove nil? [fully-current-notification-ch
                                                              (when-not caught-up-once?
                                                                caught-up-ch)]))
        ktable-initial-value (or (when ktable-checkpoint-storage
                                   (-ktable-checkpoints/retrieve-latest-checkpoint ktable-id
                                                                                   ktable-checkpoint-storage))
                                 {})
        ktable-state (atom ktable-initial-value)

        merge-updates-opts (cond-> {}
                             retention-ms (assoc :retention-ms retention-ms))
        consumer-client (reify
                          IConsumerClient
                          (consume-messages
                              [_ msgs]
                            (when (seq msgs)
                              (let [latest-ktable-value
                                    (swap! ktable-state
                                           #(merge-updates-with-ktable % msgs merge-updates-opts))]
                                (when (and ktable-checkpoint-storage
                                           (realized? has-caught-up-once))
                                  ;; we only want to consider saving checkpoints after we've caught up fully
                                  ;; This `register-ktable-value` is called after _every_ update to the ktable value.
                                  ;; We are depending on the implementation to store only a subset of registered ktable values.
                                  (-ktable-checkpoints/register-ktable-value ktable-checkpoint-storage
                                                                             ktable-id
                                                                             latest-ktable-value))))))

        seek-strategy (cond
                        (and ktable-checkpoint-storage
                             (seq ktable-initial-value))
                        (ktable-checkpoint-seek-strategy ktable-initial-value)

                        retention-ms
                        (let [seek-to-timestamp (t/- (-time/get-current-time clock)
                                                     (t/duration retention-ms))]
                          (SeekToTimestampOffset seek-to-timestamp))

                        :else nil)

        cfg (-> cfg
                (update-in [:strategies] concat (remove nil?
                                                        [(OffsetReset "earliest")
                                                         (ConsumerGroup consumer-group-id)
                                                         (when caught-up-once? (CaughtUpOnceNotifications caught-up-ch))
                                                         caught-up-notifications-strategy
                                                         seek-strategy]))
                (assoc :consumer/client consumer-client))

        consumer (make-consumer cfg)]

    (reify
      IHaltable
      (halt [_] (-comp/halt consumer))

      IKTable
      (ktable-wait-for-catchup [_ topic-partition-offset timeout-duration timeout-value]
        (ktable-atom-wait-for-catchup ktable-state topic-partition-offset timeout-duration timeout-value))
      (ktable-wait-until-fully-current [_ timeout-duration timeout-value]
        (let [timeout-ms (.toMillis ^java.time.Duration timeout-duration)
              timeout-chan (csp/timeout timeout-ms)
              result
              ;; When a ktable is current these `fully-current-notification-ch` will fire often.
              ;; We will count until we've received two notification channels before returning from this fn.
              ;; This is done because `fully-current-notification-ch` will likely contain
              ;; values of having been current before this call, so we need at least one receive
              ;; to clear it.
              (loop [current-notifs-received 0
                     previous-offsets        nil]
                (let [[v ch] (csp/alts!! [timeout-chan
                                          fully-current-notification-ch])]
                  (if (= ch timeout-chan)
                    timeout-value

                    ;; if we've received a value, we want to receive the _same_ value
                    ;; from the `fully-current-notification-ch` at least twice
                    ;; to indicate stability, ie fully caught to a stable point.
                    (let [same-as-previous?            (= v previous-offsets)
                          next-current-notifs-received (if same-as-previous?
                                                         (inc current-notifs-received)
                                                         0)]
                      ;; if we've received the same offsets twice, we're good to return
                      (if (= 2 next-current-notifs-received)
                        v
                        ;; else we wait some more
                        (recur next-current-notifs-received v))))))]
          (when (= :throw result)
            (throw (ex-info "KTable timed out waiting to become fully current."
                            {:timeout timeout-duration})))
          result))

      IDeref
      (deref [_]
        ;; wait, but with feedback every 5 seconds
        (let [starting-millis (System/currentTimeMillis)]
          (loop []
            (let [caught-up? (deref has-caught-up-once
                                    5000
                                    :timeout)]
              (when (= :timeout caught-up?)
                (log/info (str "KTable '" consumer-group-id "' loading initial data. Waited "
                               (long (/ (- (System/currentTimeMillis) starting-millis)
                                        1000))
                               " seconds so far."))
                (recur)))))
        @ktable-state)

      IBlockingDeref
      (deref [_ timeout-ms timeout-val]
        (if-not (= timeout-val
                   (deref has-caught-up-once
                          timeout-ms
                          timeout-val))
          @ktable-state
          timeout-val))

      IRef
      (getValidator [_]  (.getValidator ^clojure.lang.Atom ktable-state))
      (getWatches [_]    (.getWatches ^clojure.lang.Atom ktable-state))
      (addWatch [_ k cb] (.addWatch ^clojure.lang.Atom ktable-state k cb))
      (removeWatch [_ k] (.removeWatch ^clojure.lang.Atom ktable-state k)))))

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

