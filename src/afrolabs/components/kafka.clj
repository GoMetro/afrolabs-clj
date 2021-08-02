(ns afrolabs.components.kafka
  (:require [afrolabs.components.internal-include :as -inc]
            [afrolabs.components :as -comp]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as sgen]
            [integrant.core :as ig]
            [clojure.core.async :as csp]
            [taoensso.timbre :as timbre
             :refer [log  trace  debug  info  warn  error  fatal  report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]])
  (:import [org.apache.kafka.clients.producer ProducerConfig ProducerRecord KafkaProducer MockProducer Producer]
           [org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer MockConsumer OffsetResetStrategy ConsumerRecord Consumer]
           [java.util.concurrent Future]
           [java.util Map Collection UUID]))


(comment

  (set! *warn-on-reflection* true)

  )
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol IProducer
  "Can produce records. To kafka, probably."

  (get-producer [_] "Returns the actual kafka API producer object")
  (produce! [_ records]
    "Produces a collection of record maps. This is a side-effect!"))

(defprotocol IConsumer
  "The client interface for the kafka consumer component"
  (get-consumer [_] "Returns the actual kafka API consumer object (or mock)"))

(defprotocol IConsumerClient
  "Given to a consumer (thread), invoked with received messages, returns messages to be produced."
  (consume-messages [_ msgs] "Consumes a collection of messages from subscribed topics. Returns a collection of messages to be produced."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- producer-produce
  [^Producer producer msgs]
  (doseq [m (->> msgs
                 (map #(ProducerRecord. (:topic %)
                                        (:key %)
                                        (:value %)))
                 (map #(.send producer %))
                 (doall))]
    (.get ^Future m)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Strategies

(defprotocol IUpdateProducerConfigHook
  "A Strategy protocol for modifying the configuration properties before the Producer object is created. Useful for things like serdes."
  (update-producer-cfg-hook [this cfg] "Takes a config map and returns a (modified version of the) map."))

(defprotocol IUpdateConsumerConfigHook
  "A Strategy protocol for modifying the configuration properties before the Consumer objects is created. Useful for things like serdes."
  (update-consumer-cfg-hook [this cfg] "Takes a config map and returns a (modified version of the) map."))

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
  "A strategy protocol to interept messages between the consumer object thread and the consumer-client.

  All the middlewares will form a chain, succesively intercepting and modifying messages one after the other, with later interceptors having access to the changes made be earlier interceptors.

  It is assumed that the result of any consumer-client is again messages that must be produced, so in reality this middleware accepts kafka messages (in map format) both from and to the consumer thread."
  (consumer-middleware-hook [this msgs] "Accepts a collection of messages and returns a collection of messages."))


(def satisfies-some-of-the-strategy-protocols (->> [IShutdownHook
                                                    IPostConsumeHook
                                                    IConsumerInitHook
                                                    IUpdateConsumerConfigHook
                                                    IUpdateProducerConfigHook
                                                    IConsumerMiddleware]
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
  [& {:keys [schema-registry-url]
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

  (reify
    IUpdateProducerConfigHook
    (update-producer-cfg-hook
        [_ cfg]
      (cond-> (assoc cfg "schema.registry.url" schema-registry-url)
        (#{:both :key}   producer-option)  (assoc ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG   "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer")
        (#{:both :value} producer-option)  (assoc ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer")))

    IUpdateConsumerConfigHook
    (update-consumer-cfg-hook
        [_ cfg]
      (cond-> (assoc cfg "schema.registry.url" schema-registry-url)
        (#{:both :key}   consumer-option)  (assoc ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG  "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer")
        (#{:both :value} consumer-option)  (assoc ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG    "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer")))))

(defstrategy StringSerializer
  [& {producer-option :producer
      consumer-option :consumer
      :or {producer-option :both
           consumer-option :both}}]

  ;; validation of arguments, fail early
  (let [allowed-values #{:key :value :both}]
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
        (#{:both :key}   consumer-option)  (assoc ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG  "org.apache.kafka.common.serialization.StringDeserializer")
        (#{:both :value} consumer-option)  (assoc ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG    "org.apache.kafka.common.serialization.StringDeserializer")))))

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
                                   :offset (dec (.position consumer tp))}))
                    topic-partition-assignment)]
          (when (= (into #{}
                         (map (fn [[tp o]] {:topic (.topic tp)
                                            :partition (.partition tp)
                                            :offset o}))
                         (.endOffsets consumer
                                      topic-partition-assignment))
                   current-offsets)
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

(defstrategy ConfluentCloud
  ;; "Sets the required and recommended config to connect to a kafka cluster in confluent cloud.
  ;; Based on: https://docs.confluent.io/cloud/current/client-apps/config-client.html#java-client"
  [& {:keys [api-key api-secret]}]

  ;; some validation
  (when-not (and api-key api-secret)
    (throw (ex-info "You must specify the api-key and api-secret for configuration with ConfluentCloud strategy." {})))

  (letfn [(merge-common [cfg]
            (merge cfg
                   {"sasl.mechanism"           "PLAIN"
                    "sasl.jaas.config"         (format (str "org.apache.kafka.common.security.plain.PlainLoginModule required "
                                                            "username=\"%s\" "
                                                            "password=\"%s\" "
                                                            ";")
                                                       api-key api-secret)
                    "security.protocol"        "SASL_SSL"
                    "client.dns.lookup"        "use_all_dns_ips"
                    "reconnect.backoff.max.ms" "10000"
                    "request.timeout.ms"       "30000"}))]

    (reify
      IUpdateConsumerConfigHook
      (update-consumer-cfg-hook
          [_ cfg]
        (-> cfg
            (merge-common)
            (merge {"session.timeout.ms" "45000"})))

      IUpdateProducerConfigHook
      (update-producer-cfg-hook
          [_ cfg]
        (-> cfg
            (merge-common)
            (merge {"acks"      "all"
                    "linger.ms" "5"}))))))

;;;;;;;;;;;;;;;;;;;;

(defn normalize-strategies
  "Takes a collection of strategy-protocol-implementing-objects or strategy-keyword-declaration-vectors and turns the strategy-keyword-declaration-vectors into strategy-protocol-implementing-objects."
  [ss]
  (vec (concat (filter satisfies-some-of-the-strategy-protocols ss)
               (->> ss
                    (filter (complement satisfies-some-of-the-strategy-protocols))
                    (map (partial create-strategy))))))

(comment

  (normalize-strategies [(StringSerializer)
                         (ConfluentJSONSchema :schema-registry-url "")
                         [:strategy/FreshConsumerGroup]
                         [:strategy/SubscribeWithTopicsCollection ["test"]]])
  ;; [#object[afrolabs.components.kafka$StringSerializer$reify__27765 0x3e1f8c0a "afrolabs.components.kafka$StringSerializer$reify__27765@3e1f8c0a"]
  ;;  #object[afrolabs.components.kafka$ConfluentJSONSchema$reify__27753 0x24337527 "afrolabs.components.kafka$ConfluentJSONSchema$reify__27753@24337527"]
  ;;  #object[afrolabs.components.kafka$FreshConsumerGroup$reify__27820 0x2257b42a "afrolabs.components.kafka$FreshConsumerGroup$reify__27820@2257b42a"]]

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

(defn make-producer
  [{:keys [bootstrap-server
           strategies]
    :as producer-config}]
  (s/assert ::producer-config producer-config)
  (let [strategies (normalize-strategies strategies)]
    (let [starting-cfg {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-server}
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

        -comp/IHaltable
        (halt [_] (.close ^Producer producer))))))

;;;;;;;;;;;;;;;;;;;;

(-comp/defcomponent {::-comp/ig-kw       ::kafka-producer
                     ::-comp/config-spec ::producer-config}
  [cfg] (make-producer cfg))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; CONSUMER

(s/def :consumer/client #(satisfies? IConsumerClient %))
(s/def :consumer.poll/timeout pos-int?)
(s/def :consumer/mock #(= MockConsumer (class %)))

(s/def ::consumer-config (s/and (s/keys :req-un [::bootstrap-server]
                                        :req    [:consumer/client]
                                        :opt-un [::strategies]
                                        :opt    [:consumer.poll/timeout
                                                 :consumer/mock])))

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

  (let [post-consume-hooks (into []
                                 (filter (partial satisfies? IPostConsumeHook))
                                 strategies)
        combined-post-consume-hook
        (fn [& args]
          (doseq [s post-consume-hooks]
            (apply post-consume-hook s args)))]
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

          (when (seq consumption-results)
            (warn "This consumer is not configured for the consumer->produce cycle."))

          (combined-post-consume-hook consumer consumed-records)))

      ;; we're done .poll'ing and shutting down
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
  [consumer must-stop consumer-config consumer-properties has-stopped]
  (csp/go
    (let [[status xtra :as thread-result]
          (csp/<! (csp/thread (-consumer-main consumer
                                              must-stop
                                              :consumer-config consumer-config
                                              :consumer-properties consumer-properties)))]
      (when (= :error status)
        (error xtra ;; hopefully, an exception packaged with try/catch in -consumer-main
               "Kafka consumer main thread finished with exception."))

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

      -comp/IHaltable
      (halt [_]
        (reset! must-stop true)
        @has-stopped
        (.close ^Consumer consumer)))))

(-comp/defcomponent {::-comp/ig-kw       ::kafka-consumer
                     ::-comp/config-spec ::consumer-config}
  [cfg] (make-consumer cfg))
