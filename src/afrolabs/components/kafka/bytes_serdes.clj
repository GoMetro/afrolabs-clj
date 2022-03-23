(ns afrolabs.components.kafka.bytes-serdes
  (:require [taoensso.timbre :as log]
            [afrolabs.components.kafka :as -kafka])
  (:import [org.apache.kafka.common.header Headers]
           [afrolabs.components.kafka IUpdateConsumerConfigHook IUpdateProducerConfigHook]
           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.clients.producer ProducerConfig]))

(comment

  (compile 'afrolabs.components.kafka.bytes-serdes)

  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(gen-class :name "afrolabs.components.kafka.bytes_serdes.Serializer"
           :prefix "ser-"
           :main false
           :implements [org.apache.kafka.common.serialization.Serializer])

(defn ser-serialize
  ([_ _ byte-data]
   (when byte-data
     (java.util.Arrays/copyOf byte-data (alength byte-data))))
  ([this _ _ byte-data]
   (ser-serialize this nil byte-data)))

(defn ser-close [_])
(defn ser-configure [_ _ _])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(gen-class :name "afrolabs.components.kafka.bytes_serdes.Deserializer"
           :prefix "deser-"
           :main false
           :implements [org.apache.kafka.common.serialization.Deserializer])

(defn deser-deserialize
  ([_ _ byte-data]
   (when byte-data
     (java.util.Arrays/copyOf byte-data (alength byte-data))))
  ([this _ _ byte-data]
   (deser-deserialize this nil byte-data)))

(defn deser-close [_])
(defn deser-configure [_ _ _])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(-kafka/defstrategy ByteArraySerializer
  [& {consumer-option :consumer
      producer-option :producer
      :or {consumer-option :none
           producer-option :none}}]

  ;; validation of arguments, fail early
  (let [allowed-values #{:key :value :both :none}]
    (when-not (and (allowed-values consumer-option)
                   (allowed-values producer-option))
      (throw (ex-info "Specify proper values for :consumer and/or :producer for ByteArraySerializer."
                      {::allowed-values  allowed-values
                       ::consumer-option consumer-option
                       ::producer-option producer-option}))))

  (reify
    IUpdateConsumerConfigHook
    (update-consumer-cfg-hook
        [_ cfg]
      (cond-> cfg
        (#{:both :key}   consumer-option)  (assoc ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG    "afrolabs.components.kafka.bytes_serdes.Deserializer")
        (#{:both :value} consumer-option)  (assoc ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG  "afrolabs.components.kafka.bytes_serdes.Deserializer")))

    IUpdateProducerConfigHook
    (update-producer-cfg-hook
        [_ cfg]
      (cond-> cfg
        (#{:both :key}   producer-option) (assoc ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG       "afrolabs.components.kafka.bytes_serdes.Serializer")
        (#{:both :value} producer-option) (assoc ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG     "afrolabs.components.kafka.bytes_serdes.Serializer")))))
