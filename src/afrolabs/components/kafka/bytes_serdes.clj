(ns afrolabs.components.kafka.bytes-serdes
  (:require [taoensso.timbre :as log]
            [afrolabs.components.kafka :as -kafka])
  (:import [org.apache.kafka.common.header Headers]
           [afrolabs.components.kafka IUpdateConsumerConfigHook]
           [org.apache.kafka.clients.consumer ConsumerConfig]))

(comment

  (compile 'afrolabs.components.kafka.bytes-serdes)

  )


(gen-class :name "afrolabs.components.kafka.bytes_serdes.Deserializer"
           :prefix "deser-"
           :main false
           :implements [org.apache.kafka.common.serialization.Deserializer])

(defn deser-deserialize
  ([_ _ byte-data]
   (java.util.Arrays/copyOf byte-data (alength byte-data)))
  ([this _ _ byte-data]
   (deser-deserialize this nil byte-data)))

(defn deser-close [_])
(defn deser-configure [_ _ _])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(-kafka/defstrategy ByteArrayDeserializer
  [& {consumer-option :consumer
      :or {consumer-option :none}}]

  ;; validation of arguments, fail early
  (let [allowed-values #{:key :value :both :none}]
    (when-not (allowed-values consumer-option)
      (throw (ex-info "ByteArrayDeserializer can only be used for consumers."
                      {::allowed-values  allowed-values
                       ::consumer-option consumer-option}))))

  (reify
    IUpdateConsumerConfigHook
    (update-consumer-cfg-hook
        [_ cfg]
      (cond-> cfg
        (#{:both :key}   consumer-option)  (assoc ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG    "afrolabs.components.kafka.bytes_serdes.Deserializer")
        (#{:both :value} consumer-option)  (assoc ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG  "afrolabs.components.kafka.bytes_serdes.Deserializer")))))
