(ns afrolabs.components.kafka.bytes-serdes
  (:require [taoensso.timbre :as log]
            [afrolabs.components.kafka :as -kafka])
  (:import [org.apache.kafka.common.header Headers]
           [afrolabs.components.kafka IUpdateConsumerConfigHook IUpdateProducerConfigHook]
           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.clients.producer ProducerConfig]
           [java.nio ByteBuffer]))

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
     (java.util.Arrays/copyOf ^bytes byte-data
                              (alength ^bytes byte-data))))
  ([this _ _ byte-data]
   (ser-serialize this nil byte-data)))

(defn ser-close [_])
(defn ser-configure [_ _ _])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(gen-class :name "afrolabs.components.kafka.bytes_serdes.Deserializer"
           :prefix "deser-"
           :main false
           :implements [org.apache.kafka.common.serialization.Deserializer])

(defn deser-deserialize-String-Headers-byte<>
  ([_ _ ^bytes byte-data]
   (when byte-data
     (java.util.Arrays/copyOf ^bytes byte-data
                              (alength ^bytes byte-data))))
  ([this _ _ ^bytes byte-data]
   (deser-deserialize-String-Headers-byte<> this nil byte-data)))

(defn deser-deserialize-String-Headers-ByteBuffer
  ([_ _ ^ByteBuffer byte-data]
   (when byte-data
     (let [result (make-array Byte/TYPE (.remaining byte-data))]
       (.get byte-data ^bytes result)
       result)))
  ([this _ _ byte-data]
   (deser-deserialize-String-Headers-ByteBuffer this nil byte-data)))

(defn deser-close [_])
(defn deser-configure [_ _ _])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; (defonce initialised (atom nil))
