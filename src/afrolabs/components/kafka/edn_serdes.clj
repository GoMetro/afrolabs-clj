(ns afrolabs.components.kafka.edn-serdes
  (:gen-class
   :implements [org.apache.kafka.common.serialization.Serializer]
   :main false)
  (:require [clojure.tools.reader.edn :as edn])
  (:import [org.apache.kafka.common.header Headers]))

(gen-class :name "afrolabs.components.kafka.edn_serdes.Serializer"
           :prefix "ser-"
           :main false
           :implements [org.apache.kafka.common.serialization.Serializer])

(defn ser-serialize
  ([_ _ data]
   (.getBytes (pr-str data)))
  ([this _ _ data]
   (ser-serialize this nil data)))

(defn ser-close [_])
(defn ser-configure [_ _ _])

;;;;;;;;;;;;;;;;;;;;

(gen-class :name "afrolabs.components.kafka.edn_serdes.Deserializer"
           :prefix "deser-"
           :main false
           :implements [org.apache.kafka.common.serialization.Deserializer])

(defn deser-deserialize
  ([_ _ byte-data]
   (edn/read-string (String. ^bytes byte-data)))
  ([this _ _ byte-data]
   (deser-deserialize this nil byte-data)))

(defn deser-close [_])
(defn deser-configure [_ _ _])

(comment

  (compile 'afrolabs.components.kafka.edn-serdes)


  (def ser (afrolabs.components.kafka.edn_serdes.Serializer.))

  (def bs (.serialize ser "topic" {:a 1}))

  (def des (afrolabs.components.kafka.edn_serdes.Deserializer.))

  (.deserialize des "oeu"
                (byte-array bs))


  )
