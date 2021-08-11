(ns afrolabs.components.kafka.json-serdes
  (:gen-class
   :implements [org.apache.kafka.common.serialization.Serializer]
   :main false)
  (:require [clojure.data.json :as json])
  (:import [org.apache.kafka.common.header Headers]))

;;;;;;;;;;;;;;;;;;;;

(gen-class :name "afrolabs.components.kafka.json_serdes.Serializer"
           :prefix "ser-"
           :main false
           :implements [org.apache.kafka.common.serialization.Serializer])

(defn ser-serialize
  ([_ _ data]
   (.getBytes (json/write-str data)))
  ([this _ _ data]
   (ser-serialize this nil data)))

(defn ser-close [_])
(defn ser-configure [_ _ _])

;;;;;;;;;;;;;;;;;;;;

(gen-class :name "afrolabs.components.kafka.json_serdes.Deserializer"
           :prefix "deser-"
           :main false
           :implements [org.apache.kafka.common.serialization.Deserializer])

(defn deser-deserialize
  ([_ _ byte-data]
   (json/read-str (String. byte-data)))
  ([this _ _ byte-data]
   (deser-deserialize this nil byte-data)))

(defn deser-close [_])
(defn deser-configure [_ _ _])

;;;;;;;;;;;;;;;;;;;;

(comment

  (compile 'afrolabs.components.kafka.json-serdes)


  (def ser (afrolabs.components.kafka.json_serdes.Serializer.))

  (def bs (.serialize ser "topic" {:a 1}))

  (def des (afrolabs.components.kafka.json_serdes.Deserializer.))

  (.deserialize des "oeu"
                (byte-array bs))




  )
