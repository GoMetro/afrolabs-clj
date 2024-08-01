(ns afrolabs.components.kafka.transit-serdes
  (:gen-class
   :implements [org.apache.kafka.common.serialization.Serializer]
   :main false)
  (:require [clojure.tools.reader.edn :as edn]
            [taoensso.timbre :as log]
            [java-time :as t]
            [afrolabs.transit :as -transit])
  (:import
   [java.io ByteArrayOutputStream ByteArrayInputStream]
   [org.apache.kafka.common.header Headers]
   [java.nio ByteBuffer]))

(comment

  (set! *warn-on-reflection* true)

  )

(gen-class :name "afrolabs.components.kafka.transit_serdes.Serializer"
           :prefix "ser-"
           :main false
           :implements [org.apache.kafka.common.serialization.Serializer])

(defn ser-serialize
  ([_ _ data]
   (when data
     (-transit/write-transit-bytes data)))
  ([this _ _ data]
   (ser-serialize this nil data)))

(defn ser-close [_])
(defn ser-configure [_ _ _])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(gen-class :name "afrolabs.components.kafka.transit_serdes.Deserializer"
           :prefix "deser-"
           :state state
           :init init
           :main false
           :implements [org.apache.kafka.common.serialization.Deserializer])

(defn deser-init []
  [[] (atom nil)])

(defn deser-deserialize-String-Headers-byte<>
  ([_this topic ^bytes byte-data]
   (when byte-data
     (try (-transit/read-transit (ByteArrayInputStream. byte-data))
          (catch Throwable t
            (log/error t (str "Unable to deserialize transit from topic '" topic "'.\n"
                              "The string value of the data is: \n" (String. ^bytes byte-data) "\n"
                              "The byte-array value is: " (mapv identity byte-data)))))))
  ([this _ _ byte-data]
   (deser-deserialize-String-Headers-byte<> this nil byte-data)))

(defn deser-deserialize-String-Headers-ByteBuffer
  ([_this topic ^ByteBuffer byte-data]
   (when byte-data
     (let [byte-array (make-array Byte/TYPE (.remaining byte-data))]
       (.get byte-data ^bytes byte-array)
       (try (-transit/read-transit (ByteArrayInputStream. byte-array))
            (catch Throwable t
              (log/error t (str "Unable to deserialize transit from topic '" topic "'.\n"
                                "The string value of the data is: \n" (String. ^bytes byte-array) "\n"
                                "The byte-array value is: " (mapv identity byte-array))))))))
  ([this _ _ byte-data]
   (deser-deserialize-String-Headers-ByteBuffer this nil byte-data)))

(defn deser-close [_])
(defn deser-configure [_this _config-settings _])

(comment

  (compile 'afrolabs.components.kafka.transit-serdes)


  (def ser (afrolabs.components.kafka.transit_serdes.Serializer.))

  (def bs (.serialize ser "topic" {:a 1}))

  (def des (afrolabs.components.kafka.transit_serdes.Deserializer.))

  (.deserialize ^org.apache.kafka.common.serialization.Deserializer des
                "oeu"
                (byte-array bs))


  )
