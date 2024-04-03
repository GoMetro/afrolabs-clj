(ns afrolabs.components.kafka.edn-serdes
  (:gen-class
   :implements [org.apache.kafka.common.serialization.Serializer]
   :main false)
  (:require [clojure.tools.reader.edn :as edn]
            [taoensso.timbre :as log]
            [java-time :as t])
  (:import
   [org.apache.kafka.common.header Headers]
   [java.nio ByteBuffer]))

(gen-class :name "afrolabs.components.kafka.edn_serdes.Serializer"
           :prefix "ser-"
           :main false
           :implements [org.apache.kafka.common.serialization.Serializer])

(defn ser-serialize
  ([_ _ data]
   (when data
     (.getBytes (pr-str data))))
  ([this _ _ data]
   (ser-serialize this nil data)))

(defn ser-close [_])
(defn ser-configure [_ _ _])

;;;;;;;;;;;;;;;;;;;;

(def config-keys
  {:parse-inst-as-java-time "afrolabs.components.kafka.edn_serdes_parse_inst_as_java_time"})

(gen-class :name "afrolabs.components.kafka.edn_serdes.Deserializer"
           :prefix "deser-"
           :state state
           :init init
           :main false
           :implements [org.apache.kafka.common.serialization.Deserializer])

(defn deser-init []
  [[] (atom nil)])

(defn deser-deserialize-String-Headers-byte<>
  ([this topic ^bytes byte-data]
   (when byte-data
     (try (edn/read-string @(.state this)
                           (String. byte-data))
          (catch Throwable t
            (log/error t (str "Unable to deserialize EDN from topic '" topic "'.\n"
                              "The string value of the data is: \n" (String. ^bytes byte-data) "\n"
                              "The byte-array value is: " (mapv identity byte-data)))))))
  ([this _ _ byte-data]
   (deser-deserialize-String-Headers-byte<> this nil byte-data)))

(defn deser-deserialize-String-Headers-ByteBuffer
  ([this topic ^ByteBuffer byte-data]
   (when byte-data
     (let [byte-array (make-array Byte/TYPE (.remaining byte-data))]
       (.get byte-data byte-array)
       (try (edn/read-string @(.state this)
                             (String. ^bytes byte-array))
            (catch Throwable t
              (log/error t (str "Unable to deserialize EDN from topic '" topic "'.\n"
                                "The string value of the data is: \n" (String. ^bytes byte-array) "\n"
                                "The byte-array value is: " (mapv identity byte-array))))))))
  ([this _ _ byte-data]
   (deser-deserialize-String-Headers-ByteBuffer this nil byte-data)))

(defn deser-close [_])
(defn deser-configure [this config-settings _]
  (reset! (.-state this)
          {:readers (cond-> {}
                      (get config-settings (:parse-inst-as-java-time config-keys))
                      (assoc 'inst #(t/instant %)))}))

(comment

  (compile 'afrolabs.components.kafka.edn-serdes)


  (def ser (afrolabs.components.kafka.edn_serdes.Serializer.))

  (def bs (.serialize ser "topic" {:a 1}))

  (def des (afrolabs.components.kafka.edn_serdes.Deserializer.))

  (.configure des {} true)
  (.deserialize des "oeu"
                (byte-array bs))


  )

