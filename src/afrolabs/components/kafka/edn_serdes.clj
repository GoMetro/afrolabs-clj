(ns afrolabs.components.kafka.edn-serdes
  (:gen-class
   :implements [org.apache.kafka.common.serialization.Serializer]
   :main false)
  (:require [clojure.tools.reader.edn :as edn]
            [taoensso.timbre :as log]
            [java-time :as t])
  (:import [org.apache.kafka.common.header Headers]))

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

(defn deser-deserialize
  ([this _ byte-data]
   (when byte-data
     (edn/read-string @(.state this)
                      (String. ^bytes byte-data))))
  ([this _ _ byte-data]
   (deser-deserialize this nil byte-data)))

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

