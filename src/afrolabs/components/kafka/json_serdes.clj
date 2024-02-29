(ns afrolabs.components.kafka.json-serdes
  (:gen-class
   :implements [org.apache.kafka.common.serialization.Serializer]
   :main false)
  (:require [clojure.data.json :as json]
            [taoensso.timbre :as log]
            [clojure.string :as str])
  (:import [org.apache.kafka.common.header Headers]))

;;;;;;;;;;;;;;;;;;;;

(def json-deserializer-keyfn-option-name
  (let [option-kw ::json-deserializer-keyfn]
    (-> (str (namespace option-kw) "_" (name option-kw))
        (str/replace "." "_")
        (str/replace "-" "_"))))

;;;;;;;;;;;;;;;;;;;;

(gen-class :name "afrolabs.components.kafka.json_serdes.Serializer"
           :prefix "ser-"
           :main false
           :implements [org.apache.kafka.common.serialization.Serializer])

(defn ser-serialize
  ([_ _ data]
   (.getBytes ^String (json/write-str data)))
  ([this _ _ data]
   (ser-serialize this nil data)))

(defn ser-close [_])
(defn ser-configure [_ _ _])

;;;;;;;;;;;;;;;;;;;;

(gen-class :name "afrolabs.components.kafka.json_serdes.Deserializer"
           :prefix "deser-"
           :state state
           :init init
           :main false
           :implements [org.apache.kafka.common.serialization.Deserializer])

(defn deser-init []
  [[] (atom nil)])

(defn deser-deserialize
  ([this topic byte-data]
   (when (and byte-data
              (pos? (count byte-data)))
     (try
       (json/read-str (String. ^bytes byte-data)
                      @(.state this))
       (catch Throwable t
         (log/error t (str "Unable to json deserialize from topic '" topic "'.\n"
                           "The string value of the data is:\n" (String. ^bytes byte-data) "\n"
                           "The byte-array value is: " (map identity byte-data)))))))
  ([this topic _headers byte-data]
   (deser-deserialize this topic byte-data)))

(defn deser-close [_])
(defn deser-configure
  [this config-settings _]
  (when-let [read-opts (cond-> {}
                         (get config-settings json-deserializer-keyfn-option-name)
                         (assoc :key-fn
                                (case (get config-settings json-deserializer-keyfn-option-name)
                                  "keyword" keyword
                                  "identity" identity
                                  (do (log/warn (str "Option " json-deserializer-keyfn-option-name
                                                     " for afrolabs.components.kafka.json_serdes.Deserializer "
                                                     "was given value '" (get config-settings json-deserializer-keyfn-option-name) "' "
                                                     "and this is not recognised. Using 'identity'."))
                                      identity))))]
    (log/debug (str "Setting json deserializer options to: " read-opts))
    (reset! (.-state this) read-opts)))

;;;;;;;;;;;;;;;;;;;;

(comment

  (compile 'afrolabs.components.kafka.json-serdes)


  (def ser (afrolabs.components.kafka.json_serdes.Serializer.))

  (def bs (.serialize ser "topic" {:a 1}))

  (def des (afrolabs.components.kafka.json_serdes.Deserializer.))

  (.deserialize des "oeu"
                (byte-array bs))




  )
