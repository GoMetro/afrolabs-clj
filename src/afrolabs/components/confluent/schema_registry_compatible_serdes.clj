(ns afrolabs.components.confluent.schema-registry-compatible-serdes
  "Provides a Confluent JSONSchema compatible serializer (for producers).

  Supports the wire-format: https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format"
  (:require [clojure.data.json :as json]
            [afrolabs.components.kafka :as -kafka]
            [taoensso.timbre :as log]
            [clojure.java.io :as io])
  (:import [org.apache.kafka.clients.producer ProducerConfig]
           [afrolabs.components.kafka IUpdateProducerConfigHook]
           [java.util UUID]
           [java.io ByteArrayOutputStream]
           [java.lang.ref Cleaner]))

(comment

  (compile 'afrolabs.components.confluent.schema-registry-compatible-serdes)

  )

;; Long essay:
;; A class that implements org.apache.kafka.common.serialization.Serializer is expected to have a zero-argument constructor.
;; This is because the producer thread will actually instantiate it. This also makse it hard to pass in context
;; and without extra trickery, this will make the class an effective singleton, which is problematic for creating components
;; that aim to be reusable, concurrently and in different contexts.
;; This class does support being configured (with textual values).
;; To aid in this, we will make use of an atom (schema-asserter-registry) which is a map from context-guids to
;; schema-asserters. We will then configured the producer with the correct context-guid (which is created invisibly and dynamically)

(defonce schema-asserter-registry (atom {}))

;; this also means we will leak memory if we create large amounts of ConfluentJSONSchemaCompatibleSerializer
;; we therefore have to remember to remove the context-guid from the schema-asserter-registry atom
;; once the objects go out of scope. Normally, you would do this with the finalize() method, but that has been deprecated.
;; we will use a cleaner instead.
;; this is the reference used: https://medium.com/javarevisited/time-to-say-goodbye-to-the-finalize-method-in-java-a2f5b7e4f1b1

(defonce cleaner (Cleaner/create))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(gen-class :name       "afrolabs.components.confluent.sr_compat_serdes.Serializer"
           :prefix     "ser-"
           :main       false
           :init       "init"
           :implements [org.apache.kafka.common.serialization.Serializer])

(defn ser-init [] [[] (atom {})])

(defn ser-configure
  [this cfg _]
  (log/debug (str "ser-configure; cfg= " cfg))
  (swap! (.state this)
         assoc :context-guid
         (get cfg (str "afrolabs.components.confluent.sr_compat_serdes.Serializer.context-guid"))))

(defn int->4byte-array
  "Better way to do this... it may even work."
  [i]
  (let [bb (java.nio.ByteBuffer/allocate 4)]
    (.putInt bb i)
    (.array bb)))

(defn ser-serialize
  ([this topic data]
   (let [context-guid        (:context-guid @(.state this))
         schema-asserter     (get @schema-asserter-registry context-guid)
         schema-id           (-kafka/get-schema-id schema-asserter topic)
         bytes-output-stream (ByteArrayOutputStream.)
         schema-id-bytes     (int->4byte-array schema-id)]

     ;; write the 5 bytes header
     (.write bytes-output-stream (byte 0))             ;; first byte is always 0
     (.write bytes-output-stream schema-id-bytes 0 4)  ;; 4 bytes worth of schema-id integer

     ;; write the json string
     (with-open [bytes-writer        (io/writer bytes-output-stream)]
       (json/write data bytes-writer))

     (.toByteArray bytes-output-stream)))
  ([this topic _headers data]
   (ser-serialize this topic data)))

(defn ser-close
  [this]
  (swap! (.state this)
         dissoc :context-guid))


;;;;;;;;;;;;;;;;;;;;

;; A value serializer, that works like JsonSerializer
;; except it supports the confluent wire format for specifying which schema-id is compatible with the json payload
;; Only supports producing.
(-kafka/defstrategy ConfluentJSONSchemaCompatibleSerializer
  [& {:keys           [schema-asserter
                       producer]
      :or             {producer :value}}]

  (let [allowed-values #{:key :value :both}]
    (when-not (allowed-values producer)
      (throw (ex-info (format "ConfluentJSONSchemaCompatibleSerializer expects one of '%s' for :producer."
                              (str allowed-values))
                      {::allowed-values allowed-values
                       ::producer       producer}))))

  (let [context-guid (str (UUID/randomUUID))]
    (swap! schema-asserter-registry assoc context-guid schema-asserter)
    (let [result
          (reify
            IUpdateProducerConfigHook
            (update-producer-cfg-hook
                [_ cfg]
              (cond->                       (assoc cfg                                          "afrolabs.components.confluent.sr_compat_serdes.Serializer.context-guid" context-guid)
                (#{:both :key}   producer)  (assoc ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG   "afrolabs.components.confluent.sr_compat_serdes.Serializer")
                (#{:both :value} producer)  (assoc ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "afrolabs.components.confluent.sr_compat_serdes.Serializer"))))]

      ;; ask the cleaner to remove the context-guid from the schema-asserter-registry once this object goes out of scope
      (.register ^Cleaner cleaner
                 result (fn []
                          (log/debug (str "In ConfluentJSONSchemaCompatibleSerializer; cleaning the reference for "
                                          context-guid))
                          (swap! schema-asserter-registry
                                 dissoc context-guid)))

      result)))

