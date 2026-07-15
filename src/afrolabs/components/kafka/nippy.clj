(ns afrolabs.components.kafka.nippy
  "This namespace implements a kafka serdes (Serialization & Deserialization) strategy to be a drop-in replacement/upgrade
  over the default edn-serdes."
  (:require
   ;; [afrolabs.components.kafka :as -kafka :refer [defstrategy]]
   [clojure.edn :as edn]
   [java-time.api :as t]
   [taoensso.nippy :as nippy]
   [taoensso.timbre :as log])
  (:import
   [java.nio ByteBuffer]))

(def ^:dynamic *log-thaw-exceptions* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Serializer

(gen-class :name "afrolabs.components.kafka.nippy.Serializer"
           :prefix "ser-"
           :main false
           :implements [org.apache.kafka.common.serialization.Serializer])

(defn ser-serialize
  ([_ _ data]
   (when-not (nil? data)
     (nippy/freeze data)))
  ([this _ _ data]
   (ser-serialize this nil data)))

(defn ser-close [_])
(defn ser-configure [_ _ _])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def nippy-encoding-starter
  "This has been cribbed from the nippy source. It is the magic marker that identifies a stream as nippy-encoded. We will use it, sniff the packet, and know whether to even try nippy or go straight to Edn for deser."
  (.getBytes "NPY" java.nio.charset.StandardCharsets/UTF_8))

(defn bytes-starts-with-NPI?
  [^bytes bs]
  (and bs
       (< 3 (alength bs))
       (= (aget bs 0) (aget nippy-encoding-starter 0))
       (= (aget bs 1) (aget nippy-encoding-starter 1))
       (= (aget bs 2) (aget nippy-encoding-starter 2))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Deserializer

(gen-class :name "afrolabs.components.kafka.nippy.Deserializer"
           :prefix "deser-"
           :main false
           :implements [org.apache.kafka.common.serialization.Deserializer])

(defn- nippy-decode-exception?
  "Returns true when the exception signals that the bytes were not nippy-encoded,
  as opposed to some other kind of failure during deserialization."
  [e]
  (and (instance? clojure.lang.ExceptionInfo e)
       (= "taoensso.nippy" (:ns (ex-data e)))))

(defn- edn-fallback
  "Last-resort deserialization of `byte-data` as EDN.
  Parses #inst literals as java.time.Instant, matching the behaviour of EdnSerializer."
  [^bytes byte-data topic]
  (try
    (edn/read-string {:readers {'inst #(t/instant %)}} (String. byte-data))
    (catch Throwable ex
      (when *log-thaw-exceptions*
        (log/error ex (str "Unable to deserialize EDN fallback from topic '" topic "'"))))))

(defn- deser-bytes
  "Attempts nippy first. Falls back to EDN when the failure indicates the bytes
  were not nippy-encoded. Any other exception is logged and nil is returned."
  [^bytes byte-data topic]
  (try
    (if (bytes-starts-with-NPI? byte-data)
      (nippy/thaw byte-data)
      (edn/read-string {:readers {'inst #(t/instant %)}} (String. byte-data)))
    (catch Throwable ex
      (log/error ex (str "Unable to deserialize nippy bytes from topic '" topic "'"))))

  #_(try
    (nippy/thaw byte-data)
    (catch Throwable ex
      (edn/read-string {:readers {'inst #(t/instant %)}} (String. byte-data)))))

(defn deser-deserialize-String-Headers-byte<>
  ([_ topic ^bytes byte-data]
   (when-not (or (nil? byte-data)
                 (zero? (alength byte-data)))
     (deser-bytes byte-data topic)))
  ([this _ _ byte-data]
   (deser-deserialize-String-Headers-byte<> this nil byte-data)))

(defn deser-deserialize-String-Headers-ByteBuffer
  ([_ topic ^ByteBuffer byte-data]
   (when-not (or (nil? byte-data)
                 (zero? (.remaining byte-data)))
     (let [byte-array (make-array Byte/TYPE (.remaining byte-data))]
       (.get byte-data ^bytes byte-array)
       (deser-bytes byte-array topic))))
  ([this _ _ byte-data]
   (deser-deserialize-String-Headers-ByteBuffer this nil byte-data)))

(defn deser-close [_])
(defn deser-configure [_ _ _])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Strategy



(comment

  (compile 'afrolabs.components.kafka.nippy)

  )
