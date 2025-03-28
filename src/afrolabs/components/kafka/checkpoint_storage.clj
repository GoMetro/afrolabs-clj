(ns afrolabs.components.kafka.checkpoint-storage
  "This namespace contains some code that helps to persist (take snapshots) of ktables
  and also to read them back.

  The idea is that a ktable will accept an optional configuration for `:checkpoint-storage` and if set
  use the protocol `IKTableCheckpointStorage` to regularly save ktable values and load the most recent one
  on ktable startup.

  All ktables have meta-data on the ktable value itself as well as the record values.
  For this reason we need to persist meta-data during serialization.

  Some ktables have records (`defrecord`) in the values. These records need to be serialized and deserialized
  in a generic way, hence the use of the `miner.tagged` library, which \"only\" requires that the record
  gets a special implementation of `clojure.core/print-method`, which typically will use the
  `pr-tagged-record-on` fn in this ns."
  (:require
   [afrolabs.components :as -comp]
   [afrolabs.components.health :as -health]
   [afrolabs.components.time :as -time]
   [afrolabs.components.kafka.checkpoint-storage.stores :as -cp-stores]
   [clojure.core.async :as csp]
   [clojure.java.io :as io]
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [java-time.api :as t]
   [miner.tagged :as tag]
   [taoensso.timbre :as log]
   )
  (:import
   [java.io File]
   [java.time Duration]))

(def duration-units #{:seconds :minutes :hours})

(defprotocol IKTableCheckpointStorage
  "A protocol for managing the storage of snapshots/checkpoints of ktable values.

  - Stores (some) snapshots values
  - Retrieves (the most up-to-date) snapshot

  The decision on whether to actually save/store a checkpoint is internal to the implementation of this protocol.
  The ktable will call `register-ktable-value` after every change to the ktable, and saving every new value
  will defeat the purpose of the solution.

  Therefore, the implementation of this protocol will silently drop most registered checkpoint values and only keep some.
  "
  (register-ktable-value [_ ktable-id ktable-value] "Stores a ktable value as a checkpoint.")
  (retrieve-latest-checkpoint [_ ktable-id] "Retrieves the most up-to-date ktable checkpoint value"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn normalize-duration
  "Accepts something that represents or is a duration, and turns it into a duration object."
  [duration-like]
  (cond
    (instance? java.time.Duration duration-like)
    duration-like

    (and (sequential? duration-like)
         (pos-int? (first duration-like))
         (duration-units (second duration-like)))
    (apply t/duration duration-like)))

(comment

  (normalize-duration (t/duration 1 :minutes))
  ;; #object[java.time.Duration 0x7cba1687 "PT1M"]
  (normalize-duration [1 :minutes])
  ;; #object[java.time.Duration 0x6d1dd9ac "PT1M"]

  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^{:dynamic true
       :doc     (str "System-level binding for whether checkpoints are serialized into bytestreams with GZip encoding (or not).\n"
                     "Be careful with this. This binding cannot be changed during the system lifetime. The component for checkpoints\n"
                     "initializes the worker thread with this binding's value at the time the component is started. To change \n"
                     "the GZip behaviour, the component/system must be restarted with the new binding value.")}
  *serialize-with-gzip?* true)

(defn serialize
  "Performs ->edn serialization on passed values.

  The single-arity accumulates the serialized value in memory and returns the serialized data as a string value.
  The single-arity version does not apply GZip encoding.

  The intended use, the two-arity, returns nil and writes the serialized data in gzip encoding to `output-stream`.
  This fn does NOT close the `output-stream`.

  See `*serialize-with-gzip?*`."
  ([x]
   (let [bytes-stream (java.io.ByteArrayOutputStream.)
         _ (serialize x bytes-stream)]
     (.toString bytes-stream "UTF-8")))
  ([x output-stream]
   (when-not (instance? java.io.OutputStream output-stream)
     (throw (ex-info "Can't serialize to this destination; Only supports instances of java.io.OutputStream."
                     {:output-stream-type (type output-stream)})))
   (let [output-stream'    (if-not *serialize-with-gzip?*
                             output-stream
                             (java.util.zip.GZIPOutputStream. output-stream))
         writer (io/writer output-stream')]
     (binding [*print-meta*     true
               *print-dup*      nil
               *print-length*   nil
               *print-level*    nil
               *print-readably* true
               *out*            writer]
       (pr x)
       (flush)
       (when *serialize-with-gzip?*
         (.finish ^java.util.zip.GZIPOutputStream output-stream'))
       nil))))

(defn deserialize
  "Perform edn->value de-serialization and returns the resultant value.

  `x-str` can be one of the following types:
  - `string` the literal string value is deserialized (NOT treated like a filename nor URL). (No GZip decoding.)
  - `InputStream` stream is read as a character stream and deserialized. The binding value of `*serialize-with-gzip?*` affects this fn.
  2-arity overload accepts `reader-options` as the second argument.
  This options may have the same value as options in `(clojure.edn/read-string opts s)`.

  Does not close the input stream."
  ([x-str] (deserialize x-str {}))
  ([str-or-input-stream reader-options]
   (cond
     (string? str-or-input-stream)
     (tag/read-string reader-options
                      str-or-input-stream)

     (instance? java.io.InputStream str-or-input-stream)
     (let [input-stream (if-not *serialize-with-gzip?*
                           str-or-input-stream
                           (java.util.zip.GZIPInputStream. str-or-input-stream))]
       (tag/read reader-options
                 (java.io.PushbackReader. (io/reader input-stream))))

     :else
     (throw (ex-info "Deserialize don't know what to do with this type."
                     {:type (type str-or-input-stream)})))))

(comment

  ;; playing with gzip when serializing and deserializing (roundtripping)
  (binding [*serialize-with-gzip?* false] ;; binding added after gzip implemented as part of serdes
    (let [x {:a ^:testing [1 2 :three]}]
      (with-open [bytes-stream (java.io.ByteArrayOutputStream.)
                  gzip-stream  (java.util.zip.GZIPOutputStream. bytes-stream)]
        (serialize x gzip-stream)
        (.finish gzip-stream) ;; important part of GZIPOutputStream API
        (with-open [bytes-input (io/input-stream (.toByteArray bytes-stream))
                    gzip-input-stream (java.util.zip.GZIPInputStream. bytes-input)]
          (deserialize gzip-input-stream)))))
  ;; {:a [1 2 :three]}


  ;; previous, but with built-in gzipping
  (let [x {:a ^:testing [1 2 :three]}]
    (with-open [bytes-stream (java.io.ByteArrayOutputStream.)]
      (serialize x bytes-stream)
      (with-open [bytes-input (io/input-stream (.toByteArray bytes-stream))]
        (deserialize bytes-input))))
  ;; {:a [1 2 :three]}


  )

;; NOTE: This copied from tagged library and modified to add support for meta-data
;; https://github.com/miner/tagged/pull/4
(defn pr-tagged-record-on
  "Prints the EDN tagged literal representation of the record `this` on the java.io.Writer `w`.
  Useful for implementing a print-method on a record class.  For example:

     (defmethod print-method my.ns.MyRecord [this w]
       (miner.tagged/pr-tagged-record-on this w))

  Adopted from tagged in order to print meta-data."
  [this ^java.io.Writer w]
  (when (and *print-meta*
             (instance? clojure.lang.IMeta this)
             (seq (meta this)))
    (.write w "^")
    (print-method (meta this) w)
    (.write w " "))

  ;; need do to var-ref `tag/tag-string` because it is private
  (.write w "#")
  (.write w ^String (#'tag/tag-string (class this)))
  (.write w " ")
  (print-method (into {} this) w))

(comment

  ;; is meta-data carried along?
  (-> {:a [1 2 ^:testing {}]} 
      (serialize) (deserialize)
      :a (nth 2) meta)
  ;; {:testing true}

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;;    what do be done with records?   ;;
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; using miner.tagged helps here, provided we implement a multimethod on the type

  (defrecord testr [^long a ^String b ^boolean c])
  (defmethod print-method afrolabs.components.kafka.checkpoint_storage.testr [this w] (pr-tagged-record-on this w))

  (def tr1 (->testr 1 "oeu" false))

  ;; in absence of print-method multimethod
  (pr-str tr1) ;; "#afrolabs.components.kafka.checkpoint_storage.testr{:a 1, :b \"oeu\", :c false}"
  ;; if print-method has been eval'd
  (pr-str tr1) ;; "#afrolabs.components.kafka.checkpoint-storage/testr {:a 1, :b \"oeu\", :c false}"

  ;; can now be read
  (= (tag/read-string (pr-str tr1))
     tr1) ;; true

  ;; this allows ktables that contain record values to serialize and deserialize their values
  ;; in a generic way, by only implementing the multi-method for the record type

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  (-> {:a [1 2
           (with-meta (->testr 12 "hello world" false)
             {:testing false})]}
      (serialize)
      (deserialize)
      :a (nth 2) meta
      )
  ;; {:testing false}
  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- make-checkpoint-fn
  "Accepts a component config, a callback-fn (`save-snapshot-callback`)
  and returns `register-checkpoint-candidate`. 

  - `save-snapshot-callback` : Accepts ktable-id and ktable-value in a tuple. Must persist this value in the blob store.
  - `register-checkpoint-candidate` : (result). Use this function on every new ktable value. Some of the passed values
    will be passed to `save-snapshot-callback`, based on timing.
  - Call the `register-checkpoint-candidate` fn with `nil` to shut down the background process.

  The purpose of this fn is to ensure that ktable checkpoints are called regularly, but not too often.
  This is achieved by saving once per time window duration."
  [{:keys [timewindow-duration]}
   save-snapshot-callback
   & {:keys [logging-context]}]

  (let [new-values-ch       (csp/chan (csp/sliding-buffer 1))
        make-new-timeout    #(csp/timeout (.toMillis ^Duration timewindow-duration))

        background-process
        (csp/thread (log/with-context+ (or logging-context {})
                      (loop [current-state {}
                             timeout-ch    (make-new-timeout)]
                        (let [[v ch] (csp/alts!! [new-values-ch timeout-ch])

                              [new-value new-timeout-ch :as recur-params]
                              (cond
                                ;; if we receive nil, the input ch was closed
                                ;; it should be safer to just silently ignore the last values, due to the previously saved snapshot
                                (and (= ch new-values-ch)
                                     (nil? v))
                                nil
                                
                                ;; we've received a new value, so we want to keep it for when we decide to make a snapshot
                                ;; We keep the current `timeout-ch`
                                (= ch new-values-ch)
                                [(assoc current-state (first v) (second v)) timeout-ch]

                                ;; we've timed out, so it's time to actually save the current value (if it is a value)
                                (= ch timeout-ch)
                                (let [next-timeout (make-new-timeout)]
                                  (when (seq current-state)
                                    (log/debug "Saving new snapshots...")
                                    ;; we have something to save and it is time to save it
                                    (doseq [item current-state]
                                      (log/debug (str "Saving snapshot for " (first item)))
                                      (save-snapshot-callback item)))
                                  ;; since we've saved the last values, our next current value is `{}` again
                                  [{} next-timeout]))]
                          (when (seq recur-params)
                            (recur new-value new-timeout-ch)))))
                    (log/info "Finished with checkpoint-keeping background thread."))]

    (fn register-checkpoint-candidate [x]
      (if x
        (csp/>!! new-values-ch x)
        (do (csp/close! new-values-ch)
            (csp/<!! background-process))))))

(defn store-ktable-checkpoint!
  "Actually persists the passed `checkpoint-value` to a \"path\" in the storage medium based on `ktable-id`."
  [{:as _cfg :keys [clock
                    checkpoint-store
                    service-health-trip-switch]}
   [ktable-id
    checkpoint-value]]
  (log/with-context+ {:ktable-id    ktable-id}
    (try
      (let [checkpoint-instant (-time/get-current-time clock)
            checkpoint-name    (format (if *serialize-with-gzip?*
                                         "%d-%s.edn.gz"
                                         "%d-%s.edn")
                                       (t/to-millis-from-epoch checkpoint-instant)
                                       (t/format :iso-offset-date-time
                                                 (t/zoned-date-time checkpoint-instant
                                                                    (t/zone-id "UTC"))))]
        (with-open [file-stream (-cp-stores/open-storage-stream checkpoint-store
                                                                ktable-id
                                                                checkpoint-name)]
          (serialize checkpoint-value
                     file-stream)))
      (catch Throwable t
        (log/error t "Unable to store ktable checkpoints! Tripping health trip switch")
        (-health/indicate-unhealthy! service-health-trip-switch
                                     ::filesystem-ktable-checkpoint)))))

(defn retrieve-latest-ktable-checkpoint
  "Lists all checkpoint values in files, scans for a name that is most recent, based on the millis-since-epoch before the first \\-
  and returns that value.

  May return `nil` if no (working) checkpoints could be found or loaded."
  [{:as _cfg :keys [checkpoint-store
                    parse-inst-as-java-time]}
   ktable-id]
  (let [last-checkpoint-id (->> (-cp-stores/list-checkpoint-ids checkpoint-store
                                                                ktable-id)
                                (map (juxt (comp (partial re-matches -cp-stores/checkpoint-id-re))
                                           identity))
                                ;; [0 1] reaches the first re match group, which is millis-since-epoch
                                (map #(update-in % [0 1] Long/parseLong))
                                ;; the reduce is changing the shape of the result
                                ;; [millis-since-epoch-of-snapshot snapshot-id]
                                ;; get the chekpoint-id with the max millis-timestamp
                                (reduce (fn [[current-millis _current-file :as current-pick] [[_ file-millis] cp-id]]
                                          (if (> file-millis current-millis)
                                            [file-millis cp-id]
                                            current-pick))
                                        [0 nil])
                                ;; keep the timestamp-id
                                second)]
    (when last-checkpoint-id
      (with-open [fs (-cp-stores/open-retrieval-stream checkpoint-store
                                                       ktable-id
                                                       last-checkpoint-id)]
        ;; tries to accommodate that we may save a snapshot with gzip-or-not
        ;; and would still want to be able to load it, so we detect it from the checkpoint-id (filename)
        ;; we do it like this, because deserialize deals with the /stream/ not the name of the checkpoint
        ;; and has no other context (besides dynamic binding) to know to use gzip or not.
        (let [gzip? (str/ends-with? last-checkpoint-id
                                    ".gz")]
          (binding [*serialize-with-gzip?* gzip?]
            (deserialize fs
                         (cond-> {}
                           parse-inst-as-java-time (merge {:readers {'inst t/instant}})))))))))

(defn make-checkpoint-storage-component
  "Returns an implementation of `IKTableCheckpointStorage` that stores checkpoints on the local file system."
  [{:as   cfg}]
  (let [checkpoint (make-checkpoint-fn cfg #(store-ktable-checkpoint! cfg %))]
    (reify
      IKTableCheckpointStorage
      (register-ktable-value [_ ktable-id ktable-value]
        (checkpoint [ktable-id ktable-value]))
      (retrieve-latest-checkpoint [_ ktable-id]
        (retrieve-latest-ktable-checkpoint cfg ktable-id))

      -comp/IHaltable
      (halt [_] (checkpoint nil)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::timewindow-duration
  (s/or :duration-spec (s/tuple pos-int? #{:seconds :minutes :hours})
        :duration-instance #(instance? java.time.Duration %)))
(s/def ::parse-inst-as-java-time (s/nilable boolean))
(s/def ::filesystem-ktable-checkpoint-cfg
  (s/keys :req-un [::-time/clock
                   ::timewindow-duration
                   ::-health/service-health-trip-switch
                   ::-cp-stores/checkpoint-store]
          :opt-un [::parse-inst-as-java-time]))

(-comp/defcomponent {::-comp/ig-kw                  ::filesystem-ktable-checkpoint
                     ::-comp/config-spec            ::filesystem-ktable-checkpoint-cfg}
  [cfg] (make-checkpoint-storage-component (update cfg :timewindow-duration normalize-duration)))
