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
   [clojure.core.async :as csp]
   [clojure.spec.alpha :as s]
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

(def ^{:arglists '([storage-path])
       :doc      "Checks that the path exists and is a directory, or creates it, or throws. Caches results per storage-path."
       :private  true}
  ensure-fs-directory-path!
  (memoize (fn [storage-path]
             (let [storage-path-f (File. storage-path)]
               (try
                 (if (.exists storage-path-f)
                   (when-not (.isDirectory storage-path-f)
                     (throw (ex-info "The storage path for filesystem-checkpoint-storage must point to a directory."
                                     {:path storage-path})))
                   (when-not (.mkdirs storage-path-f)
                     (throw (ex-info "Unable to create the file system ktable storage path."
                                     {:path storage-path}))))
                 (catch Throwable t
                   (let [error-msg "Unable to initialize the filesystem checkpoint storage path."]
                     (log/error t error-msg)
                     (throw (ex-info error-msg
                                     {:path storage-path}
                                     t)))))
               storage-path-f))))

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

(defn serialize
  "Performs ->edn serialization on passed values."
  [x]
  (binding [*print-meta*     true
            *print-dup*      nil
            *print-length*   nil
            *print-level*    nil
            *print-readably* true]
    (pr-str x)))

(defn deserialize
  "Perform edn->value de-serialization on passed values."
  [x-str]
  (tag/read-string x-str))

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

  - `save-snapshot-callback` : Accepts ktable-id and ktable-value. Must persist this value in the blob store.
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
                      (loop [current-value nil
                             timeout-ch    (make-new-timeout)]
                        (let [[v ch] (csp/alts!! [new-values-ch timeout-ch])

                              [new-value new-timeout-ch :as recur-params]
                              (cond
                                ;; if we receive nil, the input ch was closed
                                ;; it should be safer to just silently ignore the last value, due to the previously saved snapshot
                                (nil? v)
                                nil
                                
                                ;; we've received a new value, so we want to keep it for when we decide to make a snapshot
                                ;; We keep the current `timeout-ch`
                                (= ch new-values-ch)
                                [v timeout-ch]

                                ;; we've timed out, so it's time to actually save the current value (if it is a value)
                                (= ch timeout-ch)
                                (let [next-timeout (make-new-timeout)]
                                  (when current-value
                                    (log/debug "Saving new snapshot.")
                                    ;; we have something to save and it is time to save it
                                    (save-snapshot-callback current-value))
                                  ;; since we've saved the last value, our next current value is `nil` again
                                  [nil next-timeout]))]
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
  [{:as _cfg :keys [clock storage-path service-health-trip-switch]}
   ktable-id
   checkpoint-value]
  (log/with-context+ {:ktable-id    ktable-id
                      :storage-path storage-path}
    (try
      (let [storage-dir        (ensure-fs-directory-path! storage-path)
            checkpoint-dir     (ensure-fs-directory-path! (.getAbsolutePath (File. storage-dir ktable-id)))
            checkpoint-instant (-time/get-current-time clock)
            checkpoint-name    (format "%d-%s.edn"
                                       (t/to-millis-from-epoch (t/instant))
                                       (t/format :iso-offset-date-time
                                                 (t/zoned-date-time checkpoint-instant
                                                                    (t/zone-id "UTC"))))]
        (spit (File. checkpoint-dir checkpoint-name)
              (serialize checkpoint-value)))
      (catch Throwable t
        (log/error t "Unable to store ktable checkpoints! Tripping health trip switch")
        (-health/indicate-unhealthy! service-health-trip-switch
                                     ::filesystem-ktable-checkpoint)))))

(def checkpoint-filename-re #"(\d+)-.*\.edn")

(comment

  (re-matches checkpoint-filename-re
              "1742903513700-2025-03-25T11:51:53.700182Z.edn")

  )

(defn retrieve-latest-ktable-checkpoint
  "Lists all checkpoint values in files, scans for a name that is most recent, based on the millis-since-epoch before the first \\-
  and returns that value."
  [{:as _cfg :keys [storage-path]}
   ktable-id]
  (let [storage-dir        (ensure-fs-directory-path! storage-path)
        checkpoint-dir     (ensure-fs-directory-path! (.getAbsolutePath (File. ^File storage-dir ^String ktable-id)))
        ^java.io.FileFilter checkpoint-file-filter (fn [f]
                                                     (boolean (and (.isFile f)
                                                                   (re-matches checkpoint-filename-re (.getName f)))))
        last-checkpoint        (->> (.listFiles ^File checkpoint-dir checkpoint-file-filter)
                                    (map (juxt (comp (partial re-matches checkpoint-filename-re)
                                                     #(.getName ^File %))
                                               identity))
                                    (map #(update-in % [0 1] Long/parseLong))
                                    (reduce (fn [[current-millis _current-file :as current-pick] [[_ file-millis] f]]
                                              (if (> file-millis current-millis)
                                                [file-millis f]
                                                current-pick))
                                            [0 nil])
                                    second)]
    (when last-checkpoint
      (deserialize (slurp last-checkpoint)))))

(defn make-checkpoint-storage-component
  "Returns an implementation of `IKTableCheckpointStorage` that stores checkpoints on the local file system."
  [{:as   cfg
    :keys [storage-path]}]

  ;; TODO: This must move to where file-system logic is kept
  ;; We are keeping this (somewhere) because we want the potential throw to happen in the thread that creates the component,
  ;; so it can fail early rather than later in a worker thread.
  (ensure-fs-directory-path! storage-path)
  
  (let [checkpoint (make-checkpoint-fn cfg
                                       (partial store-ktable-checkpoint! cfg))]
    (reify
      IKTableCheckpointStorage
      (register-ktable-value [_ ktable-id ktable-value]
        (checkpoint [ktable-id ktable-value]))
      (retrieve-latest-checkpoint [_ ktable-id]
        (retrieve-latest-ktable-checkpoint cfg ktable-id))

      -comp/IHaltable
      (halt [_] (checkpoint nil)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::storage-path (s/and string?
                             (comp pos? count)))
(s/def ::timewindow-duration
  (s/or :duration-spec (s/tuple pos-int? #{:seconds :minutes :hours})
        :duration-instance #(instance? java.time.Duration %)))
(s/def ::filesystem-ktable-checkpoint-cfg
  (s/keys :req-un [::-time/clock
                   ::storage-path
                   ::timewindow-duration
                   ::-health/service-health-trip-switch]))

(-comp/defcomponent {::-comp/ig-kw                  ::filesystem-ktable-checkpoint
                     ::-comp/identity-component-cfg ::filesystem-ktable-checkpoint-cfg}
  [cfg] (make-checkpoint-storage-component (update cfg :timewindow-duration normalize-duration)))
