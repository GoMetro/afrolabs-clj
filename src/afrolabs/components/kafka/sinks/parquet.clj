(ns afrolabs.components.kafka.sinks.parquet
  "This namespace exports a component that can be used to 'sink' a kafka topic into
  an S3 bucket like storage system with parquet encoding."
  (:require
   [afrolabs.components :as -comp]
   [afrolabs.components.aws.client-config :as -client-config]
   [afrolabs.components.health :as -health]
   [afrolabs.components.kafka :as -kafka]
   [afrolabs.components.kafka.back-pressure-aware-consumer :as -bpac]
   [afrolabs.components.kafka.checkpoint-storage.stores :as -checkpoint-stores]
   [afrolabs.components.time :as -time]
   [clojure.core.async :as csp]
   [clojure.spec.alpha :as s]
   [cognitect.aws.client.api :as aws]
   [integrant.core :as ig]
   [java-time.api :as t]
   [net.cgrand.xforms :as x]
   [taoensso.timbre :as log]
   [tech.v3.dataset :as ds]
   [tech.v3.libs.parquet :as ds-parquet]
   [com.potetm.fusebox.retry :as retry]
   )
  (:import
   [java.io File FileOutputStream])
  )

;;;;;;;;;;;;;;;;;;;;

(let [partition-instant-format-string (java.time.format.DateTimeFormatter/ofPattern "'year'=yyyy/'month'=MM/'day'=dd")
      zone-id                         (java.time.ZoneId/of "UTC")]
  (defn- instant->partition
    "This fn accepts an instant and returns a/the generic HIVE-style partition for this instant.
  `<year>=YYYY/<month>=MM/<day>=DD`

  NOTE:
  - Partition is based off the UTC zoned date
  - This version does not partition by hour.
  - This value will not start with `/` neither end with `/`."
    [i]
    (.format partition-instant-format-string
             (cond
               (t/instant? i)
               (.atZone i zone-id)

               (t/zoned-date-time? i)
               (t/with-zone-same-instant i zone-id)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::fs:store-root
  (s/and string?
         (fn [p]
           (and (pos? (count p))
                (let [d (java.io.File. ^String p)]
                  (or (not (.exists d))
                      (and (.exists d)
                           (.isDirectory d))))))))

(defn- fs:open-parquet-file-data-stream
  [{:keys [fs:store-root
           dataset-name]}
   partition]

  (let [dir-file (-> (File. fs:store-root)
                     (File. partition))]
    (log/with-context+ {:dir-file dir-file}
      (try
        (let [working-dir (afrolabs.components.kafka.checkpoint-storage.stores/ensure-fs-directory-path!
                           (.getAbsolutePath dir-file))
              output-file (File. ^File working-dir
                                 ^String (str "part-" (random-uuid) ".parquet"))]
          (FileOutputStream. output-file))
        (catch Throwable t
          (log/error t "Unable to open a FileinputStream on checkpoint file.")
          (throw t))))))

(s/def ::s3:bucket-name (s/and string?
                               (comp pos? count)))
(s/def ::s3:path-prefix (s/nilable string?))
(s/def ::storage-type #{:s3 :filesystem})

(def ^:dynamic *s3-chunk-file-size* (* 256 1024 1024))
(defn- s3:open-parquet-file-data-stream
  "Will return a `java.io.OutputStream` which can be used to upload data (byte[]) to an s3 object.

  It is the responsibility of the caller to close the result stream."
  [{:keys [s3-client
           s3:path-prefix
           s3:bucket-name]}
   partition]

  (let [destination-address (str (when (seq s3:path-prefix)
                                   s3:path-prefix )
                                 partition
                                 "/part-" (random-uuid) ".parquet")]
    ;; we are sneakily re-using the storage logic for ktable checkpoints
    ;; because it knows how to upload very large inputstreams to s3
    (binding [-checkpoint-stores/*chunk-size-bytes*                                 *s3-chunk-file-size*
              -checkpoint-stores/*input-stream->multi-part-s3-upload:extra-s3-args* {:ServerSideEncryption "AES256"}]
      (#'-checkpoint-stores/open-s3-upload-stream{:s3-client s3-client}
                                                 {:bucket s3:bucket-name
                                                  :key    destination-address}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defmulti open-parquet-file-data-stream (fn [{:keys [storage-type]} _partition]
                                          storage-type))
(defmethod open-parquet-file-data-stream :s3
  [cfg partition]
  (s3:open-parquet-file-data-stream cfg partition))
(defmethod open-parquet-file-data-stream :filesystem
  [cfg partition]
  (fs:open-parquet-file-data-stream cfg partition))

;;;;;;;;;;;;;;;;;;;;

(def max-export-retries 5)

;; "simple" retrier with exponential backoff
(def retryer (retry/init {::retry/retry? (fn [n ms ex]
                                           (< n max-export-retries))
                          ::retry/delay (fn [n ms ex]
                                          (min (retry/delay-exp 500 n)
                                               10000))}))

(defn- export!
  "Creates parquet file exports from everything in `state`. Returns `nil`."
  [{:as   cfg
    :keys [record->event-timestamp-column-name]}
   state]
  (doseq [[partition {:keys [dataset topic]}] state]
    (try (retry/with-retry retryer
           (with-open [fos (open-parquet-file-data-stream cfg partition)]
             (let [ds (ds/sort-by-column (dataset)
                                         (record->event-timestamp-column-name topic))]
               (ds-parquet/ds->parquet ds fos)
               (log/with-context+ {:partition  partition
                                   :nr-records (ds/row-count ds)}
                 (log/info "Saved parquet file.")))))
         (catch Throwable t
           (log/error t "Unable to persist parquet files even after retries."))))
  nil)

(defn- export-any!
  "Inspects the state and finds any accumulators that qualifies to roll a new parquet file.

  Triggers include
  - the maximum collection timewindow (no single parquet file must contain event data covering a larger duration than some max)
  - the number of records in the dataset is more than the maximum. (Any extras will just be included in the export)

  Returns a new state without the exported dataset data."
  [{:as   cfg
    :keys [clock
           max-file-duration
           max-nr-of-msgs]}
   state]
  (let [duration-based-cutof (t/- (-time/get-current-instant clock)
                                  max-file-duration)
        exportable?          (fn [[_key {:as   _state-item
                                         :keys [oldest-timestamp
                                                nr-records]}]]
                               (or (t/before? oldest-timestamp
                                              duration-based-cutof)
                                   (>= nr-records
                                       max-nr-of-msgs)))

        ;; sort the data into what must be exported and what must be held back a little longer
        {held-state       false
         exportable-state true} (into {}
                                      (x/by-key exportable?
                                                (x/into {}))
                                      state)]

    ;; export what qualified
    (when (seq exportable-state)
      (export! cfg exportable-state))

    held-state))

(defn- next-timeout
  "Inspects the state and returns a single instant when the oldest/earliest dataset is triggered for flushing.

  If the state is empty, returns `nil`."
  [{:as   _cfg
    :keys [max-file-duration]}
   state]
  (cond
    (not (seq state))
    nil

    (= 1 (count state))
    (t/+ (-> state first second :oldest-timestamp)
         max-file-duration)

    :else
    (let [earliest-timestamp
          (->> state
               (map (comp :oldest-timestamp second))
               (reduce t/min))]
      (t/+ earliest-timestamp
           max-file-duration))))

(defn- ->msg
  "Accepts a message and returns a 2-tuple of
  - the partition (string) of the dataset that the record belongs to, (used like an s3 path prefix)
  - the modified row data that must/will be insterted into the relevant dataset. (must be flat map/dict data)

  may return `nil` in which case the record must be discarded."
  [{:as   _cfg
    :keys [record->row:fn
           dataset-name]}
   {:as              kafka-msg
    :keys            [topic]}]

  (try (let [[event-ts row-data] (record->row:fn kafka-msg)
             dataset-partition   (str "/" dataset-name
                                      "/" topic
                                      "/" (instant->partition event-ts))]

         [dataset-partition row-data])
       (catch Throwable t
         (log/with-context+ (select-keys kafka-msg [:topic :partition :offset])
           (log/error t "->msg failed to work for a message."))
         nil ;; be explicit
         )))

(defn- collect!
  "Collects a new batch of messages into the state."
  [{:as   cfg
    :keys [clock]}
   state
   msgs]
  (let [right-now (-time/get-current-instant clock)]
    (reduce (fn [state' {:as msg :keys [topic]}]
              (let [[dataset-partition row-data] (->msg cfg msg)]
                (update state'
                        dataset-partition
                        (fnil (fn [{:as   partition-state
                                    :keys [dataset]}]

                                ;; this is a side-effect!
                                ;; `dataset` is a reader-fn that accepts one record at a time, building up fancy things in the background
                                (dataset row-data)
                                (update partition-state :nr-records inc))
                              {:oldest-timestamp right-now
                               :topic            topic
                               :dataset          (ds/mapseq-parser)
                               :nr-records       0}))))
            (or state {})
            msgs)))

(defn- cleanup!
  "Exports any and all state, flushes into new parquet files regardless of collection duration.

  This should only be used when the consumer is shutting down."
  [cfg state] (export! cfg state))

;;;;;;;;;;;;;;;;;;;;

(defn make-msgs-consumer-worker
  [{:as   cfg
    :keys [clock]}
   incoming-msgs-ch]
  (csp/thread (loop [state {}]
                ;; this loop performs the ->parquet export at the start of the loop, if there is anything to do
                (let [state           (export-any! cfg state) ;; NOTE: shadow-binding
                      next-to-instant (next-timeout cfg state)
                      next-timeout-ch (when next-to-instant
                                        (csp/timeout (.toMillis (t/duration (-time/get-current-instant clock)
                                                                            next-to-instant))))

                      [v ch] (csp/alts!! (remove nil?
                                                 [incoming-msgs-ch
                                                  next-timeout-ch]))

                      next-state
                      (cond
                        ;; stop signal
                        (and (= ch incoming-msgs-ch)
                             (nil? v))
                        (do (log/trace "cleaning up...")
                            (cleanup! cfg state)
                            nil)

                        ;; we now know we've received messages in v
                        ;; collect all of the messages into the state
                        (= ch incoming-msgs-ch)
                        (do (log/trace "collecting...")
                            (log/spy :trace "what was collected"
                                     (collect! cfg
                                               state
                                               v)))

                        ;; we were just sitting and waiting for a rolling timewindow to trigger, this is it
                        ;; return the current state, let the next iteration do any work required
                        (= ch next-timeout-ch)
                        (do (log/trace "timed out")
                            state))]
                  (when next-state
                    (recur next-state))))
              (log/info "Parquet sink worker thread stopped.")))

;;;;;;;;;;;;;;;;;;;;

(defn- resolve*
  [cfg field]
  (let [x (get cfg field)
        replacement?
        (or (when (fn? x) x)
            (let [rr (requiring-resolve x)
                  rr' (var-get rr)]
              (when (fn? rr') rr')))]
    (if replacement?
      (assoc cfg field replacement?)
      cfg)))

;;;;;;;;;;;;;;;;;;;;

(defmulti prepare-storage :storage-type)
(defmethod prepare-storage :s3
  [{:as cfg :keys [aws-client-config]}]
  (assoc cfg
         :s3-client
         (aws/client (assoc aws-client-config :api :s3))))

(def ^{:arglists '([storage-path])
       :doc      "Checks that the path exists and is a directory, or creates it, or throws. Caches results per storage-path."
       :private  true}
  ensure-fs-directory-path!
  (memoize (fn [storage-path]
             (let [storage-path-f (File. ^String storage-path)]
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
(defmethod prepare-storage :filesystem
  [{:as cfg :keys [fs:store-root]}]
  ;; side-effect only
  (ensure-fs-directory-path! fs:store-root)
  ;; return original
  cfg)

;;;;;;;;;;;;;;;;;;;;

(defn make-component
  [{:as cfg :keys [topic-regex-str
                   storage-type
                   fs:store-root
                   s3:bucket-name]}]

  ;; check storage parameters
  (case storage-type
    :s3 (when-not s3:bucket-name
          (throw (ex-info "When the parquet sink is used with S3, a :bucket-name parameter is required."
                          {:provided s3:bucket-name})))
    :filesystem (when-not fs:store-root
                  (throw (ex-info "When the parquet sink is used with filesystem, an :fs:store-root is required."
                                  {:provided fs:store-root}))))

  (tap> [:parquet-cfg cfg])

  (let [cfg (-> cfg
                (resolve* :record->event-timestamp-column-name)
                (resolve* :record->row:fn)
                (update   :max-file-duration (partial apply t/duration))
                (prepare-storage))
        incoming-msgs-ch (csp/chan 1)
        consumer-worker (make-msgs-consumer-worker cfg
                                                   incoming-msgs-ch)
        consumer-client (reify
                          -kafka/IConsumerClient
                          (consume-messages [_ msgs]
                            (when (seq msgs)
                              (csp/>!! incoming-msgs-ch msgs))
                            nil))

        back-pressure-aware-consumer (-bpac/make-strategy {:actual-consumer-client consumer-client})

        cfg (-> cfg
                (update :strategies (fn [strategies]
                                      (into strategies
                                            (remove nil?
                                                    [(-kafka/SubscribeWithTopicsRegex
                                                      (re-pattern topic-regex-str)
                                                      [back-pressure-aware-consumer])
                                                     back-pressure-aware-consumer
                                                     ]))))
                (assoc :consumer/client back-pressure-aware-consumer))

        consumer (-kafka/make-consumer cfg)]

    (reify
      clojure.lang.IDeref
      (deref [_] cfg)

      -comp/IHaltable
      (halt [_]
        (-comp/halt consumer)
        (csp/close! incoming-msgs-ch)
        (csp/<!! consumer-worker)))))

;;;;;;;;;;;;;;;;;;;;

(s/def ::max-nr-of-msgs pos-int?)
(s/def ::max-file-duration (s/or :duration-spec (s/tuple pos-int? #{:seconds :minutes :hours})
                                 :duration-instance #(instance? java.time.Duration %)))

;; this fn must return a 2-tuple [event-timestamp event-row-data]
(s/def ::symbol (s/or :fn fn?
                      :symbol-fn (fn [x]
                                   (and (symbol? x)
                                        (requiring-resolve x)))))
(s/def ::record->row:fn  ::symbol)
;; a map of topic name to data row column name for order of sorting
(s/def ::record->event-timestamp-column-name ::symbol)

(s/def ::dataset-name (s/and string?
                             (comp pos? count)))

(s/def ::topic-regex-str (fn [s]
                           (and (string? s)
                                (pos? (count s))
                                (re-pattern s))))

(s/def ::component-cfg (s/keys :req-un [::max-nr-of-msgs
                                        ::max-file-duration
                                        ::record->row:fn
                                        ::record->event-timestamp-column-name
                                        ::dataset-name
                                        ::topic-regex-str
                                        ::-client-config/aws-client-config
                                        ::-kafka/bootstrap-server
                                        ::-kafka/strategies
                                        ::-health/service-health-trip-switch
                                        ::-time/clock
                                        ::storage-type
                                        ]
                               :opt-un [::fs:store-root
                                        ::s3:bucket-name
                                        ::s3:path-prefix
                                        ]))

(-comp/defcomponent {::-comp/ig-kw              ::parquet-sink
                     ::-comp/config-spec        ::component-cfg
                     ::-comp/supports:disabled? true}
  [cfg] (make-component cfg))
