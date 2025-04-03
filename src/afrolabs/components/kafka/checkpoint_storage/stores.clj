(ns afrolabs.components.kafka.checkpoint-storage.stores
  (:require
   [afrolabs.components :as -comp]
   [afrolabs.components.health :as -health]
   [clojure.spec.alpha :as s]
   [clojure.java.io :as io]
   [cognitect.aws.client.api :as aws]
   [integrant.core :as ig]
   [taoensso.timbre :as log]
   )
  (:import
   [java.io
    File FileOutputStream FileInputStream FilenameFilter SequenceInputStream
    PipedInputStream PipedOutputStream InputStream]
   ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def checkpoint-id-re #"(\d+)-.*\.edn(\.gz)?")

(defn- validate-checkpoint-id!
  [checkpoint-id]
  (when-not (re-matches checkpoint-id-re checkpoint-id)
    (throw (ex-info "checkpoint-id must match the pattern for checkpoint-ids."
                    {:regex  checkpoint-id-re
                     :actual checkpoint-id}))))

(comment

  (re-matches checkpoint-id-re
              "1742903513700-2025-03-25T11:51:53.700182Z.edn")
  (re-matches checkpoint-id-re
              "1742903513700-2025-03-25T11:51:53.700182Z.edn.gz")

  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol ICheckpointStore
  "A wrapper around a blob-store (the filesystem or S3 or something else) which supports
  the operations that the checkpoint-storage requires from a blob store.

  It is assumed that `ktable-id` & `entry-id` form part of the final name of the entry in the blobstore."
  (open-storage-stream [_ ktable-id checkpoint-id]
    "Returns an instance of OutputStream, into which the ktable's value will be streamed.
The client is responsible for closing the stream.")
  (list-checkpoint-ids [_ ktable-id]
    "Returns a sequence of opaque \"handles\" to checkpoint-id's that can subsequently be loaded.")
  (open-retrieval-stream [_ ktable-id checkpoint-id]
    "Returns an instance of InputStream, from which a specific ktable's checkpoint can be hydrated.
The client is responsible for closing the stream.")
  (delete-checkpoint-id [_ ktable-id checkpoint-id]
    "Instructs the blob-store to delete a specific checkpoint."))

(s/def ::checkpoint-store #(satisfies? ICheckpointStore %))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; file system
;; useful for local dev

(def ^{:arglists '([storage-path])
       :doc      "Checks that the path exists and is a directory, or creates it, or throws. Caches results per storage-path."}
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

;;;;;;;;;;;;;;;;;;;;

(defn- fs:open-checkpoint-output-stream
  "Actually persists the passed `checkpoint-value` to a \"path\" in the storage medium based on `ktable-id`."
  [{:keys [fs:store-root
           service-health-trip-switch]}
   ktable-id
   checkpoint-id]

  (validate-checkpoint-id! checkpoint-id)
  
  (log/with-context+ {:ktable-id     ktable-id
                      :storage-path  fs:store-root
                      :checkpoint-id checkpoint-id}
    (try
      (let [checkpoint-dir (ensure-fs-directory-path!
                            (.getAbsolutePath (File. ^String fs:store-root
                                                     ^String ktable-id)))]

        (FileOutputStream. (File. ^File checkpoint-dir
                                  ^String checkpoint-id)))
      (catch Throwable t
        (log/error t "Unable to open a FileOutputStream for storing a ktable checkpoint.")
        (-health/indicate-unhealthy! service-health-trip-switch
                                     ::filesystem-ktable-checkpoint)
        (throw t)))))

(def ^{:tag     FilenameFilter
       :private true}
  checkpoint-filename-filter
  (fn [^File _dir ^String fname]
    (boolean (re-matches checkpoint-id-re fname))))

(defn- fs:list-checkpoint-ids
  [{:keys [fs:store-root
           service-health-trip-switch]}
   ktable-id]
  (log/with-context+ {:ktable-id    ktable-id
                      :storage-path fs:store-root}
    (try
      (let [checkpoint-dir (ensure-fs-directory-path!
                            (.getAbsolutePath (File. ^String fs:store-root
                                                     ^String ktable-id)))]
        (.list ^File checkpoint-dir
               checkpoint-filename-filter))
      (catch Throwable t
        (log/error t "Unable to retrieve a file listing for the checkpoint store.")
        (-health/indicate-unhealthy! service-health-trip-switch
                                     ::filesystem)
        (throw t)))))

(defn- fs:open-checkpoint-id-stream
  [{:keys [fs:store-root
           service-health-trip-switch]}
   ktable-id
   checkpoint-id]

  (validate-checkpoint-id! checkpoint-id)
  
  (log/with-context+ {:ktable-id     ktable-id
                      :checkpoint-id checkpoint-id
                      :storage-path  fs:store-root}
    (try
      (let [checkpoint-dir (ensure-fs-directory-path!
                            (.getAbsolutePath (File. ^String fs:store-root
                                                     ^String ktable-id)))
            checkpoint-file (File. ^File checkpoint-dir
                                   ^String checkpoint-id)]
        (FileInputStream. checkpoint-file))
      (catch Throwable t
        (log/error t "Unable to open a FileinputStream on checkpoint file.")
        (-health/indicate-unhealthy! service-health-trip-switch
                                     ::filesystem)
        (throw t)))))


(defn- fs:delete-checkpoint
  [{:keys [fs:store-root
           service-health-trip-switch]}
   ktable-id
   checkpoint-id]

  (validate-checkpoint-id! checkpoint-id)
  
  (log/with-context+ {:ktable-id     ktable-id
                      :storage-path  fs:store-root
                      :checkpoint-id checkpoint-id}
    (try
      (let [checkpoint-dir (ensure-fs-directory-path!
                            (.getAbsolutePath (File. ^String fs:store-root
                                                     ^String ktable-id)))]
        (.delete (File. ^File checkpoint-dir
                        ^String checkpoint-id)))
      (catch Throwable t
        (log/error t "Unable to delete checkpoint file.")
        (-health/indicate-unhealthy! service-health-trip-switch
                                     ::filesystem)
        (throw t)))))

;;;;;;;;;;;;;;;;;;;;

(defn make-filesystem-checkpoint-store
  [{:as cfg
    :keys [fs:store-root]}]
  (ensure-fs-directory-path! fs:store-root)
  (reify
    ICheckpointStore
    (open-storage-stream [_ ktable-id checkpoint-id]
      (fs:open-checkpoint-output-stream cfg
                                        ktable-id
                                        checkpoint-id))

    (list-checkpoint-ids [_ ktable-id]
      (fs:list-checkpoint-ids cfg
                              ktable-id))

    (open-retrieval-stream [_ ktable-id checkpoint-id]
      (fs:open-checkpoint-id-stream cfg
                                    ktable-id
                                    checkpoint-id))
    
    (delete-checkpoint-id [_ ktable-id checkpoint-id]
      (fs:delete-checkpoint cfg
                            ktable-id
                            checkpoint-id))))

;;;;;;;;;;;;;;;;;;;;

(s/def ::fs:store-root
  (s/and string?
         (fn [p]
           (and (pos? (count p))
                (let [d (java.io.File. ^String p)]
                  (or (not (.exists d))
                      (and (.exists d)
                           (.isDirectory d))))))))

(s/def ::filesystem-cfg
  (s/keys :req-un [::fs:store-root
                   ::-health/service-health-trip-switch]))

(-comp/defcomponent {::-comp/ig-kw       ::filesystem
                     ::-comp/config-spec ::filesystem-cfg}
  [cfg] (make-filesystem-checkpoint-store cfg))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; The cognitect aws library has some (understandable) issues with (very) large S3 files.
;; - GetObject returns a realized byte[], which obviously requires as much memory as the response is big.
;;   For very large objects you might run into the issue where the JVM cannot access as much RAM as that.
;; - GetObject has an internal limitation which prevents loading data larger than Integer/MAX_VALUE.
;;   This is vaguely related to how java uses int's to access arrays.
;;
;; More info in this thread: https://github.com/cognitect-labs/aws-api/issues/209
;;
;; The code below is from that bug-report and relies on chunking downloads into a sequence of
;; byte[] and then "streaming" those with the help of java.io.SequenceInputStream & clojure.lang.SeqEnumeration
;;
;; We picked the chunk-size-bytes to be 1Gb because this is "large"-ish, and < Integer/MAX_VALUE (~2 GB)

(defn- parse-content-range
  "Extract the object size from the ContentRange response attribute and convert to Long type
  e.g. \"bytes 0-5242879/2243897556\"

  Returns 2243897556"
  [content-range]
  (when-let [object-size (re-find #"[0-9]+$" content-range)]
    (Long/parseLong object-size)))

(def ^{:private true
       :dynamic true
       :doc     "The size of the byte[]/chunks of data that is uploaded or downloaded to and from S3."}
  *chunk-size-bytes*
  (* 100 1024 1024)) ;; 100 MB - a little arbitrarily chosen from code samples in AWS docs

(defn- get-object-chunks
  [s3-client {:keys [bucket, key]}]
  (iteration (fn [range-byte-pos]
               (let [to-byte-pos (+ range-byte-pos *chunk-size-bytes*)
                     range (str "bytes=" range-byte-pos "-" (dec to-byte-pos))
                     op-map {:op :GetObject :request {:Bucket bucket :Key key :Range range}}
                     {:keys [ContentRange] :as response} (aws/invoke s3-client op-map)]
                 (println :range range :response response)

                 ;; todo: check the response for errors

                 (assoc response :range-byte-pos to-byte-pos
                                 :object-size (parse-content-range ContentRange))))
             :initk 0
             :kf (fn [{:keys [range-byte-pos, object-size]}]
                   (when (< range-byte-pos object-size)
                     range-byte-pos))
             :vf :Body))

(defn- seq-enumeration
  "Returns a java.util.Enumeration on a seq"
  {:static true}
  [coll]
  (clojure.lang.SeqEnumeration. coll))

(defn- s3-object->InputStream
  "Returns an InputStream. It is the caller's responsibility to close this stream when done reading from it."
  [s3-client s3-obj-address]
  (SequenceInputStream. (seq-enumeration (sequence (get-object-chunks s3-client s3-obj-address)))))

(defn- input-stream->chunks
  "Reads bytes from an `InputStream` and chunks it up into calls to (callback ^byte[] chunk ^long nr-of-bytes).

  Accepts a (java.io.InputStream) `input-stream` & `callback`.

  `callback` is `(fn [^bytes bs nr-of-bytes])`.
  - `bs` is a byte array
  - The first `nr-of-bytes` bytes of `bs` is the data in the chunk. This might be less than `bs.length`.
  - One final call will be made with `(callback nil -1)` to indicate the end of the stream.

  The byte array (1st parameter of callback) is the same instance for the lifetime of `input-stream->chunks`.
  This violates value semantics! The receiver of this data MUST read & copy the data out of this array
  before returning the callback.

  A callback mechanism is used in order to control memory allocation. (1 x byte[*chunk-size-bytes*])"
  [^InputStream input-stream
   callback]
  (let [bs (byte-array *chunk-size-bytes*)]
    (loop [remaining-bytes *chunk-size-bytes*]
      (let [read-this-time (.read input-stream
                                  bs
                                  (- *chunk-size-bytes* remaining-bytes)
                                  remaining-bytes)
            loop-step-result
            (cond
              ;; the stream is done, call back with what we have
              (= -1 read-this-time)
              (let [in-this-chunk (- *chunk-size-bytes* remaining-bytes)]
                (when (pos? in-this-chunk)
                    (callback bs in-this-chunk))
                  nil ;; we are done uploading chunks
                  )

              ;; we read as much as we had space, we have a full chunk
              (= remaining-bytes read-this-time)
              (do (callback bs *chunk-size-bytes*)
                  *chunk-size-bytes*)

              ;; we read less than remaining-bytes, read some more, the chunk has more space left
              (< read-this-time remaining-bytes)
              (- remaining-bytes read-this-time))]

        (when loop-step-result
          (recur loop-step-result))))
    (callback nil -1)))

(comment

  (binding [*chunk-size-bytes* 2]
    (let [bs  (byte-array (range 25))
          result-a (atom [])]
      (with-open [bis (java.io.ByteArrayInputStream. bs)]
        (input-stream->chunks bis
                              #(when %1 ;; on the last call, byte-array is `nil` and number-of-bytes is `-1`
                                 (swap! result-a
                                        into
                                        ;; important that a copy is made of the incoming byte-array contents
                                        (take %2 %1)))))
      @result-a))
  ;; [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 21 22 23 24]


  )




(defn- open-s3-upload-stream
  "Will return a `java.io.OutputStream` which can be used to upload data (byte[]) to an s3 object.

  It is the responsibility of the caller to close the result stream."
  [s3-client {:as destination-address
              :keys [bucket key]}]
  (let [result       (PipedOutputStream.)
        input-stream (PipedInputStream. result
                                        *chunk-size-bytes*)
        ]
    ;; we start a thread that will read data in chunks from the OutputStream and upload it to S3





    result))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- s3:open-checkpoint-output-stream
  [cfg])

(defn- s3:list-checkpoint-ids
  [cfg])

(defn- s3:open-checkpoint-id-stream
  [cfg]
  )

(defn- s3:delete-checkpoint
  [cfg])

(defn make-s3-checkpoint-store
  [{:as cfg}]
  (reify
    ICheckpointStore
    (open-storage-stream [_ ktable-id checkpoint-id]
      (s3:open-checkpoint-output-stream cfg
                                        ktable-id
                                        checkpoint-id))

    (list-checkpoint-ids [_ ktable-id]
      (s3:list-checkpoint-ids cfg
                              ktable-id))

    (open-retrieval-stream [_ ktable-id checkpoint-id]
      (s3:open-checkpoint-id-stream cfg
                                    ktable-id
                                    checkpoint-id))

    (delete-checkpoint-id [_ ktable-id checkpoint-id]
      (s3:delete-checkpoint cfg
                            ktable-id
                            checkpoint-id))))

;;;;;;;;;;;;;;;;;;;;


(s/def ::s3:bucketname (s/and string?
                              (comp pos? count)))
(s/def ::s3:path-prefix (s/and string?
                               (comp pos? count)))
(s/def ::s3-cfg
  (s/keys :req-un [::s3:bucketname
                   ::s3:path-prefix
                   ::-health/service-health-trip-switch]))

(-comp/defcomponent {::-comp/ig-kw       ::s3
                     ::-comp/config-spec ::s3-cfg}
  [cfg] (make-s3-checkpoint-store cfg))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TODO: If the number of checkpoint storage providers grow, a better mechanism for extensibility is required
;; that does not require modification of this component to achieve.

(defmethod ig/init-key ::checkpoint-storage-switcher
  [_ {:as cfg t :type}]
  (case t
    ::filesystem (ig/init-key ::filesystem cfg)))

(defmethod ig/halt-key! ::checkpoint-storage-switcher
  [_ state]
  (when (satisfies? -comp/IHaltable state)
    (-comp/halt state)))
