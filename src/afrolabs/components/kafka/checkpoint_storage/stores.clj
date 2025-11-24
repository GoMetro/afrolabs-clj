(ns afrolabs.components.kafka.checkpoint-storage.stores
  (:require
   [afrolabs.components :as -comp]
   [afrolabs.components.aws :as -aws]
   [afrolabs.components.aws.client-config :as -aws-client-config]
   [clojure.core.async :as csp]
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [cognitect.aws.client.api :as aws]
   [integrant.core :as ig]
   [taoensso.timbre :as log]
   )
  (:import
   [java.io
    File FileOutputStream FileInputStream
    FilenameFilter
    SequenceInputStream PipedInputStream PipedOutputStream InputStream OutputStream
    ByteArrayInputStream ByteArrayOutputStream]
   [java.security
    MessageDigest]
   [java.util
    Base64]
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
  [{:keys [fs:store-root]}
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
        (throw t)))))

(def ^{:tag     FilenameFilter
       :private true}
  checkpoint-filename-filter
  (fn [^File _dir ^String fname]
    (boolean (re-matches checkpoint-id-re fname))))

(defn- fs:list-checkpoint-ids
  [{:keys [fs:store-root]}
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
        (throw t)))))

(defn- fs:open-checkpoint-id-stream
  [{:keys [fs:store-root]}
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
        (throw t)))))


(defn- fs:delete-checkpoint
  [{:keys [fs:store-root]}
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
  (s/keys :req-un [::fs:store-root]))

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
;; We picked the chunk-size-bytes to be a certain size in order to be "large"-ish, and < Integer/MAX_VALUE (~2 GB)

(defn- parse-content-range
  "Extract the object size from the ContentRange response attribute and convert to Long type
  e.g. \"bytes 0-5242879/2243897556\"

  Returns 2243897556"
  [content-range]
  (when-let [object-size (re-find #"[0-9]+$" content-range)]
    (Long/parseLong object-size)))

(def ^{:private false
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

                     {:keys [ContentRange] :as response}
                     (-aws/throw-when-anomaly (aws/invoke s3-client op-map))]
                 ;; errors result in exceptions

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
  [{:keys [s3-client]} s3-obj-address]
  (SequenceInputStream. (seq-enumeration (sequence (get-object-chunks s3-client s3-obj-address)))))

(comment

  (with-open [s3-is (s3-object->InputStream {:s3-client  s3-client}
                                            {:bucket "ktable-checkpoint-store20250401120818030200000001"
                                             :key "test.edn"})
              fos (FileOutputStream. (File. "/tmp/test.edn"))]
    (io/copy s3-is fos))


  )

;;;;;;;;;;

(defn- input-stream->chunks
  "Reads bytes from an `InputStream` and chunks it up into calls to (callback-with-chunk ^byte[] chunk nr-of-bytes).

  Accepts a (java.io.InputStream) `input-stream` & `callback-with-chunk`.

  The `input-stream` will not be closed in `input-stream->chunks`.

  `callback-with-chunk` is `(fn [^bytes bs nr-of-bytes])`.
  - `bs` is a byte array
  - The first `nr-of-bytes` bytes of `bs` is the data in the chunk. This might be less than `bs.length`.
  - One final call will be made with `(callback-with-chunk nil -1)` to indicate the end of the stream.

  The byte array (1st parameter of callback-with-chunk) is the same instance for the lifetime of `input-stream->chunks`.
  This violates value semantics! The receiver of this data MUST read & copy the data out of this array
  before returning the callback-with-chunk.

  A callback-with-chunk mechanism is used in order to control memory allocation. (1 x byte[*chunk-size-bytes*])"
  [^InputStream input-stream
   callback-with-chunk]
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
                    (callback-with-chunk bs in-this-chunk))
                  nil ;; we are done uploading chunks
                  )

              ;; we read as much as we had space, we have a full chunk
              (= remaining-bytes read-this-time)
              (do (callback-with-chunk bs *chunk-size-bytes*)
                  *chunk-size-bytes*)

              ;; we read less than remaining-bytes, read some more, the chunk has more space left
              (< read-this-time remaining-bytes)
              (- remaining-bytes read-this-time))]

        (when loop-step-result
          (recur loop-step-result))))
    (callback-with-chunk nil -1)))

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

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  (require '[afrolabs.components.aws.sso :as -aws-sso-profile-provider])

  (def s3-client (aws/client {:api :s3
                              :region "af-south-1"
                              :credentials-provider (-aws-sso-profile-provider/provider (or (System/getenv "AWS_PROFILE")
                                                                                            (System/getProperty "aws.profile")
                                                                                            "default"))}))

  (aws/ops s3-client)
  (aws/invoke s3-client
              {:op :ListObjects
               :request {:Bucket "ktable-checkpoint-store20250401120818030200000001"}})


  )

;;;;;;;;;;;;;;;;;;;;

(defn- input-stream->multi-part-s3-upload
  "Accepts a (java.io.) InputStream, an s3-client and an s3 object coordinate/destination.

  Breaks up the InputStream into chunks of size `*chunk-size-bytes*`
  and uploads each chunk using S3 Multipart upload.

  Verifies the upload using SHA-1 checksums.

  This fn will not close the InputStream, but will consume it fully."
  [^InputStream input-stream
   s3-client
   {:as   _destination
    :keys [bucket key]}]
  (let [full-checksum-byte-os (ByteArrayOutputStream.)
        upload-state     (atom [])

        {:as   _create-multipart-upload-result
         :keys [UploadId]}
        (log/spy :debug "CreateMultipartUpload Result"
                 (-aws/throw-when-anomaly
                  (aws/invoke s3-client
                              {:op      :CreateMultipartUpload
                               :request {:Bucket            bucket
                                         :Key               key
                                         :ChecksumAlgorithm "SHA1"}})))]

    (input-stream->chunks
     input-stream
     (fn [^bytes chunk-bytes nr-of-bytes-in-chunk]
       (if (and chunk-bytes (pos? nr-of-bytes-in-chunk))
         ;; still uploading chunks
         (let [;; create a checksum for this part
               part-sha1-checksum-bytes
               (let [md (MessageDigest/getInstance "SHA-1")]
                 (.update md chunk-bytes 0 nr-of-bytes-in-chunk)
                 (.digest md))

               ;; Update the checksum/digest for the full upload.
               ;; We are sending the bytes of the part checksums into the full digest.
               ;; from https://docs.aws.amazon.com/AmazonS3/latest/userguide/tutorial-s3-mpu-additional-checksums.html
               _ (.write full-checksum-byte-os
                         part-sha1-checksum-bytes)

               part-sha1-checksum
               (String.
                (.encode (Base64/getEncoder)
                         ^bytes part-sha1-checksum-bytes))

               {:as   part-upload-result
                :keys []}
               (log/spy :debug "UploadPart Result"
                        (-aws/throw-when-anomaly
                         (aws/invoke s3-client
                                     {:op      :UploadPart
                                      :request (log/spy :debug "UploadPart Request"
                                                        {:Bucket       bucket
                                                         :Key          key
                                                         :UploadId     UploadId
                                                         :PartNumber   (inc (count @upload-state))
                                                         :ChecksumSHA1 part-sha1-checksum
                                                         :Body         (ByteArrayInputStream. chunk-bytes
                                                                                              0
                                                                                              nr-of-bytes-in-chunk)})})))]

           ;; store the upload result so we know how many parts we've uploaded
           (swap! upload-state conj part-upload-result))

         ;; this is the finalizer - else part of `if`
         (let [upload-state' (log/spy :debug "final upload state"
                                      @upload-state)

               ^bytes full-checksum-sha-bytes
               (let [md (MessageDigest/getInstance "SHA-1")]
                 (.update md (.toByteArray full-checksum-byte-os))
                 (.digest md))

               full-checksum
               (str (String.
                     (.encode (Base64/getEncoder)
                              full-checksum-sha-bytes))
                    "-" (count upload-state'))

               _ (log/spy :debug "full checksum" full-checksum)]
           (log/spy :debug "CompleteMultipartUpload"
                    (-aws/throw-when-anomaly
                     (aws/invoke s3-client
                                 {:op      :CompleteMultipartUpload
                                  :request (log/spy :debug "CompleteMultipartUpload Request"
                                                    {:Bucket       bucket
                                                     :Key          key
                                                     :UploadId     UploadId
                                                     :ChecksumSHA1 full-checksum
                                                     :MultipartUpload
                                                     {:Parts (vec (for [i    (range (count upload-state'))
                                                                        :let [state-item (get upload-state' i)]]
                                                                    (-> (select-keys state-item [:ETag :ChecksumSHA1])
                                                                        (assoc :PartNumber (inc i)))))}})})))))))))

(comment

  ;; list in-progress multipart uploads
  (def bucket "ktable-checkpoint-store20250401120818030200000001")
  (aws/invoke s3-client
              {:op :ListMultipartUploads
               :request {:Bucket bucket}})

  ;; careful with this, will stop/terminate ALL multipart uploads that have not complete
  (doseq [{:keys [UploadId
                  Key]}
          (:Uploads (aws/invoke s3-client
                                {:op :ListMultipartUploads
                                 :request {:Bucket bucket}}))]
    (aws/invoke s3-client
                {:op      :AbortMultipartUpload
                 :request {:Bucket   bucket
                           :UploadId UploadId
                           :Key      Key}})
    )

  (with-open [fis (FileInputStream.
                   (File.
                    "/home/pieter/Source/github.com/GoMetro/bridge-telemetry/clj/telemetry-checkpoint-storage/lkc-12gm26_device-ids-per-product/1743508443810-2025-04-01T11:54:03.810040286Z.edn"))]
    (input-stream->multi-part-s3-upload fis
                                        s3-client
                                        {:bucket "ktable-checkpoint-store20250401120818030200000001"
                                         :key    "test.edn"}))

  )

;;;;;;;;;;;;;;;;;;;;

(defn open-s3-upload-stream
  "Will return a `java.io.OutputStream` which can be used to upload data (byte[]) to an s3 object.

  It is the responsibility of the caller to close the result stream."
  [{:keys [s3-client]}
   destination-address]
  (let [result       (PipedOutputStream.)
        input-stream (PipedInputStream. result
                                        *chunk-size-bytes*)]
    ;; we start a thread that will read data in chunks from the OutputStream and upload it to S3
    (csp/thread (try (input-stream->multi-part-s3-upload input-stream
                                                         s3-client
                                                         destination-address)
                     (catch Throwable t
                       (log/error t "Unable to upload input-stream to s3.")
                       (.close input-stream)
                       (.close result))))

    result))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- s3:open-checkpoint-output-stream
  [{:as   cfg
    :keys [s3:bucketname
           s3:path-prefix]}
   ktable-id
   checkpoint-id]
  (let [key (str s3:path-prefix "/" ktable-id "/" checkpoint-id)]
    (log/with-context+ {:ktable-id ktable-id
                        :checkpoint-id checkpoint-id}
      (open-s3-upload-stream cfg
                             {:bucket s3:bucketname
                              :key    key}))))

(defn- s3:list-checkpoint-ids
  [{:as   _cfg
    :keys [s3-client
           s3:bucketname
           s3:path-prefix]}
   ktable-id]

  (let [key-prefix (str s3:path-prefix "/" ktable-id "/")]
    (->> (iteration (fn [k]
                      (log/spy :debug "s3:list checkpoint ids result"
                               (aws/invoke s3-client
                                           (log/spy :debug "s3:list checkpoint ids request"
                                                    {:op :ListObjectsV2
                                                     :request (cond-> {:Bucket s3:bucketname
                                                                       :Prefix key-prefix}
                                                                k (assoc :ContinuationToken k))}))))
                    :somef identity
                    :vf    :Contents
                    :kf    :NextContinuationToken
                    :initk nil)
         (mapcat identity)
         (map :Key)
         (keep (fn [s3-object-key]
                 (let [key-without-prefix (last (str/split s3-object-key #"/"))]
                   (when (re-matches checkpoint-id-re key-without-prefix)
                     key-without-prefix)))))))

(comment

  (str/split "dev/pieter/lkc-ozxkny_geofence-factual-state/1743760240460-2025-04-04T09:50:40.460144083Z.edn.gz"
             #"/")

  (aws/ops s3-client)
  (aws/invoke s3-client
              {:op :ListObjectsV2
               :request {:Bucket "ktable-checkpoint-store20250401120818030200000001"
                         :Prefix ""}})

  (s3:list-checkpoint-ids {:s3-client      s3-client
                           :s3:bucketname  "ktable-checkpoint-store20250401120818030200000001"
                           :s3:path-prefix "test"}
                          "boom-ktable-id")
  (count *1)

  (doseq [i (range 50 )]
    (with-open [^OutputStream os
                (s3:open-checkpoint-output-stream {:s3-client      s3-client
                                                   :s3:bucketname  "ktable-checkpoint-store20250401120818030200000001"
                                                   :s3:path-prefix "test"}
                                                  "boom-ktable-id"
                                                  (str "warra-" i))]
      (binding [*out* (io/writer os)]
        (pr {:i i
             :s (apply str (repeat i (str i)))})
        (flush))))



  )

(defn- s3:open-checkpoint-id-stream
  [{:as   cfg
    :keys [s3:bucketname
           s3:path-prefix]}
   ktable-id
   checkpoint-id]
  (let [key (str s3:path-prefix "/" ktable-id "/" checkpoint-id)]
    (s3-object->InputStream cfg
                            {:bucket s3:bucketname
                             :key    key})))

(defn- s3:delete-checkpoint
  [{:as   _cfg
    :keys [s3:bucketname
           s3:path-prefix
           s3-client]}
   ktable-id
   checkpoint-id]
  (let [key (str s3:path-prefix "/" ktable-id "/" checkpoint-id)]
    (-aws/throw-when-anomaly
     (aws/invoke s3-client
                 {:op      :DeleteObject
                  :request {:Bucket s3:bucketname
                            :Key    key}}))))

(defn make-s3-checkpoint-store
  [{:as   cfg
    :keys [aws-client-config]}]
  (let [cfg' (assoc cfg
                    :s3-client
                    (aws/client (assoc aws-client-config :api :s3)))]
    (reify
      ICheckpointStore
      (open-storage-stream [_ ktable-id checkpoint-id]
        (s3:open-checkpoint-output-stream cfg'
                                          ktable-id
                                          checkpoint-id))

      (list-checkpoint-ids [_ ktable-id]
        (s3:list-checkpoint-ids cfg'
                                ktable-id))

      (open-retrieval-stream [_ ktable-id checkpoint-id]
        (s3:open-checkpoint-id-stream cfg'
                                      ktable-id
                                      checkpoint-id))

      (delete-checkpoint-id [_ ktable-id checkpoint-id]
        (s3:delete-checkpoint cfg'
                              ktable-id
                              checkpoint-id)))))

;;;;;;;;;;;;;;;;;;;;


(s/def ::s3:bucketname (s/and string?
                              (comp pos? count)))
(s/def ::s3:path-prefix (s/and string?
                               (comp pos? count)))
(s/def ::s3-cfg
  (s/keys :req-un [::s3:bucketname
                   ::s3:path-prefix
                   ::-aws-client-config/aws-client-config]))

(-comp/defcomponent {::-comp/ig-kw       ::s3
                     ::-comp/config-spec ::s3-cfg}
  [cfg] (make-s3-checkpoint-store cfg))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TODO: If the number of checkpoint storage providers grow, a better mechanism for extensibility is required
;; that does not require modification of this component to achieve.

(defmethod ig/init-key ::checkpoint-storage-switcher
  [_ {:as cfg t :type}]
  (case t
    :filesystem (ig/init-key ::filesystem cfg)
    :s3         (ig/init-key ::s3 cfg)))

(defmethod ig/halt-key! ::checkpoint-storage-switcher
  [_ state]
  (when (satisfies? -comp/IHaltable state)
    (-comp/halt state)))
