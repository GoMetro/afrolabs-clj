(ns afrolabs.components.kafka.checkpoint-storage.stores
  (:require
   [afrolabs.components :as -comp]
   [afrolabs.components.health :as -health]
   [clojure.spec.alpha :as s]
   [integrant.core :as ig]
   [taoensso.timbre :as log]
   )
  (:import
   [java.io
    File FileOutputStream FileInputStream FilenameFilter]
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
