(ns afrolabs.components.version-info
  "Git version information of the running service.

  The version info resource is an EDN file on the classpath:

      {:git-ref \"refs/heads/main\"
       :git-sha \"f7ba5fa0d8bfd1c8a078cae583f8b4cda39b59c3\"}"
  (:require [afrolabs.components :as -comp]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]))

(defprotocol IVersionInfo
  :extend-via-metadata true
  (version-info [_] "Return `{:git-sha :git-ref}`."))

(defn git-cmd
  "Run `git` with `args`. Return trimmed stdout, or nil on a non-zero exit."
  [& args]
  (let [{:keys [exit out]}
        (apply sh "/usr/bin/env" "git" args)]
    (when (= 0 exit)
      (str/trim out))))

(defn read-version-info
  "Return `{:git-sha :git-ref}` read from the classpath resource
  `version-info-resource`.

  When the resource cannot be read, fall back to the working directory's git
  checkout (`:git-sha` suffixed with `:DIRTY` for uncommitted changes), then to
  `\"GIT-REF-FALLBACK\"` and `\"GIT-SHA-FALLBACK\"`."
  [version-info-resource]
  (or (some-> version-info-resource
              (io/resource)
              (slurp)
              (edn/read-string)
              (select-keys [:git-sha :git-ref]))
      {:git-ref (or (git-cmd "symbolic-ref" "HEAD")
                    "GIT-REF-FALLBACK")
       :git-sha (or (when-let [ref (git-cmd "rev-parse" "HEAD")]
                      (str ref (when (-> (git-cmd "diff" "--stat")
                                         count
                                         (> 0))
                                 ":DIRTY")))
                    "GIT-SHA-FALLBACK")}))

(s/def ::version-info-resource string?)
(s/def ::version-info #(or (satisfies? IVersionInfo %)
                           (contains? (meta %) `version-info)))
(s/def ::version-info-cfg (s/keys :req-un [::version-info-resource]))

(defn make-version-info
  "Return `{:git-sha :git-ref}` read once from `version-info-resource`,
  implementing `IVersionInfo`."
  [version-info-resource]
  (with-meta (read-version-info version-info-resource)
    {`version-info identity}))

(-comp/defcomponent {::-comp/config-spec ::version-info-cfg
                     ::-comp/ig-kw       ::version-info}
  [{:keys [version-info-resource]}]
  (make-version-info version-info-resource))
