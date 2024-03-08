(ns afrolabs.version
  (:require  [clojure.java.io :as io]
             [clojure.edn :as edn]
             [clojure.java.shell :refer [sh]]
             [clojure.string :as str]))

(defn git-cmd [& args]
  (let [{:keys [exit out]}
        (apply sh "/usr/bin/env" "git" args)]
    (when (= 0 exit)
      (str/trim out))))

;; This nette/version.edn is created during build.
;; In production we are counting on nette/version.edn being available.
(defn read-version-info
  [version-info-resource]
  (or (let [loaded (some-> version-info-resource
                           (io/resource)
                           (slurp)
                           (edn/read-string)
                           (select-keys [:git-sha :git-ref]))]
        (when (seq loaded)
          loaded))
      {:git-ref (or (git-cmd "symbolic-ref" "HEAD")
                    "GIT-REF-FALLBACK")
       :git-sha (or (when-let [ref (git-cmd "rev-parse" "HEAD")]
                      (str ref (when (-> (git-cmd "diff" "--stat")
                                         count
                                         (> 0))
                                 ":DIRTY")))
                    "GIT-SHA-FALLBACK")}))
