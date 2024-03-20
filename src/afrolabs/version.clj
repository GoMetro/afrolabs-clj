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

;; The file read by this fn has to have this format:
;; {:git-ref  "refs/heads/pwab/BSA-103-add-git-commit-branch-info-into-deployed-artifact-so-you-can-see-what-version-is-running"
;;  :git-sha  "f7ba5fa0d8bfd1c8a078cae583f8b4cda39b59c3"}

(defn read-version-info
  [version-info-resource]
  (or (when-let [loaded (some-> version-info-resource
                           (io/resource)
                           (slurp)
                           (edn/read-string)
                           (select-keys [:git-sha :git-ref]))]
        loaded)
      {:git-ref (or (git-cmd "symbolic-ref" "HEAD")
                    "GIT-REF-FALLBACK")
       :git-sha (or (when-let [ref (git-cmd "rev-parse" "HEAD")]
                      (str ref (when (-> (git-cmd "diff" "--stat")
                                         count
                                         (> 0))
                                 ":DIRTY")))
                    "GIT-SHA-FALLBACK")}))
