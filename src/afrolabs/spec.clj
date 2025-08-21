(ns afrolabs.spec
  (:require [clojure.spec.alpha :as s]))


(defn assert!
  [spec x]
  (when-not (s/valid? spec x)
    (throw (ex-info (format "Invalid spec data!")
                    {:spec         spec
                     :explain-data (s/explain-data spec x)})))
  x)
