(ns afrolabs.spec
  (:require [clojure.spec.alpha :as s]))


(defn assert!
  [spec x]
  (when-not (s/valid? spec x)
    (throw (ex-info (format "Invalid data; '%s' is not valid '%s'" (str x) (str spec))
                    {:explain-str (s/explain-str spec x)
                     :explain-data (s/explain-data spec x)})))
  x)
