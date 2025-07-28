(ns afrolabs.components.time
  (:require [java-time :as t]
            [integrant.core :is ig]
            [afrolabs.components :as -comp]
            [clojure.spec.alpha :as s]))

(defprotocol IClock
  (get-current-instant [this] "Returns the current instant.")
  (get-current-time [this] "Retuns the current instant")
  (get-local-date [this] "Returns a local date."))

(s/def ::system-time-cfg (s/keys :req-un []
                                 :opt-un []))
(s/def ::clock #(satisfies? IClock %))

(def system-time-instance
  (reify
    IClock
    (get-current-instant [_] (t/instant))
    (get-current-time [_]    (t/instant))
    (get-local-date [_]      (t/local-date))))

(-comp/defcomponent {::-comp/ig-kw       ::system-time
                     ::-comp/config-spec ::system-time-cfg}
  [_] system-time-instance)
