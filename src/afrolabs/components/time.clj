(ns afrolabs.components.time
  (:require [java-time :as t]
            [afrolabs.components :as -comp]
            [clojure.spec.alpha :as s]))

(defprotocol IClock
  (get-current-time [this] "Retuns the current instant"))

(s/def ::system-time-cfg #{{}})
(s/def ::clock #(satisfies? IClock %))

(def system-time-instance
  (reify
    IClock
    (get-current-time [_] (t/instant))))

(-comp/defcomponent {::-comp/ig-kw ::system-time
                     ::-comp/spec  ::system-time-cfg}
  [_] system-time-instance)
