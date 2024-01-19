(ns afrolabs.components.kafka.utilities.healthcheck
  "Provides a utility healthcheck."
  (:require [taoensso.timbre :as log])
  (:import [afrolabs.components.health IServiceHealthTripSwitch]))

(defn make-fake-health-trip-switch
  "Creates an instance of a health-check that is useful in ad-hoc integrant systems.

  When something marks the system is un-healthy, will deliver on the promise."
  [system-is-unhealthy-promise]
  (reify
    IServiceHealthTripSwitch
    (indicate-unhealthy!
        [_ _]

      (log/error "The utility system is now unhealthy.")
      (deliver system-is-unhealthy-promise true))
    (wait-while-healthy
        [_]
      (log/warn "The utility health tripswitch cannot wait-while-healthy."))
    (healthy?
        [_]
      (log/warn "Returning constantly healthy...")
      true)))
