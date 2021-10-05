(ns afrolabs.components.kafka.utilities
  (:require [afrolabs.components.kafka :as k]
            [afrolabs.components.confluent :as -confluent]
            [afrolabs.components.confluent.schema-registry]
            [afrolabs.components.health :as -health]
            [integrant.core :as ig]
            [clojure.data.json :as json]
            [clojure.core.async :as csp]
            [taoensso.timbre :as log
             :refer [log  trace  debug  info  warn  error  fatal  report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]]
            [java-time :as time])
  (:import [afrolabs.components.health IServiceHealthTripSwitch]))

(defn load-messages-from-confluent-topic
  [& {:keys [bootstrap-server
             topics
             nr-msgs
             api-key api-secret
             extra-strategies
             msg-filter]
      :or {nr-msgs          10
           msg-filter       identity
           extra-strategies []}}]
  (let [loaded-msgs (atom nil)
        loaded-enough-msgs (promise)

        last-progress-update (atom (time/instant))

        caught-up-ch (csp/chan)
        _ (csp/go (csp/<! caught-up-ch)
                  (info "Caught up to the end of the subscribed topics, closing...")
                  (deliver loaded-enough-msgs true))

        health-trip-switch
        (reify
          IServiceHealthTripSwitch
          (indicate-unhealthy!
              [_ _]

            (log/error "load-messages-from-confluent-topic is unhealthy.")
            (deliver loaded-enough-msgs true))
          (wait-while-healthy
              [_]
            (log/warn "Cannot wait while the system is healthy..."))
          (healthy?
              [_]
            (log/warn "Return constantly healthy...")
            true))

        consumer-client
        (reify
          k/IConsumerClient
          (consume-messages
              [_ msgs]
            (let [msgs (filter msg-filter msgs)
                  new-state (swap! loaded-msgs (partial apply conj) msgs)
                  how-many (count new-state)]

              ;; do we have enough yet? is anything ever enough?
              (when (and nr-msgs
                         (< nr-msgs how-many))
                (info "Indicating that we've received enough messages...")
                (deliver loaded-enough-msgs true))

              ;; give progress indicators
              (let [t-now (time/instant)]
                (when (time/after? t-now
                                   (time/plus @last-progress-update
                                              (time/seconds 30)))
                  (info (str "Loaded " how-many " messages ..."))
                  (reset! last-progress-update t-now))))
            nil))

        ig-cfg
        {::k/kafka-consumer
         {:bootstrap-server               bootstrap-server
          :consumer/client                consumer-client
          :service-health-trip-switch     health-trip-switch
          :strategies                     (concat [(-confluent/ConfluentCloud :api-key api-key :api-secret api-secret)
                                                   (k/SubscribeWithTopicsCollection topics)
                                                   (k/FreshConsumerGroup)
                                                   (k/OffsetReset "earliest")
                                                   (k/CaughtUpNotifications caught-up-ch)]
                                                  extra-strategies)}}

        system (ig/init ig-cfg)]

    (try
      @loaded-enough-msgs
      (info "Done waiting, received enough messages.")
      (ig/halt! system)
      (info "System done shutting down.")

      ;; return value
      @loaded-msgs

      (catch Throwable t
        (warn t "Caught a throwable while waiting for messages to load. Stopping the system...")
        (ig/halt! system)
        (info "Extraordinary system stop completed.")))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn forward-topics-between-clusters
  [{:keys [bootstrap-server
           topics
           extra-strategies
           msg-filter]
    :or {msg-filter       identity
         extra-strategies []}
    :as from-cluster-cfg}
   {:keys [bootstrap-server
           extra-strategies]
    :or {extra-strategies []}
    :as to-cluster-cfg}])


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn produce-and-wait!
  "Intended for interactive use. Produces all of the records and wait for their delivery ack before returning."
  [producer msgs]
  (let [msgs (into []
                   (map #(assoc % :delivered-ch (csp/chan)))
                   msgs)]
    (k/produce! producer msgs)
    (doseq [{ch :delivered-ch} msgs] (csp/<!! ch))))
