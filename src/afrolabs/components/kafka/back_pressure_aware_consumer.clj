(ns afrolabs.components.kafka.back-pressure-aware-consumer
  "This namespace is occupied with making a strategy for a kafka consumer
  that will allow that consumer to take varying amount of time to process payloads.

  Some consumers, esp those that integrate with 3rd party systems shows swings in how long it takes to process
  consumption of kafka messages. This creates problems if the poll count drops too low. The strategy
  for dealing with this is to pause the consumer if it takes longer than some amount of time to consume
  a batch of kafka messages, and then to resume the consumer when the work queue has been cleared.

  This namespace will implement a component, that is also a kafka strategy.
  It will intercept the normal consumer-client and do this pausing/un-pausing in a transparent way."

  (:require
   [afrolabs.components :as -comp]
   [afrolabs.components.kafka :as -kafka]
   [clojure.core.async :as csp]
   [clojure.set :as set]
   [clojure.spec.alpha :as s]
   [integrant.core :as ig]
   [taoensso.timbre :as log]
   [afrolabs.prometheus :as -prom]
   [iapetos.core :as prom]
   )
  (:import
   [org.apache.kafka.clients.consumer
    Consumer]))

(-prom/register-metric (prom/gauge ::consumer-paused
                                   {:description "Is the backpressure-aware consumer strategy currently pausing the consumer?"
                                    :labels [:component]}))

(defn make-strategy
  "Creates an instance of a class which implements a bunch of protocols.
  Usage:
  - re-declare in your namespace
  - create integrant component as per usual
    - For the `:actual-consumer-client` pass your usual `:consumer/client` instance (IConsumerClient instance)
  - reference this component in your consumer configuration as `:consumer/client`
  - also in the `:strategies`, reference this instance
  - also in `:strategy/SubscribeWithTopicNameProvider` use this instance in `:consumer-rebalance-listeners`"

  [{:as                       _cfg
    :keys                     [actual-consumer-client]
    :afrolabs.components/keys [component-kw]}]
  (let [consumer*           (atom nil)
        assigned-partitions (atom nil)
        blocked-msgs        (atom [])
        msgs-in-outbox      (atom [])

        pause-unpause-state (atom nil)
        ensure-paused    (fn []
                           (when-not (= :paused @pause-unpause-state)
                             (log/warn "Back-pressure aware consumer strategy is pausing the consumer. This means the outbound integration is slow.")
                             (when-let [assigned-partitions* @assigned-partitions]
                               (.pause  ^Consumer @consumer* assigned-partitions*))
                             (prom/observe (get-gauge-consumer-paused {:component component-kw})
                                           1)
                             (reset! pause-unpause-state :paused)))
        ensure-unpaused  (fn []
                           (when-not (= :unpaused @pause-unpause-state)
                             (log/info "Back-pressure aware consumer strategy is consuming.")
                             (when-let [assigned-partitions* @assigned-partitions]
                               (.resume ^Consumer @consumer* assigned-partitions*))
                             (prom/observe (get-gauge-consumer-paused {:component component-kw})
                                           0)
                             (reset! pause-unpause-state :unpaused)))

        incoming-msgs        (csp/chan)
        result-msgs          (csp/chan 1)
        msgs-handling-thread (csp/thread
                               (loop []
                                 (when-let [msgs (csp/<!! incoming-msgs)]
                                   (when-let [results (-kafka/consume-messages actual-consumer-client msgs)]
                                     (csp/>!! result-msgs results))
                                   (recur)))
                               (csp/close! result-msgs)
                               (log/debug "Handover thread finished."))]
    (reify
      -kafka/IConsumerClient
      (consume-messages [_this msgs]
        (let [;; remember previously blocked messages
              ;; because we paused immediately after blocking
              ;; we should only be having `msgs` being the previously blocked messages
              ;; as `msgs` will be `[]` after pausing.
              msgs' (into @blocked-msgs msgs)]
          (while (not= :done
                       (let [choice (csp/alt!!
                                      [[incoming-msgs msgs']] :accepted
                                      result-msgs             ([result-msgs] [:results result-msgs])
                                      (csp/timeout 10000)     :delayed)]
                         (cond
                           ;; do we have any results to pass back?
                           ;; reading results does not complete the operation though
                           ;; only timing out or accepthing new work does
                           (and (vector? choice)
                                (= :results
                                   (first choice)))
                           (let [[_what result-msgs] choice]
                             ;; add it to whatever is in the outbox
                             (swap! msgs-in-outbox
                                    into
                                    result-msgs))

                           (= :accepted choice)
                           (do (reset! blocked-msgs [])
                               (ensure-unpaused)
                               :done)

                           (= :delayed choice)
                           (do (reset! blocked-msgs msgs')
                               (ensure-paused)
                               :done))))
            nil)
          (let [results @msgs-in-outbox]
            (reset! msgs-in-outbox [])
            results)))

      -comp/IHaltable
      (halt [_]
        (csp/close! incoming-msgs)
        (csp/close! result-msgs) ;; this carries the danger of losing some results
        (csp/<!! msgs-handling-thread))

      -kafka/IConsumerPostInitHook
      (post-init-hook [_this consumer]
        (reset! consumer* consumer))

      -kafka/IConsumerAwareRebalanceListener
      (on-partitions-revoked [_ _consumer partitions]
        (swap! assigned-partitions
               set/difference
               (set partitions)))

      (on-partitions-lost [_ _consumer partitions]
        (swap! assigned-partitions
               set/difference
               (set partitions)))

      (on-partitions-assigned [_ _consumer partitions]
        (swap! assigned-partitions
               set/union
               (set partitions))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::actual-consumer-client :consumer/client)
(s/def ::backpressure-aware-consumer-wrapper-cfg
  (s/keys :req-un [::actual-consumer-client]))

(-comp/defcomponent {::-comp/config-spec ::backpressure-aware-consumer-wrapper-cfg
                     ::-comp/ig-kw       ::backpressure-aware-consumer-wrapper}
  [cfg] (make-strategy cfg))
