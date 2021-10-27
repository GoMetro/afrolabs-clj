(ns afrolabs.components.kafka.utilities.topic-forwarder
  (:require [afrolabs.components :as -comp]
            [clojure.spec.alpha :as s]
            [afrolabs.components.kafka :as -kafka]
            [integrant.core :as ig])
  (:import [afrolabs.components.kafka IConsumerClient]))



(s/def ::topic-forwarder-cluster-cfg (s/keys :req-un [::-kafka/bootstrap-server
                                                      ::-kafka/strategies]))

(s/def :topic-forwarder-cfg/msg-filter fn?)
(s/def :topic-forwarder-cfg/msg-transform fn?)

(s/def :topic-forwarder-cfg/src (s/and ::topic-forwarder-cluster-cfg
                                       (s/keys :req-un [::-kafka/topics]
                                               :opt-un [:topic-forwarder-cfg/msg-filter])))
(s/def :topic-forwarder-cfg/dest (s/and ::topic-forwarder-cluster-cfg
                                        (s/keys :opt-un [:topic-forwarder-cfg/msg-transform])))

(s/def ::topic-forwarder-cfg (s/cat :src-cluster-cfg :topic-forwarder-cfg/src
                                    :dest-cluster-cfg :topic-forwarder-cfg/dest))

(comment

  (s/conform ::topic-forwarder-cfg
             [{:bootstrap-server "oeu"
               :strategies []
               :topics ["one"]}
              {:bootstrap-server "ueo"
               :strategies []}])


  {:src-cluster-cfg  {:bootstrap-server "oeu"
                      :strategies       []
                      :topics           ["one"]}
   :dest-cluster-cfg {:bootstrap-server "ueo"
                      :strategies       []}}

  (println (s/explain-str ::topic-forwarder-cfg
                          [{:bootstrap-server "oeu"
                            :strategies []
                            :topics ["one"]}
                           {:bootstrap-server "ueo"
                            :strategies []}]
                          ))

  (create-system-config {:bootstrap-server "oeu"
                         :strategies []
                         :topics ["one"]}
                        {:bootstrap-server "ueo"
                         :strategies []})

  )

(defn create-system-config
  [& cfg]
  (let [conforms? (s/conform ::topic-forwarder-cfg
                             cfg)]
    (if (= ::s/invalid conforms?)
      (throw (ex-info "create-system-config received bad config."
                      {::explain-str  (s/explain-str ::topic-forwarder-cfg
                                                    cfg)
                       ::explain-data (s/explain-data ::topic-forwarder-cfg
                                                      cfg)}))
      (let [{{src-bootstrap-server :bootstrap-server
              src-topics           :topics
              src-strategies       :strategies
              src-msg-filter       :msg-filter
              :or                  {src-msg-filter identity
                                    src-strategies []}}            :src-cluster-cfg
             {dest-bootstrap-server :bootstrap-server
              dest-strategies       :strategies
              dest-msg-transform    :msg-transform
              :or                   {dest-strategies    []
                                     dest-msg-transform identity}} :dest-cluster-cfg} conforms?]

        {:afrolabs.components.health/component
         {:intercept-signals                   false
          :intercept-uncaught-exceptions       false
          :trigger-self-destruct-timer-seconds nil}

         ::-kafka/kafka-producer {:bootstrap-server dest-bootstrap-server
                                  :strategies       (map second dest-strategies)}


         ::-kafka/kafka-consumer {:bootstrap-server src-bootstrap-server
                                  :strategies       (concat (map second src-strategies)
                                                            [(ig/ref ::-kafka/kafka-producer)
                                                             [:strategy/SubscribeWithTopicsCollection src-topics]
                                                             [:strategy/OffsetReset "earliest"]])
                                  :service-health-trip-switch (ig/ref :afrolabs.components.health/component)
                                  :consumer/client  (reify
                                                      IConsumerClient
                                                      (consume-messages [_ msgs]
                                                        (sequence (comp
                                                                   (filter (comp src-msg-filter :value))
                                                                   (map #(select-keys % [:topic :key :value]))
                                                                   (map #(update % :value dest-msg-transform)))
                                                                  msgs)))}}))))
