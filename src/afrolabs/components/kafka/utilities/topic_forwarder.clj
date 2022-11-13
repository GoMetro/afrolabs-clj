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
(s/def :topic-forwarder-cfg/topics (s/or :string-col     ::-kafka/topics
                                         :topic-provider ::-kafka/topic-name-provider))

(s/def :topic-forwarder-cfg/src (s/and ::topic-forwarder-cluster-cfg
                                       (s/keys :req-un [:topic-forwarder-cfg/topics]
                                               :opt-un [:topic-forwarder-cfg/msg-filter])))
(s/def :topic-forwarder-cfg/dest (s/and ::topic-forwarder-cluster-cfg
                                        (s/keys :opt-un [:topic-forwarder-cfg/msg-transform])))

(s/def :topic-forwarder-cfg/health-component :afrolabs.components.health/service-health-trip-switch)
(s/def :topic-forwarder-cfg/options (s/keys :opt-un [:topic-forwarder-cfg/health-component]))

(s/def ::topic-forwarder-cfg (s/or :without-opts (s/cat :src-cluster-cfg :topic-forwarder-cfg/src
                                                        :dest-cluster-cfg :topic-forwarder-cfg/dest)
                                   :with-opts (s/cat :src-cluster-cfg :topic-forwarder-cfg/src
                                                     :dest-cluster-cfg :topic-forwarder-cfg/dest
                                                     :options          :topic-forwarder-cfg/options)))

(comment

  (s/conform ::topic-forwarder-cfg
             [{:bootstrap-server "oeu"
               :strategies []
               :topics ["one"]}
              {:bootstrap-server "ueo"
               :strategies []}
              {}])

  (-> (s/conform ::topic-forwarder-cfg
                 [{:bootstrap-server "oeu"
                   :strategies []
                   :topics (-kafka/init-fn-list-of-topics ::list-of-topics {:topics ["one" "two"]})}
                  {:bootstrap-server "ueo"
                   :strategies []}
                  {}])
      unify-options
      unify-src-topics)

  (unify-with-options (s/conform ::topic-forwarder-cfg
                                 [{:bootstrap-server "oeu"
                                   :strategies []
                                   :topics ["one"]}
                                  {:bootstrap-server "ueo"
                                   :strategies []} ]))



  (s/explain-data ::topic-forwarder-cfg
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

(defn unify-options
  "Resolves the variance in cfg that s/or introduces for :options"
  [[option-kw opts]]
  (if (= option-kw :with-opts)
    opts
    (assoc opts :options {})))

(defn unify-src-topics
  "Unifies the variance in src-config that s/or introduces for :topic-forwarder-cfg/topics"
  [{{[option-kw topics] :topics} :src-cluster-cfg
    :as                          config}]
  (assoc-in config [:src-cluster-cfg :topics]
            (case option-kw
              :string-col     [:strategy/SubscribeWithTopicsCollection topics]
              :topic-provider [:strategy/SubscribeWithTopicNameProvider
                               :topic-name-providers [topics]])))

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
              src-topics-strategy  :topics
              src-strategies       :strategies
              src-msg-filter       :msg-filter
              :or                  {src-msg-filter identity
                                    src-strategies []}}            :src-cluster-cfg
             {dest-bootstrap-server :bootstrap-server
              dest-strategies       :strategies
              dest-msg-transform    :msg-transform
              :or                   {dest-strategies    []
                                     dest-msg-transform identity}} :dest-cluster-cfg
             {:keys [health-component]}                            :options}
            (-> conforms?
                unify-options
                unify-src-topics)

            health-component-ref    (or health-component
                                        (ig/ref :afrolabs.components.health/component))]

        (merge (when-not health-component {:afrolabs.components.health/component
                                           {:intercept-signals                   false
                                            :intercept-uncaught-exceptions       false
                                            :trigger-self-destruct-timer-seconds nil}})
               {::-kafka/kafka-producer {:bootstrap-server dest-bootstrap-server
                                         :strategies       (map second dest-strategies)}
                ::-kafka/kafka-consumer {:bootstrap-server src-bootstrap-server
                                         :strategies       (let [r (concat (map second src-strategies)
                                                                           [(ig/ref ::-kafka/kafka-producer)
                                                                            src-topics-strategy ;; unify-src-topics makes a strategy for use here
                                                                            ])]
                                                             r)
                                         :service-health-trip-switch health-component-ref
                                         :consumer/client  (reify
                                                             IConsumerClient
                                                             (consume-messages [_ msgs]
                                                               (into [] (comp
                                                                         (filter (comp src-msg-filter :value))
                                                                         (map #(select-keys % [:topic :key :value]))
                                                                         (map #(update % :value dest-msg-transform)))
                                                                     msgs)))}})))))
