(ns afrolabs.components.confluent
  (:require [afrolabs.components.kafka :as -kafka])
  (:import [afrolabs.components.kafka
            IUpdateConsumerConfigHook
            IUpdateProducerConfigHook
            IUpdateAdminClientConfigHook]))


(-kafka/defstrategy ConfluentCloud
  ;; "Sets the required and recommended config to connect to a kafka cluster in confluent cloud.
  ;; Based on: https://docs.confluent.io/cloud/current/client-apps/config-client.html#java-client"
  [& {:keys [api-key api-secret]}]

  (letfn [(merge-common
            [cfg]
            (cond-> cfg
              true
              (merge {"client.dns.lookup"        "use_all_dns_ips"
                      "reconnect.backoff.max.ms" "10000"
                      "request.timeout.ms"       "30000"})

              (and api-key    (seq api-key)
                   api-secret (seq api-secret))
              (merge {"sasl.mechanism"           "PLAIN"
                      "sasl.jaas.config"         (format (str "org.apache.kafka.common.security.plain.PlainLoginModule required "
                                                              "username=\"%s\" "
                                                              "password=\"%s\" "
                                                              ";")
                                                         api-key api-secret)
                      "security.protocol"        "SASL_SSL"})))]

    (reify
      IUpdateConsumerConfigHook
      (update-consumer-cfg-hook
          [_ cfg]
        (-> cfg
            (merge-common)
            (merge {"session.timeout.ms" "45000"})))

      IUpdateProducerConfigHook
      (update-producer-cfg-hook
          [_ cfg]
        (-> cfg
            (merge-common)
            (merge {"acks"      "all"
                    "linger.ms" "5"})))

      IUpdateAdminClientConfigHook
      (update-admin-client-cfg-hook
          [_ cfg]
        (-> cfg
            (merge-common)
            (merge {"default.api.timeout.ms" "300000"}))))))
