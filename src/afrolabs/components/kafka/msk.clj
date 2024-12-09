(ns afrolabs.components.kafka.msk
  (:require
   [afrolabs.components.kafka :as -kafka]))


(-kafka/defstrategy AwsMsk
  ;; "Sets the required and recommended config to connect to a kafka cluster in confluent cloud.
  ;; Based on: https://docs.confluent.io/cloud/current/client-apps/config-client.html#java-client"
  [& {:aws.msk/keys [username password]}]

  (letfn [(merge-common
            [cfg]
            (cond-> cfg
              true
              (merge {"client.dns.lookup"        "use_all_dns_ips"
                      "reconnect.backoff.max.ms" "10000"
                      "request.timeout.ms"       "30000"})

              (and username (seq username)
                   password (seq password))
              (merge {;; "sasl.mechanism"           "PLAIN"
                      "sasl.jaas.config"         (format (str "org.apache.kafka.common.security.scram.ScramLoginModule required "
                                                              "username=\"%s\" "
                                                              "password=\"%s\" "
                                                              ";")
                                                         username password)
                      "security.protocol"        "SASL_SSL"
                      "sasl.mechanism"           "SCRAM-SHA-512"
                      })))]

    (reify
      -kafka/IUpdateConsumerConfigHook
      (update-consumer-cfg-hook
          [_ cfg]
        (-> cfg
            (merge-common)
            (merge {"session.timeout.ms" "45000"})))

      -kafka/IUpdateProducerConfigHook
      (update-producer-cfg-hook
          [_ cfg]
        (-> cfg
            (merge-common)
            (merge {"acks"      "all"
                    "linger.ms" "5"})))

      -kafka/IUpdateAdminClientConfigHook
      (update-admin-client-cfg-hook
          [_ cfg]
        (-> cfg
            (merge-common)
            (merge {"default.api.timeout.ms" "300000"}))))))

(-kafka/defstrategy AwsMskIam
  ;; "Sets the config to connect to a kafka cluster hosted in AWS MSK, using IAM authn/authz.
  [& {:as _cfg}]

  (letfn [(merge-common
            [cfg]
            (cond-> cfg
              true
              (merge {"sasl.jaas.config"                   "software.amazon.msk.auth.iam.IAMLoginModule required awsMaxRetries=\"7\" awsMaxBackOffTimeMs=\"500\";"
                      "sasl.client.callback.handler.class" "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
                      "security.protocol"                  "SASL_SSL"
                      "sasl.mechanism"                     "AWS_MSK_IAM"
                      })))]

    (reify
      -kafka/IUpdateConsumerConfigHook
      (update-consumer-cfg-hook
          [_ cfg]
        (-> cfg
            (merge-common)
            (merge {"session.timeout.ms" "45000"})))

      -kafka/IUpdateProducerConfigHook
      (update-producer-cfg-hook
          [_ cfg]
        (-> cfg
            (merge-common)
            (merge {"acks"      "all"
                    "linger.ms" "5"})))

      -kafka/IUpdateAdminClientConfigHook
      (update-admin-client-cfg-hook
          [_ cfg]
        (-> cfg
            (merge-common)
            (merge {"default.api.timeout.ms" "300000"}))))))
