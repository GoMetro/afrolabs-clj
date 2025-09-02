(ns afrolabs.components.confluent
  (:require [afrolabs.components.kafka :as -kafka]
            [taoensso.timbre :as log])
  (:import [afrolabs.components.kafka
            IUpdateConsumerConfigHook
            IUpdateProducerConfigHook
            IUpdateAdminClientConfigHook]
           [org.apache.kafka.clients.producer ProducerConfig]
           [org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer]))


(-kafka/defstrategy ConfluentCloud
  ;; "Sets the required and recommended config to connect to a kafka cluster in confluent cloud.
  ;; Based on: https://docs.confluent.io/cloud/current/client-apps/config-client.html#java-client"
  [& {:keys [api-key api-secret]}]

  (log/debug (str "Connection to kafka configured with ConfluentCloud strategy..."
                  (when-not (and api-key api-secret)
                    " (don't have api-key & api-secret.)")))

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

(-kafka/defstrategy ConfluentJSONSchema
  [& {:keys [schema-registry-url
             sr-api-key
             sr-api-secret]
      producer-option :producer
      consumer-option :consumer
      :or {producer-option :value
           consumer-option :value}}]

  (when-not schema-registry-url
    (throw (ex-info "ConfluentJsonSchema strategy requires the schema-registry-url to be set." {})))

  (let [allowed-values #{:key :value :both}]
    (when-not (or (allowed-values producer-option)
                  (allowed-values consumer-option))
      (throw (ex-info "ConfluentJSONSchema expects one of #{:key :value :both} for each of :producer or :consumer, eg (ConfluentJSONSchema :schema-registry-url \"...\" :producer :both :consumer :key)"
                      {::allowed-values  allowed-values
                       ::consumer-option consumer-option
                       ::producer-option producer-option}))))

  (letfn [(update-with-credentials
            [cfg]
            (if-not (and sr-api-key sr-api-secret)
              (do
                (log/debug "Setting up schema registry config without api-key and api-secret.")
                cfg)
              (assoc cfg
                     "basic.auth.credentials.source"        "USER_INFO"
                     "schema.registry.basic.auth.user.info" (str sr-api-key ":" sr-api-secret))))]
    (reify
      IUpdateProducerConfigHook
      (update-producer-cfg-hook
          [_ cfg]
        (cond-> (assoc cfg "schema.registry.url" schema-registry-url)
          true                               update-with-credentials
          (#{:both :key}   producer-option)  (assoc ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG   "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer")
          (#{:both :value} producer-option)  (assoc ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer")))

      IUpdateConsumerConfigHook
      (update-consumer-cfg-hook
          [_ cfg]
        (cond-> (assoc cfg "schema.registry.url" schema-registry-url)
          true                               update-with-credentials
          (#{:both :key}   consumer-option)  (assoc ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG  "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer")
          (#{:both :value} consumer-option)  (assoc ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG    "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer"))))))


