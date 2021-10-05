(ns afrolabs.components.confluent.schema-registry
  (:require  [afrolabs.components :as -comp]
             [afrolabs.components.kafka :as -kafka]
             [clojure.spec.alpha :as s]
             [reitit.core :as reitit]
             [org.httpkit.client :as http-client]
             [clojure.data.json :as json])
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

              (and api-key
                   api-secret)
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn valid-json? [s]
  (try
    (json/read-str s)
    (catch Throwable _ false)))

(s/def :provided-schema/key (s/and string?
                                   #(pos-int? (count %))
                                   valid-json?))
(s/def :provided-schema/value :provided-schema/key)
(s/def :provided-schema/topic (s/and string?
                                     #(pos-int? (count %))))

(s/def ::provided-topic-schema (s/and (s/keys :opt-un [:provided-schema/key
                                                       :providec-schema/value])
                                      #(or (:provided-schema/key %)
                                           (:provided-schema/value %))))

(s/def ::get-topic-json-schemas (s/map-of :provided-schema/topic
                                          ::provided-topic-schema))

;;;;;;;;;;;;;;;;;;;;

(defprotocol ITopicJSONSchemaProvider
  "A client protocol to provide a map between topic names and json schemas. The confluent schema asserter will consume this protocol against the schema registry.

  The result must conform to ::get-topic-json-schemas

  - Schemas are JSON-valued strings
  - either key, or value (or both) must be specified

  Eg1: {\"topic-with-key-only\"   {:key \"<JSON Schema>\"}}
  Eg2: {\"topic-with-value-only\" {:value \"<JSON Schema>\"}}
  Eg3: {\"topic-with-key-value\"  {:key \"<JSON Schema>\"
                                   :value \"<JSON Schema\"}} "
  (get-topic-json-schemas [_] "Returns a map of topic names to JSON schemas."))

(defprotocol IConfluentSchemaAsserter
  "A protocol for using the schema registry, eg mapping from topic names to schema id's"
  (get-schema-id [_ topic-name] "Returns the most recent known schema id for this topic name."))

;;;;;;;;;;;;;;;;;;;;

(s/def ::topic-json-schema-provider #(satisfies? ITopicJSONSchemaProvider %))
(s/def ::topic-json-schema-providers (s/coll-of ::topic-json-schema-provider))

(s/def ::schema-registry-url (s/and string?
                                    #(pos-int? (count %))))
(s/def ::schema-registry-api-key (s/or :n nil?
                                       :s (s/and string?
                                                 #(pos-int? (count %)))))
(s/def ::schema-registry-api-secret ::schema-registry-api-key)

(s/def ::confluent-schema-asserter-cfg (s/keys :req-un [::topic-json-schema-providers
                                                        ::schema-registry-url
                                                        ::schema-registry-api-secret
                                                        ::schema-registry-api-key]))

;;;;;;;;;;;;;;;;;;;;
;; HTTP/REST utilities for confluent schema registry API

(defn make-default-http-options
  [{:as   cfg
    :keys [schema-registry-api-key
           schema-registry-api-secret]}]
  (cond->
      {:user-agent (format "Afrolabs Confluent Schema Registry API Client")
       :headers    {"Accept"       "application/vnd.schemaregistry.v1+json"
                    "Content-Type" "application/vnd.schemaregistry.v1+json"}
       :as         :text}
    (and schema-registry-api-key
         schema-registry-api-secret)
    (assoc
     :basic-auth  [schema-registry-api-key
                   schema-registry-api-secret])))

(def confluent-api-router
  (reitit/router
   [["/schemas/ids/:id"                              ::schema-by-id]
    ["/schemas/types"                                ::schema-types]
    ["/schemas/ids/:id/versions"                     ::schema-versions]
    ["/subjects"                                     ::subjects]
    ["/subjects/:subject"                            ::subject]
    ["/subjects/:subject/versions"                   ::subject-versions]
    ["/subjects/:subject/versions/:version"          ::subject-version]
    ["/subjects/:subject/versions/:version/schema"   ::subject-version-schema]
    ["/config"                                       ::cluster-config]
    ["/config/:subject"                              ::subject-config]
    ]))

(defn make-url-fn
  "Creates an fn that Adds the (confluent) url to the options map, based on the name of the route. "
  [{:keys [schema-registry-url]}]
  (fn url
    ([route-name]
     (url route-name nil))
    ([route-name params]
     (let [route (reitit/match-by-name confluent-api-router route-name)
           the-url (cond
                     (nil? route)
                     (throw (ex-info (format "Unknown confluent path, name = '%s'" (str route-name))
                                     {:route-name route-name}))

                     (reitit/partial-match? route)
                     (let [path-params  (or (when params (select-keys params (:required route)))
                                            {})
                           query-params (or (when params (select-keys params (for [k (keys params) :when (not (contains? (:required route) k))] k)))
                                            {})
                           route (reitit/match-by-name! confluent-api-router route-name path-params)]
                       (reitit/match->path route query-params))

                     :else
                     (reitit/match->path route params))]
       (str schema-registry-url the-url)))))

(comment

  (reitit/match-by-name confluent-api-router ::schema-by-id)

  (let [make-url (make-url-fn {:schema-registry-url "https://confluent.schema.registry.com"})]
    (make-url ::schema-by-id {:id 1}))

  (let [make-url (make-url-fn {:schema-registry-url "https://confluent.schema.registry.com"})]
    (make-url ::schema-types {}))

  (let [make-url (make-url-fn {:schema-registry-url "https://confluent.schema.registry.com"})]
    (make-url ::schema-versions {:id 1}))

  (let [make-url (make-url-fn {:schema-registry-url "https://confluent.schema.registry.com"})]
    [(make-url ::subjects {:deleted true})
     (make-url ::subjects {})
     (make-url ::subjects)])

  )

(defn cleanup-api-result [result]
  (cond-> result
    (get-in result [:opts :body]) (update-in [:opts :body] json/read-str)
    true (update :opts dissoc :basic-auth)))

(defn api-result
  [http-result]
  (let [result @http-result
        expected-types #{"application/vnd.schemaregistry.v1+json"
                         "application/vnd.schemaregistry+json"
                         "application/json"}
        actual-content-type (get-in result [:headers :content-type])]

    (when (not (expected-types actual-content-type))
      (throw (ex-info "Unsupported Content-Type from confluent API."
                      {:expected-types expected-types
                       :actual         actual-content-type})))

    (let [result (update result :body json/read-str)]
      (when (get-in result [:body "error_code"])
        (throw (ex-info (format "Schema Registry Error: Code='%d', Message='%s'"
                                (get-in result [:body "error_code"])
                                (get-in result [:body "message"]))
                        {:body (:body result)
                         :request (-> result
                                      (dissoc :body)
                                      cleanup-api-result)})))


      (cleanup-api-result result))))

;;;;;;;;;;;;;;;;;;;;

(defn assert-server-supports-schema-type
  [make-url options schema-type]

  (let [{:keys [body]}
        (api-result (http-client/get (make-url ::schema-types)
                                     options))]
    (when-not (contains? (set body)
                         schema-type)
      (throw (ex-info (format "Schema Registry not configured to use '%s'" schema-type)
                      {:supported-schema-types body})))))

(defn get-subjects-in-schema-registry
  [make-url options]
  (-> (make-url ::subjects)
      (http-client/get options)
      api-result
      :body
      set))

(defn upload-new-subject-schema
  [make-url options subject schema]
  (api-result (http-client/post (make-url ::subject-versions {:subject subject})
                                (assoc options
                                       :body (json/write-str {:schema schema
                                                              :schemaType "JSON"})))))

(defn get-subject-compatibility
  [make-url options subject]
  #_(api-result (http-client/get )))

;;;;;;;;;;;;;;;;;;;;

(defn make-component
  [{:as   cfg
    :keys [schema-registry-url
           topic-json-schema-providers]}]
  (s/assert ::confluent-schema-asserter-cfg cfg)
  (let [make-url             (make-url-fn cfg)
        def-opts             (make-default-http-options cfg)]

    ;; verify that the schema-registry supports jsonschema
    (assert-server-supports-schema-type make-url
                                        def-opts
                                        "JSON")

    ;; get schemas; test if they were passed correctly
    ;; throw if not
    (let [all-provided-schemas (map #(get-topic-json-schemas %)
                                    topic-json-schema-providers)
          invalid-schemas (into []
                                (filter #(s/valid? ::get-topic-json-schemas %))
                                all-provided-schemas)]

      ;; throw an error to the developer if ITopicJSONSchemaProvider was implemented incorrectly
      (when (seq invalid-schemas)
        (throw (ex-info "JSON Schemas were provided in the wrong format."
                        {:errors (into []
                                       (map #(hash-map :str (s/explain-str ::get-topic-json-schemas %)
                                                       :data (s/explain-data ::get-topic-json-schemas %)))
                                       invalid-schemas)})))

      (let [subjects-in-sr  nil

            provided-subject-schemas (into {}
                                           (mapcat (fn [[topic {:keys [key value]}]]
                                                     (concat (when key   [(str topic "-key")   key])
                                                             (when value [(str topic "-value") value]))))
                                           all-provided-schemas)

            ;; sort by those subjects that exist and have to be tested for compat
            ;; and those subjects which does not exist yet and have to be uploaded
            {must-be-checked-for-compat true
             must-be-uploaded           false} (group-by subjects-in-sr (keys provided-subject-schemas))


            topic-with-schemas ()])

      ;; now check if the schemas are compatible with the subjects if they exist
      ;; upload if they do not
      (doseq [provided-schemas all-provided-schemas]



        ))






    
    ;; fail if schemas are incompatible (?)
    (reify
      IConfluentSchemaAsserter
      (get-schema-id [_ topic-name]

        0
        ))))

(comment

  (require '[afrolabs.config :as -config])

  (def cfg
    (let [cfg-source (-config/read-parameter-sources ".env")]
      {:schema-registry-url         (:confluent-schema-registry-url cfg-source)
       :schema-registry-api-key     (:confluent-schema-registry-api-key cfg-source)
       :schema-registry-api-secret  (:confluent-schema-registry-api-secret cfg-source)
       :topic-json-schema-providers []}))

  (def make-url (make-url-fn cfg))
  (def def-opts (make-default-http-options cfg))

  (def schema-asserter
    (let [cfg-source (-config/read-parameter-sources ".env")]
      (make-component {:schema-registry-url         (:confluent-schema-registry-url cfg-source)
                       :schema-registry-api-key     (:confluent-schema-registry-api-key cfg-source)
                       :schema-registry-api-secret  (:confluent-schema-registry-api-secret cfg-source)
                       :topic-json-schema-providers []})))

  (-comp/halt schema-asserter)

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  (require '[malli.core :as malli])
  (require '[malli.json-schema :as malli-json])

  (def simple-schema [:map
                      [:b boolean?]
                      [:s string?]])
  (def simple-schema-2 [:map
                        [:b boolean?]
                        [:s string?]
                        [:i {:optional true} int?]])
  (def simple-schema-4 [:map {:closed true}
                        [:s string?]
                        [:ss {:optional true} string?]])

  (def simple-schema-json (malli-json/transform simple-schema))
  (def simple-schema-json-2 (malli-json/transform simple-schema-2))
  (def simple-schema-json-4 (malli-json/transform simple-schema-4))


  )

;;;;;;;;;;;;;;;;;;;;

(-comp/defcomponent {::-comp/ig-kw       ::confluent-schema-asserter
                     ::-comp/config-spec ::confluent-schema-asserter-cfg}
  [cfg] (make-component cfg))
