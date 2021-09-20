(ns afrolabs.components.confluent.ksqldb
  (:require [afrolabs.components :as -comp]
            [clojure.spec.alpha :as s]
            [reitit.core :as r]
            [reitit.ring :as rring]
            [clojure.string :as str]
            [ring.util.http-response :as http-response]
            [org.httpkit.server :as httpkit]
            [taoensso.timbre :as log]
            [clojure.core.async :as csp]
            [clojure.data.json :as json])
  (:import [afrolabs.components IHaltable]
           [afrolabs.components.http IHttpRequestHandler]
           [io.confluent.ksql.api.client Client ClientOptions]
           [java.time Duration]))


;; this code has been copied and pasted into here for safekeeping.
;; TODO - take this code and create a ksqldb component out of it

(s/def ::ksqldb-host (s/and string?
                            #(pos-int? (count %))))
(s/def ::ksqldb-port (s/or :i pos-int?
                           :s #(try (let [i (Integer/parseInt %)]
                                      (pos-int? i))
                                    (catch Throwable _ false))))
(s/def ::ksqldb-api-key    (s/or :n nil?
                                 :s ::ksqldb-host)) ;; cheating
(s/def ::ksqldb-api-secret (s/or :n nil?
                                 :s ::ksqldb-host)) ;; more cheating

(s/def ::websocket-cfg (s/keys :req-un [::ksqldb-host
                                        ::ksqldb-port
                                        ::ksqldb-api-key
                                        ::ksqldb-api-secret]))

(defn create-ksqldb-client
  [{:keys [ksqldb-host
           ksqldb-port
           ksqldb-api-key
           ksqldb-api-secret]
    :as cfg}]

  (s/assert ::websocket-cfg cfg)

  ;; From the docs https://docs.ksqldb.io/en/0.20.0-ksqldb/developer-guide/ksqldb-clients/java-client/
  (let [ksqldb-port (let [[t v] (s/conform ::ksqldb-port ksqldb-port)]
                      (condp = t
                        :i v
                        :s (Integer/parseInt v)))
        client-options (-> (ClientOptions/create)
                           (.setHost ksqldb-host)
                           (.setPort ksqldb-port))

        _ (when (= 443 ksqldb-port)
            (.setUseTls client-options true)
            (.setUseAlpn client-options true))

        _ (when (and ksqldb-api-key ksqldb-api-secret)
            (.setBasicAuthCredentials client-options ksqldb-api-key ksqldb-api-secret))]

    (Client/create client-options)))
