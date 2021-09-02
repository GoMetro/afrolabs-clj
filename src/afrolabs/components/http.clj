(ns afrolabs.components.http
  (:require [afrolabs.components :as -comp]
            [org.httpkit.server :as httpkit]
            [clojure.spec.alpha :as s]
            [afrolabs.components.health :as -health]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [afrolabs.components.health :as -health]
            [ring.util.http-response :as http-response]
            )
  (:import [afrolabs.components IHaltable]
           [afrolabs.components.health IServiceHealthTripSwitch]))

(defn valid-ip4?
  [ip-str]
  (let [ip-parts (str/split ip-str #"\.")]
    (when (= 4 (count ip-parts))
      (= 4
         (->> ip-parts
              (map #(try (Integer/parseInt %)
                         (catch Throwable _ -1)))
              (filter #(<= 0 % 255))
              count)))))

(s/def ::health-component #(satisfies? -health/IServiceHealthTripSwitch %))
;; non-priveleged port number
(s/def ::port (s/and pos-int?
                     #(> % 1024)
                     #(< % java.lang.Short/MAX_VALUE)))
(s/def ::ip valid-ip4?)

(defprotocol IHttpRequestHandler
  "Allows components to provide ring-like HTTP handler to an HTTP component."
  (handle-http-request [_ req] "httpkit-compatible http handler."))
(s/def ::http-request-handler #(satisfies? IHttpRequestHandler %))
(s/def ::worker-thread-name-prefix (s/and string?
                                          #(pos-int? (count %))))

(s/def ::handlers (s/coll-of ::http-request-handler))
(s/def ::http-component-cfg (s/keys :req-un [::handlers]
                                    :opt-un [::port
                                             ::ip
                                             ::worker-thread-name-prefix]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn create-http-component
  [{:keys [port
           ip
           handlers
           worker-thread-name-prefix]
    :or   {port 8000
           ip   "0.0.0.0"}
    :as   cfg}]

  (s/assert ::http-component-cfg cfg)

  (let [handler (apply some-fn
                       (map #(partial handle-http-request
                                      %)
                            handlers))
        s (httpkit/run-server handler
                              {:worker-name-prefix   worker-thread-name-prefix
                               :error-logger         (fn [txt ex]
                                                       (if-not ex
                                                         (log/error txt)
                                                         (log/error ex txt)))
                               :warn-logger          (fn [txt ex]
                                                       (if-not ex
                                                         (log/warn txt)
                                                         (log/warn ex txt)))
                               :event-logger         (fn [event-name] (log/info event-name))
                               :legacy-return-value? false
                               :port                 port
                               :ip                   ip})]

    (reify
      IHaltable
      (halt [_] (httpkit/server-stop! s)))))

(-comp/defcomponent {::-comp/config-spec ::http-component-cfg
                     ::-comp/ig-kw       ::service}
  [cfg] (create-http-component cfg))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::endpoint (s/and string?
                         #(pos-int? (count %))
                         #(str/starts-with? % "/")))
(s/def ::health-endpoint-cfg (s/keys :req-un [::health-component
                                              ::endpoint]))

(defn create-http-health-endpoint
  [{:keys [health-component
           endpoint]
    :as   cfg}]

  (s/assert ::health-endpoint-cfg cfg)

  (reify
    IHttpRequestHandler
    (handle-http-request
        [_ {:keys [uri request-method]}]
      (when (and (= uri endpoint)
                 (= :get request-method))
        (if (-health/healthy? health-component)
          (-> "Service is healthy."
              (http-response/ok)
              (http-response/content-type "text/plain"))
          (-> "Service is NOT healthy :("
              (http-response/internal-server-error)
              (http-response/content-type "text/plain")))))))

(-comp/defcomponent {::-comp/ig-kw       ::health-endpoint
                     ::-comp/config-spec ::health-endpoint-cfg}
  [cfg] (create-http-health-endpoint cfg))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;



