(ns afrolabs.components.http
  (:require [afrolabs.components :as -comp]
            [org.httpkit.server :as httpkit]
            [clojure.spec.alpha :as s]
            [afrolabs.components.health :as -health]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [afrolabs.version :as -version]
            [ring.util.http-response :as http-response]
            [ring.middleware.pratchett]
            [clojure.pprint])
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; We want middleware to be applied in order.
;; To be first in the middleware, means you will receive the request first.
;; To be last in the middleware, means you will receive the request last, and the response first.

(defprotocol IRingMiddlewareProvider
  (get-middleware [_] "Returns a collection of ring-style middleware."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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
(s/def ::middleware-providers (s/coll-of #(satisfies? IRingMiddlewareProvider %)))
(s/def ::handlers (s/coll-of ::http-request-handler))
(s/def ::http-component-cfg (s/keys :req-un [::handlers]
                                    :opt-un [::port
                                             ::ip
                                             ::worker-thread-name-prefix
                                             ::middleware-providers]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn create-http-component
  [{:keys [port
           ip
           handlers
           middleware-providers
           worker-thread-name-prefix]
    :or   {port 8000
           ip   "0.0.0.0"}
    :as   cfg}]

  (s/assert ::http-component-cfg cfg)

  (let [handler (apply some-fn
                       (map #(partial handle-http-request
                                      %)
                            handlers))
        middleware-chain (->> middleware-providers
                              (mapcat get-middleware)
                              (reverse)
                              (apply comp))
        handler' (-> handler
                     (middleware-chain)
                     (ring.middleware.pratchett/wrap-pratchett))
        s (httpkit/run-server handler'
                              {:worker-name-prefix   worker-thread-name-prefix
                               :error-logger         (fn [txt ex]
                                                       (if-not ex
                                                         (log/error txt)
                                                         (log/error ex txt)))
                               :warn-logger          (fn [txt ex]
                                                       (if-not ex
                                                         (log/warn txt)
                                                         (log/warn ex txt)))
                               :event-logger         (fn [event-name] (log/debug event-name))
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
(s/def ::version-info-resource string?)
(s/def ::health-endpoint-cfg (s/keys :req-un [::health-component
                                              ::endpoint]
                                     :opt-un [::version-info-resource]))

(defn create-http-health-endpoint
  [{:keys [health-component
           endpoint
           version-info-resource]
    :as   cfg}]

  (s/assert ::health-endpoint-cfg cfg)

  (let [{:keys [git-ref
                git-sha]} (when version-info-resource
                            (-version/read-version-info version-info-resource))]
    (reify
      IHttpRequestHandler
      (handle-http-request
          [_ {:keys [uri request-method]}]
        (when (and (= uri endpoint)
                   (= :get request-method))
          (if (-health/healthy? health-component)
            (cond-> (-> "Service is healthy."
                        (http-response/ok)
                        (http-response/content-type "text/plain"))
              git-ref (http-response/header "X-Version-GitRef" git-ref)
              git-sha (http-response/header "X-Version-GitSHA" git-sha))
            (-> "Service is NOT healthy :("
                (http-response/internal-server-error)
                (http-response/content-type "text/plain"))))))))

(-comp/defcomponent {::-comp/ig-kw       ::health-endpoint
                     ::-comp/config-spec ::health-endpoint-cfg}
  [cfg] (create-http-health-endpoint cfg))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::version-info-resource string?)

(s/def ::git-version-middleware-cfg
  (s/keys :req-un [::version-info-resource]
          :opt-un []))

(defn create-git-version-middleware
  [{:as   _cfg
    :keys [version-info-resource]}]
  (let [{:keys [git-ref git-sha]} (-version/read-version-info version-info-resource)]
    (reify
      IRingMiddlewareProvider
      (get-middleware [_]
        (if-not (and git-ref git-sha)
          []
          [(fn [handler]
             (fn [request]
               (let [response (handler request)]
                 (-> response
                     (http-response/header "X-Version-GitRef" git-ref)
                     (http-response/header "X-Version-GitSHA" git-sha)))))]))

      -comp/IHaltable
      (halt [_]))))

(-comp/defcomponent {::-comp/config-spec ::git-version-middleware-cfg
                     ::-comp/ig-kw       ::git-version-middleware}
  [cfg] (create-git-version-middleware cfg))
