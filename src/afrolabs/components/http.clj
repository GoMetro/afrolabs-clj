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

(defn add-request-id
  [handler]
  (fn [req]
    (let [request-id (random-uuid)]
      (log/with-context+ {:request-id request-id}
        (http-response/header (handler req)
                              "X-Request-Id" (str request-id))))))

(defn basic-request-logging
  [handler {:as logging-context
            :keys [port ip]}]
  (fn [{:as req
        :keys [uri
               remote-addr
               request-method]}]
    (log/with-context+ (assoc logging-context
                              :uri            uri
                              :remote-addr    remote-addr
                              :request-method request-method)
      (log/info (str "REQUEST: " ip ":" port uri))
      (let [{:as   res
             :keys [status]} (handler req)]
        ;; status is nil for websocket responses
        (log/with-context+ (cond-> {} status (assoc :status status))
          (log/info (str "RESPONSE: " ip ":" port uri
                         (when status
                           (str " [" status "]")))))
        res))))

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
        middleware-chain (or (when (seq middleware-providers)
                               (->> middleware-providers
                                    (mapcat get-middleware)
                                    (reverse)
                                    (apply comp)))
                             identity)
        handler' (-> handler
                     (middleware-chain)
                     (basic-request-logging {:port port :ip ip})
                     (add-request-id)
                     (ring.middleware.pratchett/wrap-pratchett))
        s (httpkit/run-server handler'
                              (cond-> {:error-logger         (fn [txt ex]
                                                               (if-not ex
                                                                 (log/error txt)
                                                                 (log/error ex txt)))
                                       :warn-logger          (fn [txt ex]
                                                               (if-not ex
                                                                 (log/warn txt)
                                                                 (log/warn ex txt)))
                                        ;; replaced with (basic-request-logging)
                                       :event-logger         (fn [event-name]
                                                               (log/debug (str "low-level http-kit event logger: "
                                                                               event-name)))
                                       :legacy-return-value? false
                                       :port                 port
                                       :ip                   ip}
                                worker-thread-name-prefix (assoc :worker-name-prefix   worker-thread-name-prefix)))]

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

;; The resource file identified with `version-info-resource` has to have this format:
;; {:git-ref  "refs/heads/pwab/BSA-103-add-git-commit-branch-info-into-deployed-artifact-so-you-can-see-what-version-is-running"
;;  :git-sha  "f7ba5fa0d8bfd1c8a078cae583f8b4cda39b59c3"}


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
          (do (log/warn (str "Failed to load version info from resource '"
                             version-info-resource
                             "'. Unable to create version middleware."))
              [])
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
