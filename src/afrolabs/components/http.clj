(ns afrolabs.components.http
  (:require [afrolabs.components :as -comp]
            [org.httpkit.server :as httpkit]
            [clojure.spec.alpha :as s]
            [afrolabs.components.health :as -health]
            [clojure.string :as str]
            [taoensso.timbre :as log])
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
              (filter #(<= 0 % 256))
              count)))))

(s/def ::health-component #(satisfies? -health/IServiceHealthTripSwitch %))
;; non-priveleged port number
(s/def ::port (s/and pos-int?
                     #(> % 1024)
                     #(< % java.lang.Short/MAX_VALUE)))
(s/def ::ip valid-ip4?)
(s/def ::health-endpoint-cfg (s/keys :req-un [::health-component]
                                     :opt-un [::port
                                              ::ip]))

(defn create-http-health
  [{:keys [health-component
           port
           ip]
    :or   {port 8000
           ip   "0.0.0.0"}
    :as   cfg}]

  (s/assert ::health-endpoint-cfg cfg)

  (let [handler (fn [r]
                  (if (-health/healthy? health-component)
                    {:body "Service is healthy."
                     :status 200}
                    {:body "Service is NOT healthy :("
                     :status 500}))
        s (httpkit/run-server handler
                              {:worker-name-prefix   "health-http-worker"
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

(-comp/defcomponent {::-comp/ig-kw       ::health-endpoint
                     ::-comp/config-spec ::health-endpoint-cfg}
  [cfg] (create-http-health cfg))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(comment

  (def h? (atom true))
  (reset! h? false)
  (reset! h? true)
  (def s (create-http-health {:health-component (reify
                                                  IServiceHealthTripSwitch
                                                  (indicate-unhealthy! [_ _])
                                                  (wait-while-healthy [_])
                                                  (healthy? [_] @h?))
                              :port 8002}))

  (-comp/halt s)



  )


