(ns afrolabs.prometheus
  (:require [iapetos.core :as p]
            [iapetos.collector.jvm]
            [iapetos.export]
            [afrolabs.components :as -comp]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [afrolabs.components.http :as -http]
            [ring.util.response :as ring-response]
            [ring.util.io :as ring-io]
            [clojure.java.io :as io])
  (:import [afrolabs.components.http IHttpRequestHandler]))


;; I'm done fighting trying to pretend the java SimpleClient is anything but a singleton...
;; embrace and extend...
;;
;; in client code, have code in the top-level of your ns that defonce metrics.
;; Have that defonce register the metric into this registry.
;;
;; if you introduce incompatible changes to the metrics, restart the repl.

(defonce registry
  (-> (p/collector-registry)
      (iapetos.collector.jvm/initialize)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::endpoint (s/and string?
                         #(pos-int? (count %))
                         #(str/starts-with? % "/")))
(s/def ::metrics-endpoint-cfg (s/keys :req-un [::endpoint]))


;; we're cheating by creating a component whose state is actually in a singleton...
(-comp/defcomponent {::-comp/config-spec ::metrics-endpoint-cfg
                     ::-comp/ig-kw       ::metrics-endpoint}
  [{:keys [endpoint]}]
  (reify
    IHttpRequestHandler
    (handle-http-request [_ {:keys [uri
                                    request-method]}]
      (when (and (= uri endpoint)
                 (= :get request-method))

        ;; prometheus metrics can get big
        ;; rather than converting to a string first, and then sending the string into an (http) stream
        ;; give the prometheus write-text-format a writer that is based on the stream that will become the http response body
        ;;
        ;; lifted from: https://nelsonmorris.net/2015/04/22/streaming-responses-using-ring.html
        #_(-> (ring-response/response
               (ring-io/piped-input-stream
                (fn [output-stream]
                  (let [w (io/make-writer output-stream {})]
                    (iapetos.export/write-text-format! w registry)
                    (.flush w)))))
              #_(ring-response/content-type "text/plain")
              )
        (-> (ring-response/response (iapetos.export/text-format registry))
            (ring-response/content-type "text/plain"))
        ))))

