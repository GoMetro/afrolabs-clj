(ns afrolabs.prometheus
  (:require [iapetos.core :as p]
            [iapetos.registry :as promr]
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


(defonce registry
  (atom (-> (p/collector-registry)
            (iapetos.collector.jvm/initialize))))

;;;;;;;;;;;;;;;;;;;;

(defmacro register-metric
  "A helper macro to define and register metrics in the top-level of the namespace.

  - Uses a defonce so that each metric is registered only once per JVM instance.
  - Creates an accessor fn called \"get-\"<metric-type>\"-\"<metric-name>, which can be used with prometheus api fns.
  - If you change the metric definition, you'll have to restart the JVM to see the effect.

  (eg)
  (register-metric (prometheus/counter ::test))
  ...
  (prometheus/inc (get-counter-test) 1)

  OR

  (register-metric (prometheus/counter ::test2 {:description \"d\" :labels [:label-name]}))
  ...
  (prometheus/inc (get-counter-test2 {:label-name \"label-value\"}))"
  [iapetos-metric-definition]
  (let [metric-name (second iapetos-metric-definition)
        metric-type (-> iapetos-metric-definition first name)
        metric-fn-name (name metric-name)]
    `(defonce ~(symbol (str "get-" metric-type "-" metric-fn-name))
       (do
         (swap! registry
                (fn [old-registry#]
                  (p/register old-registry#
                              ~iapetos-metric-definition)))
         (fn [& [labels# & _#]]
           (if labels#
             (@registry ~metric-name labels#)
             (@registry ~metric-name)))))))

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
        (ring-response/response
         (ring-io/piped-input-stream
          (fn [output-stream]
            (with-open [^java.io.BufferedWriter w (io/make-writer output-stream {})]
              (iapetos.export/write-text-format! w @registry)
              (.flush w)))))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn sample-data
  [^io.prometheus.client.Collector$MetricFamilySamples$Sample s]
  {:labels (->> (map vector
                     (.labelNames s)
                     (.labelValues s))
                (into {}))
   :name (.name s)
   :value (.value s)
   :timestamp-ms (.timestampMs s)})

(defn metric-family-samples->data
  [^io.prometheus.client.Collector$MetricFamilySamples x]
  {:name (.name x)
   :unit (.unit x)
   :samples (into []
                  (map sample-data)
                  (.samples x))
   :help (.help x)
   :type (condp = (.type x)
           io.prometheus.client.Collector$Type/COUNTER :counter
           io.prometheus.client.Collector$Type/GAUGE :gauge
           io.prometheus.client.Collector$Type/GAUGE_HISTOGRAM :gauge-histogram
           io.prometheus.client.Collector$Type/HISTOGRAM :histogram
           io.prometheus.client.Collector$Type/INFO :info
           io.prometheus.client.Collector$Type/STATE_SET :state-set
           io.prometheus.client.Collector$Type/SUMMARY :summary
           io.prometheus.client.Collector$Type/UNKNOWN :unknown)
   })

(defn extract-samples
  "Extracts sample values for a subset of the metrics from the registry."
  ([] (extract-samples #".*"))
  ([metric-re] (extract-samples @registry
                                metric-re))
  ([registry metric-re]
   (let [^io.prometheus.client.CollectorRegistry collector-registry (promr/raw registry)
         sample-data (enumeration-seq (.metricFamilySamples collector-registry))]
     (sequence (comp (filter #(re-matches metric-re (.name ^io.prometheus.client.Collector$MetricFamilySamples$Sample %)))
                     (map metric-family-samples->data))
               sample-data))))
