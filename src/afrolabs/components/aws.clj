(ns afrolabs.components.aws
  (:require [afrolabs.components :as -comp]
            [cognitect.aws.credentials :as aws-creds]
            [clojure.spec.alpha :as s]
            [clojure.core.async :as csp]
            [taoensso.timbre :as log]))

(defn set-aws-creds!
  "Sets java system properties for AWS credentials. This works extremely well. Too well. There are better ways to pass credentials to AWS API's"
  [access-key-id secret-key]
  (System/setProperty "aws.accessKeyId" access-key-id)
  (System/setProperty "aws.secretKey" secret-key)
  nil)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn rate-limit-wait
  "Returns a channel that will time out after a number of milliseconds (X) have passed.

  X is calculated to be the interval length to allow num-per-second operations per 1000 milliseconsd."
  [num-per-second]
  (csp/timeout (int (/ 1000 num-per-second))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::access-key-id string?)
(s/def ::secret-access-key string?)
(s/def ::basic-aws-creds-cfg (s/keys :req-un [::access-key-id
                                              ::secret-access-key]))

;; convenience for outside components that require a credentials provider
(s/def ::aws-creds-component #(satisfies? aws-creds/CredentialsProvider %))

(-comp/defcomponent {::-comp/config-spec ::basic-aws-creds-cfg
                     ::-comp/ig-kw       ::basic-aws-creds}
  [cfg]
  (aws-creds/basic-credentials-provider cfg))

;;;;;;;;;;;;;;;;;;;;

(s/def ::region string?)
(s/def ::aws-region-cfg (s/keys :req-un [::region]))

(-comp/defcomponent {::-comp/config-spec ::aws-region-cfg
                     ::-comp/ig-kw       ::aws-region-component}
  [cfg] cfg)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn throw-when-anomaly
  [aws-response]

  (when-let [err-category (:cognitect.anomalies/category aws-response)]
    (throw (ex-info (str "AWS operation returned an error: " err-category)
                    aws-response)))
  aws-response)

(defmacro backoff-and-retry-on-rate-exceeded
  ([request] `(backoff-and-retry-on-rate-exceeded {} ~request))
  ([{:keys [max-retries]
     :or   {max-retries 10}}
    request]
   `(loop [retries# 0
           result# ~request]

      (let [anomaly?#   (:cognitect.anomalies/category result#)
            throttled?# (and anomaly?#
                             (or (-> result#
                                     :ErrorResponse
                                     :Error
                                     :Code
                                     (= "Throttling"))
                                 (= anomaly?# :cognitect.anomalies/busy)))]

        (cond
          (not anomaly?#)
          result#

          (and anomaly?#
               (not throttled?#))
          (do
            (log/debugf "There was a problem ('%s') but it's not something we can do something about."
                        anomaly?#)
            result#)

          (and throttled?#
               (> retries# ~max-retries))
          (do
            (log/warnf "API was throttled, but retried (%d) more than the limit (%d)" retries# ~max-retries)
            result#)

          :else
          (let [sleepies# (long (+ (rand-int 100)          ;; jitter
                                   (* (+ 50 (rand-int 50)) ;; even more jitter
                                      (Math/pow 2 retries#))))]
            (log/warnf "Encountering throttling on AWS API call. Sleeping for %d ms, with retry %d"
                       sleepies# retries#)
            (Thread/sleep sleepies#)
            (recur (inc retries#)
                   ~request)))))))

(comment

  (backoff-and-retry-on-rate-exceeded {:max-retries 5}
                                      (+ 1 2))

  (backoff-and-retry-on-rate-exceeded (+ 1 2))

  )

