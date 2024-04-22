(ns afrolabs.components.aws
  (:require
   [afrolabs.components :as -comp]
   [afrolabs.components.aws.sso :as -aws-sso-profile-provider]
   [clojure.core.async :as csp]
   [clojure.spec.alpha :as s]
   [cognitect.aws.client.api :as aws]
   [cognitect.aws.credentials :as aws-creds]
   [taoensso.timbre :as log]
   ))

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

(defn make-basic-aws-creds
  [cfg]
  (aws-creds/basic-credentials-provider cfg))

(-comp/defcomponent {::-comp/config-spec ::basic-aws-creds-cfg
                     ::-comp/ig-kw       ::basic-aws-creds}
  [cfg] (make-basic-aws-creds cfg))

;;;;;;;;;;;;;;;;;;;;

(s/def ::region string?)
(s/def ::aws-region-cfg (s/keys :req-un [::region]))

(-comp/defcomponent {::-comp/config-spec ::aws-region-cfg
                     ::-comp/ig-kw       ::aws-region-component}
  [cfg] cfg)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn make-creds-with-region
  "Support for ad-hoc REPL based access to a credential and region component. Eg

  (-sns/make-sns-client (-aws/make-creds-with-region (:sns-aws-access-key-id cfg)
                                                     (:sns-aws-secret-access-key cfg)
                                                     \"eu-west-1\"))"
  [access-key-id
   secret-access-key
   region]
  {:aws-creds-component  (make-basic-aws-creds
                          {:access-key-id     access-key-id
                           :secret-access-key secret-access-key})
   :aws-region-component {:region region}})

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
            (log/warnf "There was a problem ('%s') but it's not something we can retry."
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
            (log/infof "Encountering throttling on AWS API call. Sleeping for %d ms, with retry %d"
                       sleepies# retries#)
            (Thread/sleep sleepies#)
            (recur (inc retries#)
                   ~request)))))))

(comment

  (backoff-and-retry-on-rate-exceeded {:max-retries 5}
                                      (+ 1 2))

  (backoff-and-retry-on-rate-exceeded (+ 1 2))

  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; A new imagining of how aws creds can be configured.
;;
;; Here, a client _config_ is configure (everything you would paas to (aws/client {}))
;; _except_ the :api <api-name> values.
;; The idea is that you have one (hopufully) such component in your integrant config
;; and then re-use this by adding :api <api-name> to the result, and passing that to (aws/client ...)

(s/def ::profile string?)
(s/def ::aws-client-config-cfg
  (s/keys :req-un [::access-key-id
                   ::secret-access-key
                   ::region
                   ::profile]))

(defn make-aws-client
  [{:keys [region
           access-key-id
           secret-access-key
           profile]
    :as _cfg}]

  (cond-> {}
    region
    (assoc :region region)

    (and access-key-id
         secret-access-key)
    (assoc :credentials-provider
           (aws-creds/basic-credentials-provider
            {:access-key-id     access-key-id
             :secret-access-key secret-access-key}))

    (not (and access-key-id
              secret-access-key))
    (assoc :credentials-provider
           (aws-creds/chain-credentials-provider
            [(aws-creds/default-credentials-provider (aws/default-http-client))
             ;; this crazy shit provides a work-around because
             ;; cognitect's profile credentials provider does not work for sso.
             ;; We are adding it at the end of the chain.
             (-aws-sso-profile-provider/provider (or profile
                                                     (System/getenv "AWS_PROFILE")
                                                     (System/getProperty "aws.profile")
                                                     "default"))]))))

(-comp/defcomponent {::-comp/ig-kw       ::aws-client-config
                     ::-comp/config-spec ::aws-client-config-cfg}
  [cfg] (make-aws-client cfg))
