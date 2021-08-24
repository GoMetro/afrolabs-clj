(ns afrolabs.components.aws
  (:require [afrolabs.components :as -comp]
            [cognitect.aws.credentials :as aws-creds]
            [clojure.spec.alpha :as s])
  )

(defn set-aws-creds!
  "Sets java system properties for AWS credentials. This works extremely well. Too well. There are better ways to pass credentials to AWS API's"
  [access-key-id secret-key]
  (System/setProperty "aws.accessKeyId" access-key-id)
  (System/setProperty "aws.secretKey" secret-key)
  nil)

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
