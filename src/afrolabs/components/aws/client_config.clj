(ns afrolabs.components.aws.client-config
  (:require
   [afrolabs.components :as -comp]
   [afrolabs.components.aws.sso :as -aws-sso-profile-provider]
   [clojure.spec.alpha :as s]
   [cognitect.aws.client.api :as aws]
   [cognitect.aws.credentials :as aws-creds]
   [taoensso.timbre :as log]
   ))

(s/def ::profile string?)
(s/def ::access-key-id (s/nilable string?))
(s/def ::secret-access-key (s/nilable string?))
(s/def ::region (s/nilable string?))
(s/def ::load-sso? boolean?)
(s/def ::aws-client-config-cfg
  (s/keys :req-un [::access-key-id
                   ::secret-access-key
                   ::region]
          :opt-un [::profile
                   ::load-sso?]))

(defn basic-session-token-provider
  [access-key-id
   secret-access-key
   session-token]
  (reify
    aws-creds/CredentialsProvider
    (fetch [_]
      {:aws/access-key-id     access-key-id
       :aws/secret-access-key secret-access-key
       :aws/session-token     session-token})))

(defn make-aws-client
  [{:keys [region
           access-key-id
           secret-access-key
           session-token
           profile
           load-sso?]
    :as _cfg}]

  (cond-> {}
    region
    (assoc :region region)

    (and access-key-id
         secret-access-key
         (not session-token))
    (assoc :credentials-provider
           (aws-creds/basic-credentials-provider
            {:access-key-id     access-key-id
             :secret-access-key secret-access-key}))

    (and access-key-id
         secret-access-key
         session-token)
    (assoc :credentials-provider
           (basic-session-token-provider access-key-id
                                         secret-access-key
                                         session-token))

    (not (and access-key-id
              secret-access-key))
    (assoc :credentials-provider
           (aws-creds/chain-credentials-provider
            (vec (remove nil?
                         [(aws-creds/default-credentials-provider (aws/default-http-client))
                          ;; this crazy shit provides a work-around because
                          ;; cognitect's profile credentials provider does not work for sso.
                          ;; We are adding it at the end of the chain.
                          (when load-sso?
                            (-aws-sso-profile-provider/provider (or profile
                                                                    (System/getenv "AWS_PROFILE")
                                                                    (System/getProperty "aws.profile")
                                                                    "default")))]))))))

(-comp/defcomponent {::-comp/ig-kw       ::aws-client-config
                     ::-comp/config-spec ::aws-client-config-cfg}
  [cfg] (#'make-aws-client cfg))
