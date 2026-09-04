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
(s/def ::assume-role-arn (s/nilable (s/and string? (comp pos? count))))
(s/def ::assume-role-session-name (s/nilable (s/and string? (comp pos? count))))
;; A resolved aws-client-config map (as returned by `make-aws-client`) whose
;; already-resolved credentials-provider is used as the base for an AssumeRole
;; exchange. Wire it in via #ig/ref to another aws-client-config component so a
;; single base identity (SSO / instance-profile) is resolved once and shared.
(s/def ::credentials-provider some?)
(s/def ::base-client-cfg (s/nilable (s/keys :req-un [::credentials-provider])))
(s/def ::aws-client-config-cfg
  (s/and
   (s/keys :req-un [::region]
           :opt-un [::access-key-id
                    ::secret-access-key
                    ::profile
                    ::load-sso?
                    ::assume-role-arn
                    ::assume-role-session-name
                    ::base-client-cfg])
   ;; If we assume a role, STS requires a session name for it, so demand one.
   (fn [{:keys [assume-role-arn assume-role-session-name]}]
     (or (not assume-role-arn)
         (boolean assume-role-session-name)))))

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

(defn- base-credentials-provider
  "The 'ordinary' credentials provider, before any AssumeRole wrapping: explicit
  basic keys (optionally with a session token), or - when no keys are supplied -
  the default credentials chain (instance-profile / env / etc, optionally + SSO)."
  [{:keys [access-key-id
           secret-access-key
           session-token
           profile
           load-sso?]}]
  (cond
    (and access-key-id
         secret-access-key
         (not session-token))
    (aws-creds/basic-credentials-provider
     {:access-key-id     access-key-id
      :secret-access-key secret-access-key})

    (and access-key-id
         secret-access-key
         session-token)
    (basic-session-token-provider access-key-id
                                  secret-access-key
                                  session-token)

    :else
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
                                                             "default")))])))))

(defn assume-role-credentials-provider
  "Wraps a base credentials-provider in an STS AssumeRole exchange: uses the base
  credentials to call `sts:AssumeRole` for `assume-role-arn`, and hands back the
  temporary role credentials. The result auto-refreshes in a background daemon
  thread before the temporary credentials expire, via cognitect's
  `cached-credentials-with-auto-refresh` reading the `:Expiration` of the STS
  response (see `calculate-ttl`).

  The base is the `:credentials-provider` of a resolved `base-client-cfg` when
  one is supplied (a shared base client-config #ig/ref'd in, so its identity -
  SSO / instance-profile - is resolved once and reused); otherwise it is derived
  from this component's own cfg via `base-credentials-provider`."
  [{:keys [region
           assume-role-arn
           assume-role-session-name
           base-client-cfg]
    :as   cfg}]
  (let [base (or (:credentials-provider base-client-cfg)
                 (base-credentials-provider cfg))
        sts  (aws/client (cond-> {:api :sts}
                           region (assoc :region region)
                           base   (assoc :credentials-provider base)))]
    (aws-creds/cached-credentials-with-auto-refresh
     (reify aws-creds/CredentialsProvider
       (fetch [_]
         (let [{:as resp :keys [Credentials]}
               (aws/invoke sts {:op      :AssumeRole
                                :request {:RoleArn         assume-role-arn
                                          :RoleSessionName assume-role-session-name}})]
           (when (:cognitect.anomalies/category resp)
             (throw (ex-info "Unable to sts:AssumeRole for the aws client-config."
                             {:assume-role-arn assume-role-arn
                              :response        resp})))
           {:aws/access-key-id     (:AccessKeyId Credentials)
            :aws/secret-access-key (:SecretAccessKey Credentials)
            :aws/session-token     (:SessionToken Credentials)
            ::aws-creds/ttl        (aws-creds/calculate-ttl Credentials)}))))))

(defn make-aws-client
  [{:keys [region assume-role-arn] :as cfg}]
  (cond-> {}
    region
    (assoc :region region)

    :always
    (assoc :credentials-provider
           (if assume-role-arn
             (assume-role-credentials-provider cfg)
             (base-credentials-provider cfg)))))

(s/def ::aws-client-config
  (s/and map?
         #(:credentials-provider %)))

(-comp/defcomponent {::-comp/ig-kw              ::aws-client-config
                     ::-comp/config-spec        ::aws-client-config-cfg
                     ::-comp/supports:disabled? true}
  [cfg] (#'make-aws-client cfg))
