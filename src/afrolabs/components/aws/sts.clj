(ns afrolabs.components.aws.sts
  (:require [afrolabs.components :as -comp]
            [afrolabs.components.aws :as -aws]
            [afrolabs.components.aws.client-config :as -aws-client-config]
            [cognitect.aws.client.api :as aws]
            [clojure.spec.alpha :as s]
            [taoensso.timbre :as log]
            [cognitect.aws.credentials :as credentials]
            ))

(defn assume-role
  [client role-arn session-name]
  (aws/invoke client
              {:op      :AssumeRole
               :request {:RoleArn         role-arn
                         :RoleSessionName (str (gensym session-name))}}))

(defn assumed-role-credentials-provider
  "Returns a credentials provider that can assume a role"
  [sts-client role-arn session-name]
  ;; (credentials/cached-credentials-with-auto-refresh)
  (reify credentials/CredentialsProvider
    (fetch [_]
      (when-let [creds (:Credentials (assume-role sts-client role-arn session-name))]
        {:aws/access-key-id     (:AccessKeyId creds)
         :aws/secret-access-key (:SecretAccessKey creds)
         :aws/session-token     (:SessionToken creds)
         ::credentials/ttl      (credentials/calculate-ttl creds)}))))
