(ns afrolabs.components.aws.cognito
  (:require [afrolabs.components :as -comp]
            [afrolabs.components.aws :as -aws]
            [afrolabs.components.aws.client-config :as -aws-client-config]
            [cognitect.aws.client.api :as aws]
            [clojure.spec.alpha :as s]
            [taoensso.timbre :as log]
            )
  (:import
   [javax.crypto Mac]
   [javax.crypto.spec SecretKeySpec]
   [java.util Base64]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; functions for calling the cognito api on the top

(def hmac-algo-name "HmacSHA256")

(defn- compute-cognito-secret-hash
  [client-secret
   username
   client-id]
  (let [key (SecretKeySpec. (.getBytes ^String client-secret "UTF-8")
                            hmac-algo-name)]
    (try
      (let [mac (Mac/getInstance hmac-algo-name)
            _ (.init ^Mac mac key)
            _ (.update ^Mac mac (.getBytes ^String username "UTF-8"))
            rawhmac-bytes (.doFinal ^Mac mac
                                    (.getBytes ^String client-id "UTF-8"))]
        (.encodeToString (Base64/getEncoder)
                         rawhmac-bytes))
      (catch Throwable t
        (log/error t "Unable to calculate the HMAC-sha256 based secret-hash for AWS API call.")
        (throw (ex-info "Unable to calculate the HMAC-sha256 based secret-hash for AWS API call."
                        {:client-secret (if client-secret "<SET>" "<UNSET>")
                         :username      username
                         :client-id     client-id}))))))

(defn admin-initiate-auth:username&password*
  "Does a simple `AdminInitiateAuth` with `ADMIN_USER_PASSWORD_AUTH` flow.

  Uses the user-pool-id, client-id, client-secret and username & password.

  Returns the raw AWS result."
  [client
   & {:as   _options
      :keys [client-id
             client-secret
             user-pool-id
             username
             password]}]
  (let [secret-hash (compute-cognito-secret-hash client-secret
                                                 username
                                                 client-id)]
    (aws/invoke client
                {:op      :AdminInitiateAuth
                 :request {:UserPoolId     user-pool-id
                           :ClientId       client-id
                           :AuthFlow       "ADMIN_USER_PASSWORD_AUTH"
                           :AuthParameters {"USERNAME"    username
                                            "PASSWORD"    password
                                            "SECRET_HASH" secret-hash}}})))

(defn admin-initiate-auth:username&password
  "Does a simple `AdminInitiateAuth` with `ADMIN_USER_PASSWORD_AUTH` flow.

  Uses the user-pool-id, client-id, client-secret and username & password.

  Returns the raw AWS result."
  [component & {:as options}]
  (admin-initiate-auth:username&password* (:client @component) options))

;;;;;;;;;;;;;;;;;;;;

(defn admin-initiate-auth:refresh-token*
  "Does an `AdminInitiateAuth` with `REFRESH_TOKEN_AUTH` flow.

  Accepts the cognitect aws-client as the first parameter.

  Returns the raw AWS result."
  [client
   & {:as   _options
      :keys [client-id
             client-secret
             user-pool-id
             refresh-token
             username]}]
  (let [secret-hash (compute-cognito-secret-hash client-secret
                                                 username
                                                 client-id)]
    (aws/invoke client
                {:op      :AdminInitiateAuth
                 :request {:UserPoolId     user-pool-id
                           :ClientId       client-id
                           :AuthFlow       "REFRESH_TOKEN_AUTH"
                           :AuthParameters {"REFRESH_TOKEN" refresh-token
                                            "SECRET_HASH"   secret-hash}}})))

(defn admin-initiate-auth:refresh-token
  "Does an `AdminInitiateAuth` with `REFRESH_TOKEN_AUTH` flow.

  Accepts the cognito component as the first parameter.

  Returns the raw AWS result."
  [component & {:as options}]
  (admin-initiate-auth:refresh-token* (:client @component)
                                      (merge options
                                             @component)))

;;;;;;;;;;;;;;;;;;;;

(defn admin-set-user-password
  "Sets the username's password to this (permanent-by-default) value.

  Returns the raw AWS result."
  [client
   & {:as _options
      :keys [user-pool-id
             username
             password
             permanent?]
      :or {permanent? true}}]
  (aws/invoke (or (:client client) client)
              {:op      :AdminSetUserPassword
               :request {:Password   password
                         :Username   username
                         :Permanent  permanent?
                         :UserPoolId user-pool-id}}))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; configuring and instantiating the component

(s/def ::aws-client-cfg map?)
(s/def ::client-id (s/and string?
                          (comp pos? count)))
(s/def ::client-secret (s/and string?
                              (comp pos? count)))
(s/def ::user-pool-id (s/and string?
                             (comp pos? count)))

(s/def ::cognito-idp-client-cfg
  (s/keys :req-un [::aws-client-cfg]
          :opt-un [::client-id
                   ::client-secret
                   ::user-pool-id]))

(defn make-cognito-client
  [{:keys [aws-client-cfg] :as cfg}]
  (let [client (-> aws-client-cfg
                   (assoc :api :cognito-idp)
                   (aws/client))
        state (assoc cfg :client client)]
    (reify
      -comp/IHaltable
      (halt [_] (aws/stop client))

      clojure.lang.IDeref
      (deref [_] state))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(-comp/defcomponent {::-comp/ig-kw       ::cognito-idp-client
                     ::-comp/config-spec ::cognito-idp-client-cfg}
  [cfg] (#'make-cognito-client cfg))
