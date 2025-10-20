(ns afrolabs.components.aws.cognito
  (:require [afrolabs.components :as -comp]
            [afrolabs.components.aws :as -aws]
            [afrolabs.components.aws.client-config :as -aws-client-config]
            [cognitect.aws.client.api :as aws]
            [clojure.spec.alpha :as s]
            [taoensso.timbre :as log]
            [org.httpkit.client :as http-client]
            [afrolabs.utils :as -utils]
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

(s/def ::auth-url (s/and string?
                         (comp pos? count)))
(s/def ::auth-scope (s/and string?
                           (comp pos? count)))
(s/def ::redirect-uri (s/and string?
                             (comp pos? count)))

(s/def ::cognito-idp-client-cfg
  (s/keys :req-un [::aws-client-cfg]
          :opt-un [::client-id
                   ::client-secret
                   ::user-pool-id

                   ::auth-url
                   ::auth-scope
                   ::redirect-uri]))

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

#_(defn wrap-idp-session-refresh
  "Verifies if the user's cognito session is still valid (for long enough) and refreshes cognito tokens in the session if tokens are about to expire or have expired."
  [handler]
  (fn [{:keys [session]
        :as   request}]
    (let [session-expires-when (some-> session
                                       :idp.session.cognito/expires-when
                                       (t/instant)
                                       (t/<< (t/new-duration 5 :minutes)))
          updated-session
          (if (or (not session-expires-when)
                  (t/> session-expires-when (t/now)))
            session ;; session does not exist or the session is not about to expire so we can keep the value we have

            ;; the cognito session exists and the tokens have to be refreshed
            (let [refresh-token-result
                  (idp/refresh-tokens (-> (:service/idp request)
                                          (select-keys [:user-pool-id
                                                        :client-id
                                                        :cognito-client])
                                          (assoc :refresh-token
                                                 (:idp.session.cognito/refresh-token session))))]

              ;; we will only return a value for updated-session if the user session is still valid
              ;; the user might be logged out via Cognito's API in which case the refresh-tokens
              ;; call will fail.
              (if (or (:cognitect.anomalies/category refresh-token-result)
                      (not (:AuthenticationResult refresh-token-result)))
                ::user-must-be-logged-out
                (merge session
                       (users/cognito-auth-result->session-data (:AuthenticationResult refresh-token-result))))))]

      (if (= ::user-must-be-logged-out updated-session)
        (login-page/logged-out-response (:session request)
                                        "You have been logged out.")
        (let [;; We don't want to change/update/set the session in the request map if it is nil
              ;;
              ;; Call the endpoint handler and inspect the response map:
              ;; - if the response-map session is nil, then we want to set it to the updated-session
              ;;   so that the new session value can be saved by the session middleware
              ;; - if the response-map session is non-nil, then it has been updated already and we want to avoid changing it
              {response-session :session
               :as              response}
              (handler (cond-> request
                         updated-session (assoc :session updated-session)))]

          (cond
            ;; the response-session _has_ a value, so we don't modify the response map
            ;; OR the :session key is present, with a nil value (which means the session must be deleted)
            (or response-session
                (contains? response :session)) response

            ;; we don't have a response-session (not even nil) AND
            ;; we did not modify the session so nothing happened, keep the response map
            (= session updated-session) response

            ;; the response did not do anything with the session, because the response :session key is not present
            ;; we did actually modify the session though, so to let the session middleware update/persist the session
            ;; we will set the session value in the response map
            :else
            (assoc response :session updated-session)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn cognito:authentication:retrieve-code-request
  "Makes a request to retrieve the user's authentication tokens using the token flow.
  Returns the http Response of this call.
  "
  [{:as   _cfg
    :keys [auth-url
           client-id
           client-secret
           redirect-uri]}
   code]
  @(http-client/post (str auth-url "/oauth2/token")
                     {:headers {"Content-Type" "application/x-www-form-urlencoded"
                                "Authorization" (str "Basic "
                                                     (.encodeToString (Base64/getEncoder)
                                                                      (.getBytes (str client-id ":" client-secret))))}
                      :body    (str "grant_type=authorization_code"
                                    "&client_id="    client-id
                                    "&redirect_uri=" (-utils/param-url-encode redirect-uri)
                                    "&code="         code)}))

(defn cognito:authentication:full-login-url
  "This is the URL where the user must go to log in."
  [{:as   _cfg
    :keys [managed-login-ui
           client-id
           auth-scope
           redirect-uri]}]
  (str managed-login-ui
       "?client_id="         client-id
       "&response_type=code" ;;
       "&scope="             auth-scope
       "&redirect_uri="      (-utils/param-url-encode redirect-uri)))

(defn other-name
  "  Handles  an old-school server-side authentication flow callback from hosted-ui cognito."
  [])
