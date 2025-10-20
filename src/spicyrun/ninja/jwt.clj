(ns spicyrun.ninja.jwt
  (:require
   [taoensso.timbre :as log]
   [com.github.sikt-no.clj-jwt :as clj-jwt]
   [failjure.core :as f]
   [afrolabs.utils :as -utils]))

(defn verify&unsign-token
  "Does a cryptographic verification on the token using jwks-url and if this succeeds, reads out the claims and returns it in a map.
  Returns a failjure on verification failure.
  Logs to :info on verification failure."
  [{:as   cfg
    :keys [jwks-url]}
   token]
  (try (clj-jwt/unsign jwks-url
                       token)
       (catch Throwable t
         (log/with-context+ {:token    token
                             :jwks-url jwks-url}
           (log/debug t "Crypto token verification failed."))
         (f/fail "Failed crypto verification."))))
