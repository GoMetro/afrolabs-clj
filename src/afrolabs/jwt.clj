(ns afrolabs.jwt
  (:require
   [com.github.sikt-no.clj-jwt :as clj-jwt]
   [failjure.core :as f]
   [taoensso.timbre :as log]))

(defn verify&unsign-token
  "Verify `token` against the JWKS at `jwks-url` and return its claims map.

  Return a failjure on verification failure, logged at :info. The token itself
  is never logged."
  [jwks-url token]
  (try (clj-jwt/unsign jwks-url token)
       (catch Throwable t
         (log/with-context+ {:jwks-url jwks-url}
           (log/info t "Crypto token verification failed."))
         (f/fail "Failed crypto verification."))))
