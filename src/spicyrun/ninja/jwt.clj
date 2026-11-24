(ns spicyrun.ninja.jwt
  (:require
   [afrolabs.jwt :as -jwt]))

(defn verify&unsign-token
  "Deprecated; use `afrolabs.jwt/verify&unsign-token`."
  [{:keys [jwks-url]} token]
  (-jwt/verify&unsign-token jwks-url token))
