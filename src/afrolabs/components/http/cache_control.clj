(ns afrolabs.components.http.cache-control
  "Reitit middleware that sets `Cache-Control` from route data.

  A route declares `{::cache-control \"<value>\"}`. Its responses with status below 400 get
  that header, unless the handler set `Cache-Control` already. Routes without the key are not
  wrapped.

    [\"/bundle/*path\" {::cache-control/cache-control \"public, max-age=31536000, immutable\"
                       :get handler}]")

(defn- with-cache-control
  [response value]
  (if (and response
           (< (:status response 200) 400)
           (not-any? #(.equalsIgnoreCase "Cache-Control" ^String %) (keys (:headers response))))
    (assoc-in response [:headers "Cache-Control"] value)
    response))

(def middleware
  {:name    ::cache-control
   :compile (fn [{::keys [cache-control]} _opts]
              (when cache-control
                (fn [handler]
                  (fn
                    ([request]
                     (with-cache-control (handler request) cache-control))
                    ([request respond raise]
                     (handler request #(respond (with-cache-control % cache-control)) raise))))))})
