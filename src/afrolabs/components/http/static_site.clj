(ns afrolabs.components.http.static-site
  "A generic HTTP request handler component that serves a static website from the classpath
  (e.g. an mdbook built into JAR resources) under a configurable URL prefix.

  It is an `afrolabs.components.http/IHttpRequestHandler`: it returns `nil` for any request
  whose URI is not under its `:url-prefix`, so it composes with the `some-fn` handler chain in
  `afrolabs.components.http/create-http-component` (first non-nil wins). It also implements
  `afrolabs.components.http.link-donation/I404LinkDonater` so it can advertise itself on a
  404 page.

  Config keys:
    :url-prefix    (required) URL path the site is mounted at, e.g. \"/docs/user-guide\".
    :resource-path (required) classpath base holding the built site, e.g.
                   \"/gometro/zappp/thyristor/user_guide\".
    :link-text     (optional) text for the donated 404 link; defaults to a prettified form of
                   the last segment of :resource-path.

  Mint per-site instances with the generated `redeclare-static-site` macro, e.g.
    (redeclare-static-site :my.app/user-guide)"
  (:require
   [afrolabs.components :as -comp]
   [afrolabs.components.http :as -http]
   [afrolabs.components.http.link-donation :as -link]
   [camel-snake-kebab.core :as csk]
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [ring.middleware.content-type :as content-type-response]
   [ring.middleware.resource :as resource-response]
   [ring.util.response :as response]))

(def ^:private max-cache-age-seconds (* 1 60 60)) ;; 1 hour's worth of seconds
(def http-cache-values
  {"Cache-Control" (format "public, max-age=%d, no-cache=\"Set-Cookie\"" max-cache-age-seconds)
   "Expires"       (str max-cache-age-seconds)})

(defn add-cache-header-middleware
  [handler]
  (fn [request]
    (when-let [response (handler request)]
      (let [{{cache-control "Cache-Control"
              expires       "Expires"} :headers} response]
        (if (or cache-control expires)
          response
          (update response :headers merge http-cache-values))))))

(defn serve-static-site
  [{:as _cfg :keys [url-prefix resource-path]}
   {:as request :keys [uri]}]
  (if (#{url-prefix (str url-prefix "/")} uri)
    (response/redirect (str url-prefix "/index.html"))
    ((-> (fn [r]
           (resource-response/resource-request (update r :uri #(subs % (count url-prefix)))
                                               resource-path))
         (content-type-response/wrap-content-type)
         (add-cache-header-middleware))
     request)))

(defn- default-link-text
  "Prettifies the last segment of a resource path, e.g.
  \"/gometro/zappp/thyristor/user_guide\" -> \"User Guide\"."
  [resource-path]
  (-> resource-path
      (str/split #"/")
      (last)
      (csk/->Camel_Snake_Case)
      (str/replace #"_" " ")))

(s/def ::url-prefix (s/and string? (comp pos? count)))
(s/def ::resource-path (s/and string? (comp pos? count)))
(s/def ::link-text (s/and string? (comp pos? count)))

(s/def ::static-site-cfg
  (s/keys :req-un [::url-prefix ::resource-path]
          :opt-un [::link-text]))

(-comp/defcomponent {::-comp/ig-kw       ::static-site
                     ::-comp/config-spec ::static-site-cfg}
  [{:keys [url-prefix resource-path link-text] :as cfg}]
  (reify
    -http/IHttpRequestHandler
    (handle-http-request [_ req]
      (when (str/starts-with? (:uri req) url-prefix)
        (#'serve-static-site cfg req)))

    -link/I404LinkDonater
    (donate-links [_]
      {:links [{:href url-prefix
                :text (or link-text (default-link-text resource-path))}]})))
