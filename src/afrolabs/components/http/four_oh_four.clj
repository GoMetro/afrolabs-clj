(ns afrolabs.components.http.four-oh-four
  "A generic HTTP request handler component that renders a \"nice\" 404 page listing useful
  links.

  Because its `handle-http-request` ALWAYS returns a response, mount it as the LAST handler in
  a port's handler vector: the `some-fn` chain in `afrolabs.components.http/create-http-component`
  reaches it only when every earlier handler declined (returned `nil`).

  The links shown come from two sources, both optional-to-populate:
    - `:link-donaters`  — objects implementing
      `afrolabs.components.http.link-donation/I404LinkDonater` (typically Integrant refs to the
      other handlers mounted on the same port), each asked to `donate-links` at render time.
    - `:hardcoded-links`— a collection of link-collections ({:links [{:href .. :text ..} ...]}).

  Config keys:
    :title           (optional) page/tab title; defaults to \"404 — Not Found\".
    :link-donaters   (required) collection of I404LinkDonater.
    :hardcoded-links (optional) collection of link-collections.

  Mint per-port instances with the generated `redeclare-four-oh-four` macro, e.g.
    (redeclare-four-oh-four :my.app/external-404)"
  (:require
   [afrolabs.components :as -comp]
   [afrolabs.components.http :as -http]
   [afrolabs.components.http.link-donation :as -link]
   [clojure.spec.alpha :as s]
   [hiccup2.core :as h]
   [ring.util.response :as response]))

(def default-title "404 — Not Found")

(defn make-404-response
  [{:as _cfg :keys [title link-donaters hardcoded-links]}]
  (let [title        (or title default-title)
        links-hiccup (->> (concat (mapcat (comp :links -link/donate-links) link-donaters)
                                  (mapcat :links hardcoded-links))
                          (map (fn [{:keys [href text]}]
                                 [:li [:a {:href href} text]])))
        links-section [:ul links-hiccup]
        page [:html {:lang "en"}
              [:head
               [:meta {:charset "UTF-8"}]
               [:meta {:name    "viewport"
                       :content "width=device-width, initial-scale=1.0"}]
               [:meta {:http-equiv "X-UA-Compatible"
                       :content    "ie=edge"}]
               [:title title]
               [:script {:src "https://cdn.tailwindcss.com?plugins=typography,forms"}]]
              [:body
               [:main {:class "m-5 place-content-center max-w-none prose "}
                [:h1 title]
                [:p "Try one of these endpoints:"]
                links-section]]]]
    (-> (str (h/html (h/raw "<!DOCTYPE html>") page))
        (response/response)
        (response/content-type "text/html")
        (response/status 404))))

(defn make-four-oh-four
  [cfg]
  (reify
    -http/IHttpRequestHandler
    (handle-http-request [_ _req]
      (make-404-response cfg))))

(s/def ::title (s/and string? (comp pos? count)))
(s/def ::href (s/and string? (comp pos? count)))
(s/def ::text (s/and string? (comp pos? count)))
(s/def ::link (s/keys :req-un [::href ::text]))
(s/def ::links (s/coll-of ::link))
(s/def ::link-collection (s/keys :req-un [::links]))

(s/def ::link-donaters
  (s/coll-of (partial satisfies? -link/I404LinkDonater)))
(s/def ::hardcoded-links
  (s/coll-of ::link-collection))

(s/def ::four-oh-four-cfg
  (s/keys :req-un [::link-donaters]
          :opt-un [::title ::hardcoded-links]))

(-comp/defcomponent {::-comp/ig-kw       ::four-oh-four
                     ::-comp/config-spec ::four-oh-four-cfg}
  [cfg] (make-four-oh-four cfg))
