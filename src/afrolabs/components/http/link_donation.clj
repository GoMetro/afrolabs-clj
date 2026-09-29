(ns afrolabs.components.http.link-donation
  "Protocol for HTTP request handlers that can contribute (\"donate\") hyperlinks to a
  rendered 404 page.

  Protocols live in their own namespace so that re-evaluating this file during interactive
  development does not invalidate the JVM interface for already-loaded implementors in other
  namespaces. See `afrolabs.components.http.four-oh-four` (the renderer) and
  `afrolabs.components.http.static-site` (an implementor).")

(defprotocol I404LinkDonater
  "Implemented by objects that can donate hyperlinks to the 404 page renderer."
  (donate-links [_]
    "Returns a link-collection: {:links [{:href \"...\" :text \"...\"} ...]}."))
