(ns afrolabs.components.nrepl
   (:require
    [afrolabs.components :as -comp]
    [clojure.core.async :as csp]
    [clojure.spec.alpha :as s]
    [nrepl.server :refer [start-server stop-server]]
    [taoensso.timbre :as log]
    ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::enabled? (s/nilable boolean?))
(s/def ::port (s/nilable pos-int?))
(s/def ::bind-address (s/and string?
                             (comp pos? count)))
(s/def ::nrepl-cfg
  (s/keys :req-un []
          :opt-un [::enabled?
                   ::port
                   ::bind-address]))

;;;;;;;;;;;;;;;;;;;;

(defn make-nrepl-instance
  [{::keys [enabled?
            port
            bind-address]
    :or    {enabled?     false
            port         42069
            bind-address "localhost"}}]

  (let [stop-nrepl (csp/chan)
        halt!      (fn [] (csp/close! stop-nrepl))
        t          (if-not enabled?
                     (csp/timeout 10)
                     (csp/thread (with-open [server (start-server :port port
                                                                  :bind bind-address)]
                                   (csp/<!! stop-nrepl)
                                   (stop-server server)
                                   (log/debug (format "Done with nrepl thread on %s:%d"
                                                      bind-address
                                                      port)))
                                 nil))]
    (reify

      -comp/IHaltable
      (halt [_]
        (halt!)
        (csp/<!! t)))))

;;;;;;;;;;;;;;;;;;;;



(-comp/defcomponent {::-comp/config-spec ::nrepl-cfg
                     ::-comp/ig-kw       ::nrepl}
  [{:as cfg}] (make-nrepl-instance cfg))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
