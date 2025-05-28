(ns afrolabs.components
  (:require [integrant.core :as ig]
            [clojure.spec.alpha :as s]
            [taoensso.timbre :as log]))

(defprotocol IHaltable
  :extend-via-metadata true
  (halt [_] "Performs any state cleanup associated with a component."))

(defmacro -write-integrant-multis
  [ig-kw init-fn-name]
  `(do
     (defmethod ig/init-key ~ig-kw
       [cfg-key# cfg#]
       (~init-fn-name cfg-key# cfg#))

     (defmethod ig/halt-key! ~ig-kw
       [cfg-key# state#]
       (when (and state#
                  (satisfies? IHaltable state#))
         (halt state#)))
     ))

(defmacro defcomponent
  "Defines a new component."
  [{::keys [config-spec ig-kw supports:disabled?]}
   body-destruct
   & body]
  (let [[cfg-sym] body-destruct
        init-fn-name (symbol (str "init-fn-" (name ig-kw)))
        redeclaration-macro-name (symbol (str "redeclare-" (name ig-kw)))
        ]
    `(do
       (defn ~init-fn-name
         [cfg-key# cfg#]

         (if (or (not ~supports:disabled?)
                 (not (:disabled? cfg#)))
           (do

             ;; validation of config against config specification
             (when-not (s/valid? ~config-spec cfg#)
               (throw (ex-info (str "Component '"
                                    cfg-key#
                                    "' did not receive valid configuration.")
                               {::explain-str  (s/explain-str ~config-spec cfg#)
                                ::explain-data (s/explain-data ~config-spec cfg#)
                                ::spec         ~config-spec})))

             ;; the body passed to the defcomponent, is actually the implementation of the ig/init-key, but kept in this separate init-fn
             ;; inline it here
             (let [~cfg-sym (assoc cfg#
                                   :afrolabs.components/ig-kw        ~ig-kw
                                   :afrolabs.components/component-kw cfg-key#)]
               ~@body))
           (do (log/info (str "Component '" cfg-key# "' is disabled.")))))

       (-write-integrant-multis ~ig-kw ~init-fn-name)

       (defmacro ~redeclaration-macro-name [kw#]
         `(-write-integrant-multis ~kw# ~~init-fn-name)))))


(s/def ::identity-component-cfg any?)
(defcomponent {::ig-kw       ::identity-component
               ::config-spec ::identity-component-cfg}
  [self] self)
