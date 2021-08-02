(ns afrolabs.service
  (:require [afrolabs.logging]
            [afrolabs.config :as -config]
            [taoensso.timbre :as log]
            [integrant.core :as ig]
            [afrolabs.components.health :as -health]
            [clojure.pprint :as pprint]
            [clojure.string :as str]))


(defn *ns*-as-config-file-path
  []
  (str (->> (-> (str *ns*)
                (str/split #"\.")
                (butlast))
            (map #(str/replace % "-" "_"))
            (str/join "/" ))
       "/config.edn"))

(defn as-system
  [{:keys [cli-args
           config-file]
    :or {config-file (*ns*-as-config-file-path)}}]

  (log/debug (str "Attempting to read config file at: " config-file))

  (let [ig-cfg (-config/read-config config-file
                                    :profile :prod
                                    :cli-args cli-args)

        ;; abusing the let-block here...
        _ (log/trace (str "Effective Integrant Config:\n"
                          (with-out-str (pprint/pprint ig-cfg))))

        ;; this does (require [...]) on all namespaces referenced in the keys of the integrant configuration map
        ;; thus if every component is a ::keyword then all the required code will by loaded at the correct time
        ;; before the components need to be initialized.
        _ (ig/load-namespaces ig-cfg)

        ig-system
        (ig/init ig-cfg)]

    ig-system))

(defn as-service
  "A service that /does logging/ is not an interactive terminal in prod and should not be writing ANSI color codes."
  [{:keys [logging-params]
    :as   cfg}]

  (apply afrolabs.logging/configure-logging!
         :disable-stacktrace-colors true
         logging-params)

  (let [{health-component ::-health/component
         :as              ig-system}
        (as-system cfg)]

    (log/info "System started...")
    (-health/wait-while-healthy health-component)

    (log/debug "Shutting down...")
    (ig/halt! ig-system)

    (log/info "System completed orderly shutdown.")))
