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


;; TODO - require a bloody spec for the arguments
(defn as-system
  [{:keys [cli-args
           config-file
           dotenv-file
           profile
           ig-cfg-ks]
    :or {config-file (*ns*-as-config-file-path)}}]

  (log/debug (str "Attempting to read config file at: " config-file))

  (let [dotenv-file (or dotenv-file ".env")
        ig-cfg (-config/read-config config-file
                                    :dotenv-file dotenv-file
                                    :profile (or profile :prod)
                                    :cli-args cli-args)

        ig-cfg-ks (or ig-cfg-ks (keys ig-cfg)) ;; default is everything

        ;; abusing the let-block here...
        _ (log/trace (str "Effective Integrant Config:\n"
                          (with-out-str (pprint/pprint ig-cfg))))

        ;; this does (require [...]) on all namespaces referenced in the keys of the integrant configuration map
        ;; thus if every component is a ::keyword then all the required code will by loaded at the correct time
        ;; before the components need to be initialized.
        _ (ig/load-namespaces ig-cfg
                              ig-cfg-ks)]

    (try
      (ig/init ig-cfg ig-cfg-ks)
      (catch Throwable t
        (log/error t "Error while initializing system. Shutting down the components that were started already...")
        (try (ig/halt! (-> t ex-data :system))
             (catch Throwable _ nil))
        nil))))

(defn as-service
  "A service that /does logging/ is not an interactive terminal in prod and should not be writing ANSI color codes."
  [{:keys [logging-params]
    :as   cfg}]

  (apply afrolabs.logging/configure-logging!
         :disable-stacktrace-colors true
         logging-params)

  (try
    (let [{health-component ::-health/component
           :as              ig-system}
          (as-system cfg)]

      (log/info "System started...")
      (-health/wait-while-healthy health-component)

      (log/info "Shutting down...")
      (ig/halt! ig-system)

      (log/info "System completed orderly shutdown."))

    (catch Throwable t
      (log/fatal t "An unrecoverable error occurred.")
      (System/exit 1))))
