(ns afrolabs.service
  (:require [afrolabs.logging]
            [afrolabs.config :as -config]
            [taoensso.timbre :as log]
            [integrant.core :as ig]
            [afrolabs.components.health :as -health]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [clojure.spec.alpha :as s]
            [afrolabs.spec :as -spec]))


(defn *ns*-as-config-file-path
  []
  (str (->> (-> (str *ns*)
                (str/split #"\.")
                (butlast))
            (map #(str/replace % "-" "_"))
            (str/join "/" ))
       "/config.edn"))

(s/def ::log-component-kw keyword?)
(s/def ::log-component-dependencies-pred ifn?)

(s/def ::log-component-setup
  (s/keys :req-un [::log-component-kw
                   ::log-component-dependencies-pred]))

;; TODO - require a bloody spec for the arguments
(defn as-system
  [{:keys [cli-args
           config-file
           dotenv-file
           profile
           ig-cfg-ks
           logging-params
           log-component-setup]
    :or {config-file (*ns*-as-config-file-path)}}]

  (when logging-params
    (apply afrolabs.logging/configure-logging!
           logging-params))

  (when log-component-setup
    (-spec/assert! ::log-component-setup log-component-setup))

  (log/info (str "Reading config file at: " config-file))

  (let [dotenv-file (or dotenv-file ".env")
        ig-cfg' (-config/read-config config-file
                                     :dotenv-file dotenv-file
                                     :profile (or profile :prod)
                                     :cli-args cli-args)
        ;; when log-component-setup is defined, we will dynamically modify the aero config.
        ;; We will make all components depend ON the logging component (log-component-kw)
        ;; EXCEPT the ones that satisfies log-component-dependencies-pred
        ig-cfg (if-not log-component-setup
                 ig-cfg'
                 (into {}
                       (map (fn [[component-kw component-cfg]]
                              [component-kw
                               (if (and (not ((:log-component-dependencies-pred log-component-setup) component-kw))
                                        (not= (:log-component-kw log-component-setup)
                                              component-kw))
                                 (assoc component-cfg
                                        ::fake-logging-dependency (ig/ref (:log-component-kw log-component-setup)))
                                 component-cfg)]))
                       ig-cfg'))

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
  "A service that /does logging/ is not an interactive terminal in prod and should not be writing ANSI color codes.
  - `logging-params` may have several values:
    - `nil` - default logging setup will be performed
    - sequence of parameters that will be passed to `as-system`. Read the code there to understand what it will do.
    - `:component` - Used when there is a seperate logging integrant component. This is a backward-compatibility flag.
  "
  [{:keys [logging-params]
    :as   cfg}]
  (try
    (let [{health-component ::-health/component
           :as              ig-system}
          (as-system (cond-> cfg
                       (not= :component logging-params)
                       (assoc :logging-params (into [:disable-stacktrace-colors true]
                                                    logging-params))))]

      (log/info "System started...")
      (-health/wait-while-healthy health-component)

      (log/info "Shutting down...")
      (ig/halt! ig-system)

      (log/info "System completed orderly shutdown."))

    (catch Throwable t
      (log/fatal t "An unrecoverable error occurred.")
      (System/exit 1))))
