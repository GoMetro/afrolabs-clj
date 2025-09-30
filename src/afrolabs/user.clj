(ns afrolabs.user
  (:require
   [afrolabs.config :as -config]
   [afrolabs.service :as -service]
   [integrant.core :as ig]
   [taoensso.timbre :as log]
   ))


(defonce dev-system (atom nil))

(defn make-dev-system
  "Creates and starts a \"dev\" system.
  Accepts `config-files`, a map of :keyword -> resource-path-to-integrant-edn-file
  and `default-cfg`. This is passed to the `-service/as-system` fn."
  [config-files
   & {:keys [config-file]
      :as   default-cfg}]
  (-service/as-system (merge {:profile :dev}
                             (into {} default-cfg)
                             {:config-file    (or (get config-files config-file)
                                                  (when (string? config-file) config-file)
                                                  (:default config-files))})))

(defn dev-start! [& {:as   args
                     :keys [the-system
                            config-files]}]
  (reset! (or the-system
              dev-system)
          (make-dev-system config-files (dissoc args :config-files :the-system)))
  :ok)

(defn dev-stop!
  [& {:keys [the-system]}]
  (swap! (or the-system
             dev-system)
         #(when % (ig/halt! %))))

(defn dev-reset!
  [& {:as args}]
  (dev-stop! args)
  (dev-start! args))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn main-impl
  "An implementation of a main file. Intended use is to be called from the real `-main`, with a first-param of
  `config-files` being a map of :keyword -> resource-path-of-integrant-edn-file.
  The rest parameters are the normal cli args.

  Starts a system with respect to the `config-files` (all available) and the first param `config-file` (selected config).
  Respects `IG_CFG_KS` environment variable, to start a sub-selection of systems in the selected config.edn file."
  [config-files & [config-file & args]]
  (try
    (let [config-file (or (when config-file (keyword config-file))
                          :default)
          all-config-file-options (set (keys config-files))
          ig-cfg-ks (when-let [env-value (System/getenv "IG_CFG_KS")]
                      (log/debug (str "Using integrant-config-keys value: '" env-value "'."))
                      (let [edn-value
                            (->> env-value
                                 (-config/interpret-string-as-csv-row)
                                 (map keyword)
                                 (set))]
                        (log/with-context+ {:ig-cfg-kys edn-value}
                          (log/info (str "Starting a subset of components: " edn-value)))
                        edn-value))]
      (when-not (all-config-file-options config-file)
        (throw (ex-info (format "Specify one of the available config files to start. Invalid option: '%s'."
                                config-file)
                        {:provided config-file
                         :available all-config-file-options})))
      (-service/as-service
       (cond-> {:cli-args            args
                :config-file         (get config-files config-file)}
         ig-cfg-ks (assoc :ig-cfg-ks ig-cfg-ks)))
      (System/exit 0))
    (catch Throwable t
      (.printStackTrace t)
      (log/error t)
      (System/exit 1))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmacro init-system
  "This macro install fn's for `dev-reset!`, `dev-stop!` & `dev-start!`. It also installs a `-main` fn.

  Accepts a map of :keyword -> resource-path-of-integrant-edn-files."
  [config-files]
  `(do (defn ~(symbol "dev-start!")
         [{:as args#}]
         (afrolabs.user/dev-start! (assoc args# :config-files ~config-files)))
       (def ~(symbol "dev-stop!")  afrolabs.user/dev-stop!)
       (defn ~(symbol "dev-reset!")
         [& {:as args#}]
         (afrolabs.user/dev-reset! (assoc args# :config-files ~config-files)))
       (defn ~(symbol "-main")
         [& args#]
         (apply afrolabs.user/main-impl ~config-files args#))))
