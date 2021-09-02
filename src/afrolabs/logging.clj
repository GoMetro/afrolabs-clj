(ns afrolabs.logging
  "Singleton component for configuring the timbre logging framework"
  (:require [taoensso.timbre :as timbre
             :refer [log  trace  debug  info  warn  error  fatal  report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]]
            [timbre-json-appender.core :as tas]))

(def default-min-log-level-maps [[#{"afrolabs.*"} :debug]
                                 ["*" :info]])

(defn configure-logging!
  [& {:keys [disable-stacktrace-colors
             min-level-maps
             min-level
             gck-logging?
             logz-io-logging?
             ]
      :or {disable-stacktrace-colors false
           min-level-maps            default-min-log-level-maps
           min-level                 :info}}]

  (let [default-tas-config {:level               min-level
                            :should-log-field-fn (constantly true)}]
    (cond
      gck-logging?     (tas/install default-tas-config)
      logz-io-logging? (tas/install (assoc default-tas-config
                                           :msg-key :message))
      :else            (timbre/merge-config!
                        (when disable-stacktrace-colors
                          {:output-fn (partial timbre/default-output-fn {:stacktrace-fonts {}})}))))

  (timbre/merge-config! {:min-level min-level-maps}))

(comment

  (configure-logging! :disable-stacktrace-colors true)
  (configure-logging! :disable-stacktrace-colors false)
  (configure-logging!)

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; use ...

  (timbre/info (Exception. "heoeu"))
  (debug (Exception. "oeuoeuoeu"))
  (info (Exception. "oeuoeuoeu"))

  )


