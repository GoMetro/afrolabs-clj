(ns afrolabs.logging
  "Singleton component for configuring the timbre logging framework"
  (:require [taoensso.timbre :as timbre
             :refer [log  trace  debug  info  warn  error  fatal  report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]]
            [timbre-json-appender.core :as tas]))

(def default-min-log-level-maps [[#{"afrolabs.*"} :debug]
                                 ["*" :info]])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; appender that prints context values to output stream

(let [system-newline (System/getProperty "line.separator")]
  (defn- atomic-println [x]
    (print (str x system-newline))
    (flush)))

(defn context-println-appender
  "timbre println appender that additionally prints out the logging context as part of the log output.

  based on example at https://github.com/ptaoussanis/timbre/blob/master/src/taoensso/timbre/appenders/example.clj"
  [& {:keys [colorized-output?]
      :or   {colorized-output? false}}]
  (cond-> {:enabled? true
           :fn       (fn [data]
                       (let [{:keys [output_ context]} data
                             output                    (force output_)]
                         (atomic-println (str output
                                              (when (seq context)
                                                (str " :: [" context "]"))))))}
    (not colorized-output?)
    (assoc :output-fn (partial timbre/default-output-fn {:stacktrace-fonts {}}))))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn configure-logging!
  [& {:keys [disable-stacktrace-colors
             min-level-maps
             min-level
             gck-logging?
             logz-io-logging?
             println-context?
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
                        (merge
                         (when disable-stacktrace-colors
                           {:output-fn (partial timbre/default-output-fn {:stacktrace-fonts {}})})
                         (when println-context?
                           {:appenders {:println (context-println-appender {})}})))))

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn setup-simple-cli-logging!
  "Sets up logging with the context-aware println logger."
  []
  (timbre/merge-config! {:appenders
                         {:println (context-println-appender :colorized-output? true)}}))
