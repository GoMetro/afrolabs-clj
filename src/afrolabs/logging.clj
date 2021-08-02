(ns afrolabs.logging
  "Singleton component for configuring the timbre logging framework"
  (:require [taoensso.timbre :as timbre
             :refer [log  trace  debug  info  warn  error  fatal  report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]]
            [timbre-json-appender.core :as tas]))

(defn configure-logging!
  [& {:keys [disable-stacktrace-colors
             min-level-maps
             gck-logging?]
      :or {disable-stacktrace-colors false
           min-level-maps [[#{"afrolabs.*"} :debug]
                           ["*" :info]]}}]

  (let [plain-logging? (not gck-logging?)]

    (cond

      plain-logging?
      (timbre/merge-config!
       (cond-> {}
         disable-stacktrace-colors (assoc :output-fn (partial timbre/default-output-fn {:stacktrace-fonts {}}))
         (not disable-stacktrace-colors) (assoc :output-fn timbre/default-output-fn)))

      gck-logging?
      (do
        (tas/install)
        (timbre/merge-config!
         (cond-> {}
           min-level-maps (assoc :min-level min-level-maps)))))))

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


