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

    ;; json-based logging output
    ;; needs a side-effect to install itself
    ;; so we do that up-front
    (when gck-logging? (tas/install))

    (timbre/merge-config!
     (merge
      {:min-level min-level-maps}
      (cond
        plain-logging?
        (cond-> {}
          disable-stacktrace-colors (assoc :output-fn (partial timbre/default-output-fn {:stacktrace-fonts {}}))
          (not disable-stacktrace-colors) (assoc :output-fn timbre/default-output-fn)))))))

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


