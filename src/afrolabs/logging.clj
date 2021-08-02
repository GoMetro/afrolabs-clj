(ns afrolabs.logging
  "Singleton component for configuring the timbre logging framework"
  (:require [taoensso.timbre :as timbre
             :refer [log  trace  debug  info  warn  error  fatal  report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]]))

(defn configure-logging!
  [& {:keys [disable-stacktrace-colors
             min-level-maps]
      :or {disable-stacktrace-colors false
           min-level-maps [[#{"afrolabs.*"} :debug]
                           ["*" :info]]}}]

  (timbre/merge-config!
   (cond-> {}
     disable-stacktrace-colors (assoc :output-fn (partial timbre/default-output-fn {:stacktrace-fonts {}}))
     (not disable-stacktrace-colors) (assoc :output-fn timbre/default-output-fn)
     min-level-maps (assoc :min-level min-level-maps))))

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


