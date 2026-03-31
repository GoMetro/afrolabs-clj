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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; in-memory log tap
;;
;; A single global ring buffer appender. Install it once; read from anywhere.
;; nil = not installed.

(defonce ^:private tap-state (atom nil))

(defn- log-tap-appender-fn
  [{:keys [output_ level ?ns-str ?file ?line ?err context]}]
  (let [entry {:output  (force output_)
               :level   level
               :ns      ?ns-str
               :file    ?file
               :line    ?line
               :context context
               :error   ?err}]
    (swap! tap-state
           (fn [{:keys [mode n buffer] :as state}]
             (when state
               (let [buf' (conj buffer entry)]
                 (assoc state :buffer
                        (if (and (= mode :most-recent-n) (> (count buf') n))
                          (subvec buf' 1)
                          buf'))))))))

(defn install-log-tap!
  "Install a global in-memory log buffer appender. Idempotent — calling again
  when already installed is a no-op.

  mode:
    :most-recent-n  (default) retain only the last `n` log entries (ring buffer)
    :all            retain every entry (unbounded — use with care)"
  ([] (install-log-tap! :most-recent-n 100))
  ([mode] (install-log-tap! mode 100))
  ([mode n]
   (when (compare-and-set! tap-state nil {:mode mode :n n :buffer []})
     (timbre/merge-config!
      {:appenders {::log-tap {:enabled? true
                              :async?   false
                              :fn       log-tap-appender-fn}}}))))

(defn log-tap-entries
  "Return the current contents of the in-memory log buffer, oldest first.
  Returns nil if the tap has not been installed."
  []
  (:buffer @tap-state))

(defn uninstall-log-tap!
  "Remove the in-memory log buffer appender and discard buffered entries."
  []
  (reset! tap-state nil)
  (timbre/merge-config! {:appenders {::log-tap nil}}))
