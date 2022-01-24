(ns afrolabs.components.health
  (:require [integrant.core :is ig]
            [afrolabs.components :as -comp]
            [clojure.core.async :as csp]
            [clojure.spec.alpha :as s]
            [beckon]
            [taoensso.timbre :as log])
  (:import [java.lang Thread System]
           [afrolabs.components IHaltable]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; OS signals, Like when you press Ctrl-C (SIGINT)

;; signal-callbacks is an atom that contains a set of callbacks.
;; the callback-fn's must accept one parameter, a string value of the signal eg "INT" or "TERM"
;; it is intended that this callack mechanism is only accessed via the health-component
(defonce signal-callbacks
  (let [callbacks (atom #{})]
    (beckon/reinit-all!)
    (doseq [signal #{"INT" "TERM"}]
      (reset! (beckon/signal-atom signal)
              #{(fn []
                  (let [effective-callbacks @callbacks]
                    (log/warnf "Intercepted OS Signal '%s'. Calling '%d' callbacks..." signal (count effective-callbacks))
                    (doseq [cb effective-callbacks]
                      (try
                        (cb signal)
                        (catch Throwable t
                          (log/error t "Uncaught exception on the OS Signals callback :("))))))}))
    callbacks))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Default JVM-wide uncaught exception handler.
;; This triggers on any thread where the exception is uncaught.
;; the symbol uncaught-exception-handlers contains a set of callback-fn's
;; each callback must accept 2 params: the thread object, and the exception object

(defonce uncaught-exception-handlers
  (let [callbacks (atom #{})]
    (Thread/setDefaultUncaughtExceptionHandler
     (reify Thread$UncaughtExceptionHandler
       (uncaughtException [_ thread ex]
         (let [effective-callbacks @callbacks]
           (log/warn ex
                     (format "Uncaught exception on thread '%s'. Invoking %d callbacks..."
                             (.getName thread)
                             (count effective-callbacks)))
           (doseq [cb effective-callbacks]
             (try
               (cb thread ex)
               (catch Throwable t
                 (log/warn t "Uncaught exception on the uncaught exception handler. Booooooooo."))))))))

    callbacks))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol IServiceHealthTripSwitch
  (indicate-unhealthy! [_ subsystem] "A subsystem indicates that it is unhealthy.")
  (wait-while-healthy [_] "Returns when indicate-unhealth! has been called.")
  (healthy? [_] "Returns boolean indicating health."))

(s/def ::service-health-trip-switch #(satisfies? IServiceHealthTripSwitch %))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::intercept-signals boolean?)
(s/def ::intercept-uncaught-exceptions boolean?)
(s/def ::trigger-self-destruct-timer-seconds (s/or :n nil?
                                                   :i pos-int?))
(s/def ::health-cfg (s/keys :opt-un [::intercept-signals
                                     ::intercept-uncaught-exceptions
                                     ::trigger-self-destruct-timer-seconds]))

(defn make-health-component
  [{:keys [intercept-signals
           intercept-uncaught-exceptions
           trigger-self-destruct-timer-seconds]
    :as cfg}]

  (s/assert ::health-cfg cfg)

  (let [healthy? (atom true)
        not-healthy-any-longer? (promise)

        ;; when something/somebody sets the state to unhealthy
        ;; we will automatically deliver the promise
        ;; which any wait-while-healthy might be depending on
        _ (add-watch healthy? ::healthy-watcher
                     (fn [_ _ _ new-healthy-state]
                       (when (false? new-healthy-state)
                         (log/warn "In watcher; found new-healthy-state of not-healthy.")
                         (deliver not-healthy-any-longer? true)
                         (log/warn "delivered...")
                         (remove-watch healthy? ::healthy-watcher))))

        ;; helper callback
        indicate-unhealthy (fn []
                             (log/warn "Indicating inhealthy...")
                             (reset! healthy? false))

        self-destruct-singleton (delay
                                  (.start (Thread. (do
                                                     (doseq [i (reverse (range 1 trigger-self-destruct-timer-seconds))]
                                                       (log/warnf "Self destructing in %d seconds..." i)
                                                       (Thread/sleep (* i 1000)))
                                                     (System/exit 1)))))

        os-signal-handler-self-destruct-log-msg
        (delay
          (log/warn "Triggering the self-destruct timer if another OS signal arrives."))

        uncaught-exception-handler-self-destruct-log-msg
        (delay
          (log/warn "Triggering the self-destruct timer if another uncaught exception arrives."))]

    (letfn [(self-destruct
              [& _]
              (when trigger-self-destruct-timer-seconds
                @self-destruct-singleton))

            (os-signal-handler
              [signal]
              (log/fatalf "Health component received OS signal '%s', marking the system as unhealthy."
                          signal)
              (indicate-unhealthy)
              (when trigger-self-destruct-timer-seconds
                @os-signal-handler-self-destruct-log-msg
                (swap! signal-callbacks conj self-destruct)))

            (uncaught-exception-handler
              [thread exception]
              (log/fatalf exception
                          "Health component handling uncaught exception, marking system as unhealthy. Thread: '%s', Exception:\n%s"
                          (str thread) (str exception))
              (indicate-unhealthy)
              (when trigger-self-destruct-timer-seconds
                @uncaught-exception-handler-self-destruct-log-msg
                (swap! uncaught-exception-handlers conj self-destruct)))]

      (when intercept-signals
        (log/info "Setting up the OS signal handler...")
        (swap! signal-callbacks conj os-signal-handler))

      (when intercept-uncaught-exceptions
        (log/info "Setting up uncaught exceptions handler...")
        (swap! uncaught-exception-handlers conj uncaught-exception-handler))

      (reify
        IServiceHealthTripSwitch
        (indicate-unhealthy!
            [_ subsystem]
          (log/fatalf "The subsystem '%s' has indicated that the system is now unhealthy!"
                      (str subsystem))
          (indicate-unhealthy))

        (wait-while-healthy [_]
          ;; this is a promise. will block the thread until it delivers
          @not-healthy-any-longer?)

        (healthy? [_]
          ;; this is an atom, will not block, will instead return the value
          @healthy?)

        IHaltable
        (halt [_]
          (indicate-unhealthy)
          (when intercept-signals             (swap! signal-callbacks            disj os-signal-handler))
          (when intercept-uncaught-exceptions (swap! uncaught-exception-handlers disj uncaught-exception-handler))
          nil)))))


(-comp/defcomponent {::-comp/config-spec ::health-cfg
                     ::-comp/ig-kw       ::component}
  [cfg] (make-health-component cfg))
