(ns afrolabs.components.health
  (:require [integrant.core :is ig]
            [afrolabs.components :as -comp]
            [clojure.core.async :as csp]
            [clojure.spec.alpha :as s]
            [beckon]
            [taoensso.timbre :as log])
 (:import [java.lang Thread]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; OS signals, Like when you press Ctrl-C (SIGINT)

;; SIGINT or such will post into this channel
(defonce signal-ch (atom (csp/chan (csp/dropping-buffer 1))))

;; resets the process' signal handling to be a concern of this namespace
(defn init-signals!
  []
  (beckon/reinit-all!)
  (reset! (beckon/signal-atom "INT")
          #{(fn [] (csp/>!! @signal-ch "INT"))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Default JVM-wide uncaught exception handler.
;; This triggers on any thread where the exception is uncaught.

(defonce uncaught-exception-ch (atom (csp/chan (csp/dropping-buffer 1))))

(defn init-uncaught-exception-handler!
  []
  (Thread/setDefaultUncaughtExceptionHandler
   (reify Thread$UncaughtExceptionHandler
     (uncaughtException [_ thread ex]
       (log/fatal ex
                  (str "Uncaught exception on" (.getName thread)))
       (csp/>!! @uncaught-exception-ch
                [(.getName thread) ex])))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol IServiceHealthTripSwitch
  (indicate-unhealthy! [_ subsystem] "A subsystem indicates that it is unhealthy.")
  (wait-while-healthy [_] "Returns when indicate-unhealth! has been called.")
  (healthy? [_] "Returns boolean indicating health."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn make-health-component
  [{:keys [intercept-signals
           intercept-uncaught-exceptions]}]
  (let [health-ch (csp/chan)
        -healthy? (atom true)
        -indicate-unhealthy #(do (csp/close! health-ch)
                                 (reset! -healthy? false))]

    ;; in prod, interept-signals must be true
    ;; so the ServireHealthTripSwitch can be triggerd when the process receives SIGINT
    ;; in dev, we don't want to use signals.
    (if-not intercept-signals (log/warn "Not listening for SIGINT.")
            (do
              (log/info "Setting up SIGINT listener...")
              (init-signals!)
              (csp/go
                (let [sigint-ch @signal-ch
                      [v ch] (csp/alts! [health-ch sigint-ch])]
                  (when (= ch sigint-ch)
                    (log/fatal (str "Intercepted an OS signal: '" v "'. Marking the system as unhealthy..."))
                    (-indicate-unhealthy))))))

    ;; do we want this component do think it's unhealthy if some thread somewher has lost itself
    ;; in the throws of a violent exception? Yes... yes we want that.
    (if-not intercept-uncaught-exceptions (log/warn "Not paying attention to uncaught exceptions.")
            (do
              (log/info "Listening for uncaught exceptions...")
              (init-uncaught-exception-handler!)
              (csp/go
                (let [uncaught-exs-ch @uncaught-exception-ch
                      [_ ch] (csp/alts! [health-ch uncaught-exs-ch])]
                  (when (= ch uncaught-exception-ch)
                    (log/fatal (str "Found an uncaught exception handler on another thread: Marking the system as unhealthy..."))
                    (-indicate-unhealthy))))))

    ;; this is the implementation that's being returned as a component.
    (reify
      ;;;;;;;;;;
      IServiceHealthTripSwitch
      (indicate-unhealthy!
          [_ p]
        (log/warn (str "A subsystem has indicated that it is unhealthy: " p))
        (-indicate-unhealthy))

      (wait-while-healthy [_] (csp/<!! health-ch))
      (healthy? [_] @-healthy?)

      ;;;;;;;;;;
      -comp/IHaltable
      (halt
          [_]
        (csp/close! health-ch)))))


(s/def ::intercept-signals boolean?)
(s/def ::intercept-uncaught-exceptions boolean?)
(s/def ::health-cfg (s/keys :opt-un [::intercept-signals
                                     ::intercept-uncaught-exceptions]))

(-comp/defcomponent {::-comp/config-spec ::health-cfg
                     ::-comp/ig-kw       ::component}
  [cfg] (make-health-component cfg))
