(ns afrolabs.components.sentry
  "Sentry error reporting without the `io.sentry.Sentry` singleton.

  Each `::client` owns its own `SentryClient` and `Scopes`; several can coexist
  in one JVM. Requires `io.sentry/sentry` on the classpath, which afrolabs does
  not bring in by default.

  Events carry Clojure data as Sentry contexts:

      \"log-context\"  timbre context, or `:log-context` of the capture fns
      \"ex-data\"      `ex-data` of the first `ExceptionInfo` in the cause chain
      \"exception\"    `Throwable->map` of the exception, without `:trace`"
  (:require [afrolabs.components :as -comp]
            [afrolabs.components.version-info :as -version-info]
            [clojure.spec.alpha :as s]
            [clojure.string :as str])
  (:import [io.sentry Scope Scopes ScopeCallback SentryClient SentryEvent SentryLevel
            SentryOptions SentryOptions$BeforeSendCallback SystemOutLogger]
           [io.sentry.protocol Message]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; clojure data -> sentry json

(defn- key->str
  [k]
  (cond
    (string? k) k
    (keyword? k) (str k)
    (symbol? k) (str k)
    :else (pr-str k)))

(defn- ->sentry-data
  "Return `x` as data Sentry serializes faithfully: Java maps and lists of
  strings, numbers, booleans and nil. Keywords keep their leading colon, so
  `:order/id` reads as `\":order/id\"`."
  [x]
  (cond
    (or (nil? x) (string? x) (number? x) (boolean? x)) x
    (keyword? x) (str x)
    (symbol? x) (str x)
    (class? x) (.getName ^Class x)
    (map? x) (let [m (java.util.LinkedHashMap.)]
               (doseq [[k v] x]
                 (.put m (key->str k) (->sentry-data v)))
               m)
    (or (sequential? x) (set? x)) (java.util.ArrayList. ^java.util.Collection (mapv ->sentry-data x))
    :else (pr-str x)))

(defn- tag-name
  "Sentry tag keys may not contain `/`; `:order/id` becomes `\"order.id\"`."
  [k]
  (if (keyword? k)
    (if-let [n (namespace k)]
      (str n "." (name k))
      (name k))
    (str k)))

(defn- tag-value
  [v]
  (if (keyword? v)
    (subs (str v) 1)
    (str v)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; capturing

(defn scopes
  "Return the root `io.sentry.Scopes` of `client`, or nil when disabled."
  ^Scopes [client]
  (:scopes client))

(defn enabled?
  "Return true when `client` sends events. False for nil and for a client
  without a DSN."
  [client]
  (some? (scopes client)))

(defn- current-scopes
  "Return the `Scopes` captures from `client` go to."
  ^Scopes [client]
  (scopes client))

(def ^:private timbre->sentry-level
  {:trace  SentryLevel/DEBUG
   :debug  SentryLevel/DEBUG
   :info   SentryLevel/INFO
   :warn   SentryLevel/WARNING
   :error  SentryLevel/ERROR
   :fatal  SentryLevel/FATAL
   :report SentryLevel/FATAL})

(defn- first-ex-info
  [^Throwable t]
  (->> t
       (iterate #(.getCause ^Throwable %))
       (take-while some?)
       (filter #(instance? clojure.lang.ExceptionInfo %))
       first))

(defn- event-contexts
  [{:keys [log-context throwable]}]
  (cond-> {}
    (seq log-context)
    (assoc "log-context" log-context)

    throwable
    (assoc "exception" (dissoc (Throwable->map throwable) :trace))

    (some-> throwable first-ex-info ex-data)
    (assoc "ex-data" (ex-data (first-ex-info throwable)))))

(defn- capture-event!
  [client {:keys [throwable message level logger tags] :as opts}]
  (when-let [scopes (current-scopes client)]
    (let [event    (if throwable (SentryEvent. ^Throwable throwable) (SentryEvent.))
          contexts (event-contexts opts)]
      (.setLevel event (timbre->sentry-level level SentryLevel/ERROR))
      (when-not (str/blank? message)
        (.setMessage event (doto (Message.) (.setMessage message))))
      (when logger
        (.setLogger event logger))
      (.captureEvent scopes event nil
                     (reify ScopeCallback
                       (run [_ scope]
                         (doseq [[k v] tags]
                           (.setTag scope (tag-name k) (tag-value v)))
                         (doseq [[k v] contexts]
                           (.setContexts scope ^String k ^Object (->sentry-data v)))))))))

(defn capture-exception!
  "Send `throwable` to Sentry. Return the event's `io.sentry.protocol.SentryId`,
  or nil when `client` is disabled.

  opts:
    :tags         map; keys and values become Sentry tags
    :log-context  map; sent as the \"log-context\" context
    :level        timbre level, default :error
    :message      string; event message beside the exception"
  ([client throwable] (capture-exception! client throwable {}))
  ([client throwable opts]
   (capture-event! client (assoc opts :throwable throwable))))

(defn capture-message!
  "Send `message` at timbre `level` to Sentry. Return the event's
  `io.sentry.protocol.SentryId`, or nil when `client` is disabled.

  opts: `:tags` and `:log-context`, as for `capture-exception!`."
  ([client message level] (capture-message! client message level {}))
  ([client message level opts]
   (capture-event! client (assoc opts :message message :level level))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; timbre

(defn timbre-appender
  "Return a timbre appender map sending log calls to `client`.

  A call with an error sends the exception, with the log message as event
  message; otherwise the log message is sent. The timbre context becomes the
  \"log-context\" context, and `?ns-str` the logger.

  opts:
    :min-level  timbre appender `:min-level`, incl. the namespace-filter
                vector form. Default :warn
    :tag-keys   timbre context keys also sent as tags. Default [:request-id]

  Does nothing for a disabled `client`. Never logs or throws."
  ([client] (timbre-appender client {}))
  ([client {:keys [min-level tag-keys]
            :or   {min-level :warn
                   tag-keys  [:request-id]}}]
   {:enabled?  true
    :async?    false
    :min-level min-level
    :fn        (fn [{:keys [level ?err ?ns-str msg_ context]}]
                 (when (enabled? client)
                   (try
                     (capture-event! client
                                     {:throwable   ?err
                                      :message     (force msg_)
                                      :level       level
                                      :logger      ?ns-str
                                      :tags        (select-keys context tag-keys)
                                      :log-context context})
                     (catch Throwable _ nil))))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; component

(s/def ::non-blank-string (s/and string? (complement str/blank?)))
(s/def ::dsn (s/nilable string?))
(s/def ::environment ::non-blank-string)
(s/def ::release ::non-blank-string)
(s/def ::server-name ::non-blank-string)
(s/def ::sample-rate (s/and number? #(<= 0 % 1)))
(s/def ::tags (s/map-of (s/or :s string? :k keyword?) string?))
(s/def ::in-app-includes (s/coll-of ::non-blank-string))
(s/def ::send-default-pii? boolean?)
(s/def ::shutdown-timeout-ms pos-int?)
(s/def ::debug? boolean?)
(s/def ::version-info ::-version-info/version-info)
(s/def ::before-send ifn?)

(s/def ::client-cfg
  (s/keys :opt-un [::dsn ::environment ::release ::server-name ::sample-rate ::tags
                   ::in-app-includes ::send-default-pii? ::shutdown-timeout-ms ::debug?
                   ::version-info ::before-send]))

(defn- sentry-options
  ^SentryOptions
  [{:keys [dsn environment release server-name sample-rate tags in-app-includes
           send-default-pii? shutdown-timeout-ms debug? version-info before-send]}]
  (let [release (or release
                    (some-> version-info -version-info/version-info :git-sha))
        opts    (SentryOptions.)]
    (.setDsn opts dsn)
    (when environment (.setEnvironment opts environment))
    (when release (.setRelease opts release))
    (when server-name (.setServerName opts server-name))
    (when sample-rate (.setSampleRate opts (double sample-rate)))
    (doseq [[k v] tags] (.setTag opts (tag-name k) v))
    (doseq [p in-app-includes] (.addInAppInclude opts p))
    (when (some? send-default-pii?) (.setSendDefaultPii opts send-default-pii?))
    (when shutdown-timeout-ms (.setShutdownTimeoutMillis opts (long shutdown-timeout-ms)))
    (when debug?
      (.setDebug opts true)
      (.setLogger opts (SystemOutLogger.)))
    (when before-send
      (.setBeforeSend opts (reify SentryOptions$BeforeSendCallback
                             (execute [_ event hint] (before-send event hint)))))
    opts))

(defn make-client
  "Return a client map for `cfg` (see `::client-cfg`), haltable via
  `afrolabs.components/halt`. A blank `:dsn` returns a disabled client. Throws
  on an invalid DSN.

  `:before-send` is `(fn [^SentryEvent event hint])` returning the event to
  send, or nil to drop it."
  [{:keys [dsn] :as cfg}]
  (if (str/blank? dsn)
    {:scopes nil}
    (let [opts   (sentry-options cfg)
          global (doto (Scope. opts) (.bindClient (SentryClient. opts)))
          scopes (Scopes. (Scope. opts) (Scope. opts) global "afrolabs.components.sentry")]
      (with-meta {:scopes scopes}
        {`-comp/halt (fn [_] (.close scopes))}))))

(-comp/defcomponent {::-comp/config-spec        ::client-cfg
                     ::-comp/ig-kw              ::client
                     ::-comp/supports:disabled? true}
  [cfg]
  (make-client cfg))
