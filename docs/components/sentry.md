# sentry

## NAME

`:afrolabs.components.sentry/client` — report errors and log events to
Sentry.

## SYNOPSIS

```clojure
;; deps.edn of the service
io.sentry/sentry {:mvn/version "8.65.1"}

(require '[afrolabs.components.sentry :as -sentry])

;; integrant key
:afrolabs.components.sentry/client

;; additional instances
(-sentry/redeclare-client ::sentry:other-project)

;; functions
(-sentry/capture-exception! client throwable)
(-sentry/capture-exception! client throwable {:tags {} :log-context {} :level :error :message ""})
(-sentry/capture-message!   client message level)
(-sentry/capture-message!   client message level {:tags {} :log-context {}})
(-sentry/timbre-appender    client)
(-sentry/timbre-appender    client {:min-level :warn :tag-keys [:request-id]})
(-sentry/enabled?           client)
(-sentry/scopes             client)
```

## DESCRIPTION

A Sentry client that does not use the `io.sentry.Sentry` singleton. Several
clients, each with its own DSN, can run in one JVM. Code that calls
`io.sentry.Sentry` directly does not report through this client.

`io.sentry/sentry` is not a dependency of afrolabs. Add it to the service.
Requiring `afrolabs.components.sentry` without it fails.

The component value is the client. Pass it to the capture functions and to
`timbre-appender`.

Without a `:dsn` the client is disabled: captures return nil and send
nothing. Use this for development and tests.

On halt, queued events are sent, waiting at most `:shutdown-timeout-ms`.

## CONFIGURATION

All optional.

`:dsn`
: Sentry DSN string. nil or blank: client disabled. An invalid DSN fails
  initialisation.

`:environment`
: String, e.g. `"production"`, `"staging"`.

`:release`
: String identifying the deployed version.

`:version-info`
: `:afrolabs.components.version-info/version-info` component. Builds a sentry
  release tag from this information.

`:server-name`
: String. Default: the host name.

`:sample-rate`
: Fraction of events sent, `0.0`–`1.0`. Default `1.0`.

`:tags`
: Map of tag name to string value, set on every event. Tag names are strings
  or keywords.

`:in-app-includes`
: Collection of package or namespace prefixes. Stack frames under these
  prefixes are marked as application code: Sentry highlights them and groups
  issues by them.

`:send-default-pii?`
: Boolean. Let Sentry attach personally identifiable information. Default
  `false`.

`:shutdown-timeout-ms`
: Maximum time halt waits to send queued events. Default `2000`.

`:debug?`
: Boolean. Print the Sentry SDK's diagnostics to stdout. Default `false`.

`:before-send`
: Code only. `(fn [^io.sentry.SentryEvent event hint])` returning the event to
  send, or nil to drop it.

`:disabled?`
: Boolean. The component value is nil.

## EVENTS

Clojure data is attached as Sentry contexts, shown as sections on the event.
Keywords keep their colon, so `:order/id` shows as `":order/id"`. UUIDs
show as their string, instants as ISO-8601 UTC strings.

`"ex-data"`
: `ex-data` of the exception if it is an `ExceptionInfo`, otherwise of the
  first `ExceptionInfo` among its causes. Absent when there is none.

`"exception"`
: `Throwable->map` of the exception without `:trace`: `:via` (`:type`,
  `:message`, `:data`, `:at` of each cause), `:cause`, `:data`.

`"log-context"`
: The timbre context, or `:log-context` of the capture functions.

Tags are searchable in Sentry. Namespaced keyword tag names become
`namespace.name`: `:order/id` becomes `order.id`.

Sentry groups messages into issues by message text. Keep log messages static
and put variable data in the timbre context.

## FUNCTIONS

`(capture-exception! client throwable opts?)`
: Send `throwable`. Returns the event id, or nil when disabled. opts:
  `:tags`, `:log-context`, `:level` (timbre level, default `:error`),
  `:message`.

`(capture-message! client message level opts?)`
: Send `message` at timbre `level`. Returns the event id, or nil when
  disabled. opts: `:tags`, `:log-context`.

`(timbre-appender client opts?)`
: Returns a timbre appender map. A log call with an error sends the
  exception with the log message; otherwise the log message is sent. The log
  namespace is the event's logger. Does nothing when `client` is disabled or
  nil. Never logs and never throws.

  `:min-level` — timbre appender `:min-level`, including the
  namespace-filter form. Default `:warn`.

  `:tag-keys` — timbre context keys also sent as tags. Default
  `[:request-id]`.

Timbre levels map to Sentry levels: `:trace` and `:debug` to `debug`,
`:info` to `info`, `:warn` to `warning`, `:error` to `error`, `:fatal` and
`:report` to `fatal`.

`(enabled? client)`
: True when the client sends events.

`(scopes client)`
: The client's `io.sentry.Scopes`, or nil when disabled.

## EXAMPLES

### Client

```clojure
:afrolabs.components.version-info/version-info
{:version-info-resource "version.edn"}

:afrolabs.components.sentry/client
{:dsn                 #option SENTRY_DSN
 :environment         #or [#option SENTRY_ENVIRONMENT "dev"]
 :version-info        #ig/ref :afrolabs.components.version-info/version-info
 :tags                {"service" "gometro-bridge"
                       "region"  #or [#option AWS_REGION "af-south-1"]}
 :in-app-includes     ["afrolabs" "afrolabs.gometro-bridge"]
 :shutdown-timeout-ms 5000}
```

The `"service"` tag separates services that report into one Sentry project.

### Timbre appender

```clojure
(require '[taoensso.timbre :as log])

(log/merge-config!
 {:appenders {:sentry (-sentry/timbre-appender
                       sentry-client
                       {:min-level [[#{"org.apache.kafka.*"} :error]
                                    ["*" :warn]]
                        :tag-keys  [:request-id :tenant-id]})}})

(log/with-context+ {:request-id "r-81f2" :tenant-id "acme"}
  (log/error (ex-info "Order rejected" {:order/id 42 :reason :out-of-stock})
             "Failed to place order"))
```

The event is titled `Failed to place order`, tagged `request-id` and
`tenant-id`, and carries:

```clojure
{"ex-data"     {":order/id" 42 ":reason" ":out-of-stock"}
 "exception"   {":via"   [{":type"    "clojure.lang.ExceptionInfo"
                           ":message" "Order rejected"
                           ":data"    {":order/id" 42 ":reason" ":out-of-stock"}
                           ":at"      [...]}]
                ":cause" "Order rejected"
                ":data"  {":order/id" 42 ":reason" ":out-of-stock"}}
 "log-context" {":request-id" "r-81f2" ":tenant-id" "acme"}}
```

Remove the appender before the client halts:

```clojure
(log/merge-config! {:appenders {:sentry nil}})
```

### Direct capture

```clojure
(-sentry/capture-exception! sentry-client e {:tags        {:job "nightly-export"}
                                             :log-context {:batch-id batch-id}})
```

## CAVEATS

Breadcrumbs are not recorded.

The appender sends synchronously on the logging thread. The SDK queues
events and sends them in the background.

A log component that refs the client forms a dependency cycle under
`afrolabs.service/as-system` with `:log-component-setup`, which makes every
component depend on the log component. Exempt the client's key and its
`:version-info` component's key via `:log-component-dependencies-pred`; this
includes keys minted with `redeclare-client`.

## SEE ALSO

[version-info](version-info.md)
