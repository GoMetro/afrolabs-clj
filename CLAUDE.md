# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`afrolabs-clj` is a Clojure library of reusable [Integrant](https://github.com/weavejester/integrant) components for Afrolabs microservices. It provides opinionated, production-ready components for HTTP, Kafka, AWS, health management, observability, and configuration.

## Commands

```bash
# AOT-compile Kafka serializers and Lambda runtime (required before using as a library)
clojure -M:build/task compile-aot

# Clean build artifacts
clojure -M:build/task clean

# Display build configuration
clojure -M:build/task config

# Vulnerability scan
clojure -M:build/nvd-vulnerabilities scan -p deps.edn

# Run tests (no test alias defined — use cognitect test runner directly)
clojure -X:test

# REPL
clojure -M:repl  # or just: clj
```

There is no dedicated test runner alias in `deps.edn`. The single test file is `test/afrolabs/components/kafka_test.clj`.

**AOT compilation is required** (via `:deps/prep-lib`) because Kafka serializer classes must exist as compiled `.class` files in `target/classes/` to be loadable by the JVM at runtime.

At the time of writing Java 25 and Clojure 1.12.4 are used (see `.tool-versions` which uses `asdf` to specify authoritive versions for dependencies).

## Architecture

### Component System (`afrolabs.components`)

The main benefit of the component system is that it allows client applications to use the `(ns-alias/redeclare-* ::new-component-integrant-key)` macros to allow multiple distinct instances of the same component in client integrant configurations. This is in addition to the default integrant component keyword that the component is declared with.

For some components this is useful, like kafka consumers, producers, and the http server component, which is often used twice like below example. 

#### Component Usage Example

Using the http component twice in one integrant config.

```clojure
(ns some-ns
  (:require [afrolabs.components.http :as -http]))
  
(-http/redeclare-service ::http)
(-http/redeclare-service ::http-internal)
```

with corresponding example client `config.edn` snippets:
```clojure
 :some-ns/http-internal
 {:handlers [#ig/ref :afrolabs.prometheus/metrics-endpoint
             #ig/ref :afrolabs.components.http/health-endpoint
             #ig/ref :afrolabs.gometro-bridge.geofences.alerts.common/fake-geofence-alerts-callback
             #ig/ref :afrolabs.gometro-bridge.callback/component]
  :port     #ref [:afrolabs.gometro-bridge.core/common-http-config :internal-port]
  :ip       #ref [:afrolabs.gometro-bridge.core/common-http-config :internal-ip]}

 :some-ns/http
 {:handlers             [#ig/ref :afrolabs.components.http/health-endpoint
                         #ig/ref :afrolabs.gometro-bridge.v2.rest/v2-http-api

                         #ig/ref :afrolabs.gometro-bridge.data-model.schemas/component
                         #ig/ref :afrolabs.gometro-bridge.docs/http-handler

                         #ig/ref :afrolabs.gometro-bridge.four-oh-four/component]
  :middleware-providers [#ig/ref :afrolabs.components.http/git-version-middleware]
  :port                 #long #or [#option HTTP_PORT
                                   "8000"]
  :ip                   #or [#option HTTP_IP
                             "127.0.0.1"]}
```

All components use the `defcomponent` macro, which wraps Integrant's `init-key`/`halt-key!` multimethods. It auto-validates config against a `clojure.spec.alpha` spec and supports optional `:disabled?` config:

```clojure
(defcomponent {::config-spec ::my-cfg-spec
               ::ig-kw       ::my-component
               ::supports:disabled? true}
  [cfg]
  (reify
    IHaltable
    (halt [_] ...)))
```

All stoppable components implement `IHaltable`. The `defcomponent` macro handles both init and halt registration.

### Service Bootstrap (`afrolabs.service`)

`as-system` and `as-service` bootstrap a full Integrant system from a `config.edn` file. Config is read via `afrolabs.config/read-config`, which supports Aero with custom tag readers:

Custom readers defined in `afrolabs.config` (not exhaustive):

- `#ref [:component-key :attribute]` — reference a specific attribute from another component's config map
- `#ig/ref` — Integrant ref (for use inside Aero config files)
- `#parameter` — env var or Java system property
- `#option` — optional parameter (nil if missing)
- `#long?` / `#int?` — parse numeric strings
- `#regex` — compile a regex pattern
- `#csv-array` — comma-separated string to vector
- `#edn` — parse an EDN string
- `#ip-hostname` — validate hostname/IP
- `#ec2/instance-identity-data` — fetch EC2 instance metadata
- `#bool` — coerce string to boolean
- `#not` — boolean negation

### Key Components

**Health (`afrolabs.components.health`):** Manages service lifecycle. Implements `IServiceHealthTripSwitch` — components can signal unhealthy state. Handles `SIGINT`/`SIGTERM` via `beckon`. `wait-while-healthy` blocks until shutdown.

**HTTP (`afrolabs.components.http`):** http-kit server with reitit routing. Components participate via `IHttpRequestHandler` (for routes) and `IRingMiddlewareProvider` (for middleware). Request IDs are injected into every request and the Timbre logging context.

**Kafka (`afrolabs.components.kafka`):** Consumer, producer, and admin client wrappers. Connection strategy is configured via the `defstrategy` macro — strategies implement `IUpdateConsumerConfigHook`, `IUpdateProducerConfigHook`, or `IUpdateAdminClientConfigHook`. Built-in strategies include `ConfluentCloud`, `AdhocConfig`, `MSK`. Multiple strategies can be composed. Available serialization formats: JSON, EDN, Transit, Nippy, Bytes, Dynamic JSON, Schema Registry–compatible.

**AWS (`afrolabs.components.aws`):** Cognitect AWS SDK wrappers for SNS, SQS, S3, SSO, STS, Cognito, CloudWatch Logs. `backoff-and-retry-on-rate-exceeded` macro handles AWS throttling. EC2 instance metadata is available via `afrolabs.config`.

**Prometheus (`afrolabs.prometheus`):** iapetos-based metrics. `register-metric` macro registers metrics with a registry component. The HTTP metrics endpoint is a separate component providing `/metrics`.

**Logging (`afrolabs.logging`):** Timbre with structured context. Use `log/with-context+` to attach key-value pairs to all log calls within a scope. JSON appender support for cloud logging.

### Extension Protocols

Protocols use `:extend-via-metadata true` throughout, enabling lightweight extension without `reify`:

```clojure
(def my-handler
  (with-meta {}
    {`IHttpRequestHandler/ring-handler (fn [_] my-ring-fn)}))
```

### Async / Stream Processing (`afrolabs.csp`)

`core.async` channels with transducer support. `partition-by-interval` provides time-windowed batching. xforms transducers are used extensively in Kafka consumers for stream processing.

### Utilities

- `afrolabs.utils`: `condas->` (conditional threading with binding), `opt-assoc` (optional map update), `time` (log execution duration)
- `afrolabs.transit`: Transit read/write helpers
- `afrolabs.spec`: `assert!` helper
- `afrolabs.user`: REPL helpers (loaded automatically in dev)

## Kafka Serializers Require AOT

The Kafka serializers (`json-serdes`, `edn-serdes`, `bytes-serdes`, `transit-serdes`, `nippy`, `schema-registry-compatible-serdes`) and `spicyrun.ninja.lambda.runtime` **must be AOT-compiled** to `.class` files. The `:deps/prep-lib` key in `deps.edn` ensures `compile-aot` runs when the library is used as a dependency.
