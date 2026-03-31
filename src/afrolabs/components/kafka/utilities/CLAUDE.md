# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

The Kafka utilities (`afrolabs.components.kafka.utilities` and its sub-namespaces) are a collection of functions intended primarily for **interactive/REPL use** and **ad-hoc integrant systems**. They wrap the lower-level Kafka component primitives into convenient, self-contained operations.

## Structure

- `utilities.clj` — the main public API; all top-level functions here
- `utilities/healthcheck.clj` — `make-fake-health-trip-switch`: creates a minimal `IServiceHealthTripSwitch` backed by a promise, for use in ad-hoc integrant systems that need to signal "done" rather than "unhealthy"
- `utilities/topic_forwarder.clj` — `create-system-config`: builds an integrant system config for forwarding messages between two Kafka clusters; used internally by `forward-topics-between-clusters-2`

## Functions

**Reading data**
- `load-messages-from-confluent-topic` — Consumes from one or more topics until caught up to current offsets. Supports `:nr-msgs` limit, `:msg-filter`, time-range filtering via `:from-timestamp`/`:to-timestamp`, and streaming via `:stream-ch`. Returns a vector of messages (or nil if `collect-messages?` is false).
- `load-ktable` — Starts a KTable component and returns an `IDeref`+`IHaltable`. Deref to get current state; halt to stop.

**Inspecting clusters**
- `list-all-topics` — Returns a set of topic name strings.
- `describe-topics` — Returns partition/leader/replica info for given topics.
- `describe-consumer-groups` — Returns consumer group membership and state.
- `describe-acls` — Returns ACL bindings from the cluster.

**Mutating clusters**
- `assert-topics` — Ensures topics exist with the given partition count (creates if missing).
- `alter-topic-config` — Increases partition count for a topic.
- `delete-some-topics-on-cluster!` — Deletes topics matching a predicate. Never deletes internal topics (those starting with `_`). Supports `:dry-run?`.
- `delete-all-topics-on-cluster!` — Convenience wrapper over `delete-some-topics-on-cluster!` with `(constantly true)`.

**Producing**
- `produce-and-wait!` — Produces a batch of messages and blocks until all delivery acks are received.

**Forwarding between clusters**
- `forward-topics-between-clusters` — REPL-use version. Takes `src-cluster-cfg` and `dest-cluster-cfg` maps and returns an `IHaltable`.
- `forward-topics-between-clusters-2` — Component-friendly version; accepts an optional `:health-component` to integrate with an existing health trip-switch. Logs forwarding throughput every 30 seconds.
- `copy-topic` — Copies all data from one topic to another on the same cluster (byte-level copy, preserves key/value only). Blocks until caught up.

## Common Parameter Patterns

Most functions accept a `:bootstrap-server` string and optionally `:confluent-api-key`/`:confluent-api-secret` (both must be non-nil to activate the `ConfluentCloud` strategy). Additional strategies can be injected via `:extra-strategies`.

Config is typically loaded with `(afrolabs.config/load-config ".env-prod")` and destructured as `{:keys [kafka-bootstrap-server confluent-api-key confluent-api-secret]}`.

The `src-cluster-cfg` / `dest-cluster-cfg` maps used by the forwarding functions are plain maps with `:bootstrap-server` and `:strategies`, matching the spec in `topic_forwarder.clj`.

### Serialization strategies

Strategies are passed as vectors in `:extra-strategies`. Common combinations:

```clojure
;; JSON values, string keys
[[:strategy/JsonSerializer :consumer :value]
 [:strategy/StringSerializer :consumer :key]]

;; Raw bytes for both (useful when value needs manual decoding, e.g. gzipped)
[[:strategy/ByteArraySerializer :consumer :both]]

;; JSON values, raw bytes for key
[[:strategy/JsonSerializer :consumer :value]
 [:strategy/ByteArraySerializer :consumer :key]]
```

## Usage Examples

### Collect all messages from a topic

```clojure
(def records
  (-kafka-utils/load-messages-from-confluent-topic
    :bootstrap-server  kafka-bootstrap-server
    :api-key           confluent-api-key
    :api-secret        confluent-api-secret
    :topics            ["my-topic"]
    :extra-strategies  [[:strategy/JsonSerializer :consumer :value]
                        [:strategy/StringSerializer :consumer :key]]
    :collect-messages? true
    :from-timestamp    (t/- (t/instant) (t/days 1))
    :to-timestamp      (t/instant)))
```

### Stream messages into an accumulator (avoid holding all data in memory)

Use `:collect-messages? false` with `:stream-ch`. The channel receives batches (vectors) of messages and is closed when consumption is complete. Process on a separate `csp/thread` and block on its result:

```clojure
(let [stream-ch   (csp/chan)
      accumulator (csp/thread
                    (loop [result (transient {})]
                      (if-let [msgs (csp/<!! stream-ch)]
                        (recur (reduce (fn [acc msg]
                                         (assoc! acc (get (:value msg) "vehicleId") msg))
                                       result msgs))
                        (persistent! result))))]
  (-kafka-utils/load-messages-from-confluent-topic
    :bootstrap-server  kafka-bootstrap-server
    :api-key           confluent-api-key
    :api-secret        confluent-api-secret
    :topics            ["my-topic"]
    :extra-strategies  [[:strategy/JsonSerializer :consumer :value]
                        [:strategy/StringSerializer :consumer :key]]
    :collect-messages? false
    :stream-ch         stream-ch
    :from-timestamp    start-instant
    :to-timestamp      to-instant)
  (csp/<!! accumulator))  ;; block until thread finishes, returns result
```

The `:value-converter-fn` passed to higher-level helpers like `topic->last-by-vehicle-id` must return a **sequence** of records (not a single record), because `mapcat` is applied to its results. For topics where each Kafka message contains multiple logical records, expand them here:

```clojure
:value-converter-fn (fn [msg]
                      (mapcat #(get % "deviceList")
                              (-> (String. ^bytes (:value msg))
                                  json/read-str
                                  json/read-str)))
```

For topics where each message is a single record, wrap in a vector or use `:value` as a shorthand:

```clojure
:value-converter-fn :value   ;; returns the :value field directly (treated as a single-element seq)
```

## Ad-hoc Integrant Systems

Several functions here spin up their own inline `ig/init` / `ig/halt!` systems rather than relying on an outer integrant context. They use `make-fake-health-trip-switch` (from `utilities.healthcheck`) to drive shutdown: a `promise` is delivered when consumption is complete, which triggers `ig/halt!`.

## Timbre logging macros: never embed side effects

Timbre's `log/debug`, `log/trace`, etc. are **macros** that skip argument evaluation entirely when the active log level is above the macro's level. This means any side-effectful expression (e.g. `swap!`, `reset!`, `deliver`) nested inside a log call is silently a no-op in production.

```clojure
;; WRONG — swap! is never called at :info log level
(log/debug [:remaining (swap! my-atom f)])

;; CORRECT — always execute the side effect, then log the result
(let [result (swap! my-atom f)]
  (log/debug [:remaining result]))
```

`log/spy` is the one exception: it **always** evaluates its form regardless of level, and only conditionally emits the log line. It is safe to use for side effects, though it's cleaner to keep side effects explicit.

This bug caused `load-messages-from-confluent-topic` with `:to-timestamp` to hang indefinitely in production (`:info` level) because the atom tracking remaining partitions was never updated.
