# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`afrolabs.components.kafka` is the Kafka namespace. It defines the three core Integrant components (`::kafka-consumer`, `::kafka-producer`, `::admin-client`) and the full strategy system that configures them. Everything from serdes to topic subscription to offset management is a strategy.

## Integrant Components

All three support `:disabled? true` in their config maps.

| Component keyword | Factory fn | Config spec |
|---|---|---|
| `::kafka-consumer` | `make-consumer` | `::consumer-config` |
| `::kafka-producer` | `make-producer` | `::producer-config` |
| `::admin-client` | `make-admin-client` | `::admin-client-cfg` |
| `::ktable` | (inline) | `::ktable-cfg` |
| `::topic-asserter` | (inline) | `::topic-asserter-cfg` |
| `::ktable-asserter` | (inline) | `::ktable-asserter-cfg` |
| `::list-of-topics` | (inline) | `::list-of-topics-cfg` |

The factory functions (`make-consumer`, `make-producer`, `make-admin-client`) can also be called directly for ad-hoc / REPL use, outside of Integrant.

### Consumer config shape

```clojure
{:bootstrap-server            "broker:9092"
 :service-health-trip-switch  health-component  ;; required
 :consumer/client             consumer-client   ;; required — implements IConsumerClient
 :strategies                  [...]             ;; optional
 :consumer.poll/timeout       1000}             ;; optional, ms
```

### Producer config shape

```clojure
{:bootstrap-server "broker:9092"
 :strategies       [...]}
```

### `IConsumerClient`

The single protocol a consumer caller must implement:

```clojure
(reify IConsumerClient
  (consume-messages [_ msgs]
    ;; msgs is a vector of maps: {:topic :partition :offset :key :value :timestamp :headers}
    ;; return value: nil, or a collection of producer message maps to be forwarded
    ))
```

## Strategy System

Strategies are the primary extension point. A strategy is an object that may implement one or more of these protocols:

| Protocol | Effect |
|---|---|
| `IUpdateConsumerConfigHook` | Modifies the raw Kafka consumer properties map before the consumer is created |
| `IUpdateProducerConfigHook` | Same for producer |
| `IUpdateAdminClientConfigHook` | Same for admin client |
| `IConsumerInitHook` | Called with the `Consumer` object before the first `.poll` (used for `.subscribe`) |
| `IConsumerPostInitHook` | Called after `IConsumerInitHook`, before first `.poll` (used for seeking) |
| `IPostConsumeHook` | Called after each `.poll` cycle with the consumer and consumed records |
| `IShutdownHook` | Called when the consumer poll loop exits |
| `IConsumerMiddleware` | Intercepts messages between the poll loop and `IConsumerClient` |
| `IConsumerMessagesPreProcessor` | Intercepts and mutates messages before `consume-messages` is called |
| `IConsumedResultsHandler` | Handles the return value of `consume-messages` (e.g. to forward to a producer) |
| `IProducerPreProduceMiddleware` | Intercepts messages before `.send` |

The `defstrategy` macro defines both a constructor function and a `create-strategy` multimethod dispatch, so strategies can be instantiated either as function calls or as keyword vectors in config files:

```clojure
;; function call form (Clojure code)
(JsonSerializer :consumer :value)

;; keyword vector form (config.edn / strategies list)
[:strategy/JsonSerializer :consumer :value]
```

`normalize-strategies` converts a mixed seq of either form into protocol-implementing objects.

## Built-in Strategies (not exhaustive)

**Serialization** — all accept `:consumer` and/or `:producer` with values `:key`, `:value`, or `:both`:
- `StringSerializer` — `org.apache.kafka.common.serialization.String*`
- `JsonSerializer` — AOT-compiled `json_serdes`; accepts `:deserialize-keys-as-keyword? true`
- `EdnSerializer` — AOT-compiled `edn_serdes`; accepts `:parse-inst-as-java-time true`
- `ByteArraySerializer` — raw byte arrays, AOT-compiled `bytes_serdes`
- `TransitSerializer` — AOT-compiled `transit_serdes`
- `NippySerializer` — AOT-compiled nippy

**Subscription:**
- `SubscribeWithTopicsCollection [topics]` — subscribe to a literal list of topic strings
- `SubscribeWithTopicsRegex [regex]` — subscribe by regex pattern
- `SubscribeWithTopicNameProvider :topic-name-providers [...]` — subscribe via `ITopicNameProvider` objects

**Offset control:**
- `OffsetReset "earliest"|"latest"` — sets `auto.offset.reset`
- `AutoCommitOffsets` — enables auto-commit (default 30s interval)
- `FreshConsumerGroup` — generates a random UUID consumer group
- `ConsumerGroup consumer-group-id` — sets a fixed consumer group
- `SeekToTimestampOffset offset` — seeks all partitions to a timestamp after partition assignment
- `SeekToPartitionOffset topic-partition-offsets` — seeks to specific partition offsets

**Caught-up detection:**
- `CaughtUpNotifications & chs` — sends current offsets to provided channel(s) whenever the consumer is fully current (runs on every poll); multiple fire events
- `CaughtUpOnceNotifications & chs` — fires once when the consumer catches up to the offsets that existed when it started

**Performance:**
- `HighThroughput` — bumps fetch sizes, poll records, batch size, linger, buffer, compression
- `ProducerBatching :batch-size :linger-ms` — tune producer batching independently
- `ProducerCompression [type]` — set producer compression (default `"lz4"`)
- `ReduceRebalanceFrequency` — increases request timeout and heartbeat interval

**Misc:**
- `AdhocConfig & config-pairs` — directly set any valid Kafka config key/value; automatically routes each key to the correct client type (consumer/producer/admin)
- `ClientId client-id` — sets `client.id` on both consumer and producer
- `ConsumerMaxPollRecords n` — sets `max.poll.records`
- `RequestTimeout ms`
- `ProduceConsumerResultsWithProducer producer` — an `IConsumedResultsHandler` that forwards `consume-messages` return values to a producer
- `RenameTopicsForProducer :rename-fn f` — an `IProducerPreProduceMiddleware` that rewrites topic names before producing
- `UpdateConsumedRecordKey :key-fn f` — pre-processor that transforms the `:key` of each consumed message
- `UpdateConsumedRecordValue :value-fn f` — pre-processor that transforms the `:value`

## Topic Management

- `assert-topics!` / `assert-topics` (deprecated) — creates missing topics; optionally increases partition count
- `delete-topics!` — deletes topics that exist
- `::topic-asserter` component — runs `assert-topics!` at system init time
- `::ktable-asserter` component — like `topic-asserter` but also enforces `cleanup.policy` for compacted topics (supports `"compact"`, `"compact,delete"`)
- `::list-of-topics` component — wraps a list of topic strings as an `ITopicNameProvider`, for use as an Integrant ref in `:topic-name-providers`

## KTable (`::ktable`)

A KTable is a consumer that maintains an in-memory snapshot of the latest value per key across its subscribed topics. It implements both `IDeref` (returns current state map) and `IKTable`:

- `ktable-wait-for-catchup` — blocks until the ktable has consumed up to specified topic-partition offsets
- `ktable-wait-until-fully-current` — blocks until the ktable is at the latest offset
- `ktable-subscribe / ktable-unsubscribe` — subscribe to the stream of processed messages with an optional transducer

KTable config requires `::ktable-id` (a unique string) and a `:clock` component (from `afrolabs.components.time`). Optionally supports `::ktable-checkpoint-storage` to persist and resume from offset checkpoints.

## `redeclare-*` Macros

Every `defcomponent` generates a `redeclare-<component-name>` macro that re-registers the same init/halt logic under a new Integrant keyword. This allows multiple independent instances of the same component type in one Integrant system:

```clojure
(ns my-app
  (:require [afrolabs.components.kafka :as -kafka]))

;; register ::my-app/consumer-a and ::my-app/consumer-b
;; as two independent kafka-consumer instances
(-kafka/redeclare-kafka-consumer ::my-app/consumer-a)
(-kafka/redeclare-kafka-consumer ::my-app/consumer-b)
```

## Producer Message Map

```clojure
{:topic     "my-topic"   ;; required
 :value     ...          ;; required
 :key       ...          ;; optional
 :headers   {...}        ;; optional, map of string -> any
 :delivered-ch (csp/chan)} ;; optional — receives delivery ack or exception
```

Produce via `(produce! producer-component [msg1 msg2 ...])`.

## Configuration Patterns in config.edn

The following patterns appear consistently across production config files.

### Shared Kafka config via `#ref`

Rather than repeating bootstrap-server and auth strategy in every component, a single plain-data config component is used as a shared reference. Note the use of `#ref` (access a value inside a component's config map) vs `#ig/ref` (reference the component object itself):

```clojure
;; Define once
:my-app/common-kafka-config
{:bootstrap-server #option KAFKA_BOOTSTRAP_SERVER
 :auth/strategy    [:strategy/ConfluentCloud
                    :api-key    #option CONFLUENT_API_KEY
                    :api-secret #option CONFLUENT_API_SECRET]}

;; Reference in consumers, producers, admin clients
:my-app/my-producer
{:bootstrap-server #ref [:my-app/common-kafka-config :bootstrap-server]
 :strategies       [#ref [:my-app/common-kafka-config :auth/strategy]
                    [:strategy/EdnSerializer :producer :value]]}
```

### Auth provider selection with `#config/case`

When supporting multiple auth backends (Confluent Cloud, MSK IAM, MSK SCRAM), `#config/case` selects both the bootstrap server and the auth strategy based on an env variable:

```clojure
:my-app/common-kafka-config
{:bootstrap-server #config/case [#or [#option KAFKA_AUTH_PROVIDER "Confluent"]
                                  "Confluent" #option CONFLUENT_BOOTSTRAP_SERVER
                                  "MskIam"    #option MSK_IAM_BOOTSTRAP_SERVER
                                  "MskScram"  #option MSK_SCRAM_BOOTSTRAP_SERVER]
 :auth/strategy    #config/case [#or [#option KAFKA_AUTH_PROVIDER "Confluent"]
                                  "Confluent" [:strategy/ConfluentCloud
                                               :api-key    #option CONFLUENT_API_KEY
                                               :api-secret #option CONFLUENT_API_SECRET]
                                  "MskIam"    [:strategy/AwsMskIam]
                                  "MskScram"  [:strategy/AwsMskScram
                                               :aws.msk/username #option MSK_SCRAM_USERNAME
                                               :aws.msk/password #option MSK_SCRAM_PASSWORD]]}
```

### Two-asserter pattern: config topics vs state topics

In practice, applications use two separate `::ktable-asserter` components because config topics and state topics need different compaction policies:

```clojure
;; Shared compaction tuning — reference this from both asserters
:my-app/common-ktable-config
{:topic-delete-retention-ms        86400000   ;; tombstones kept 24 hours
 :ktable-segment-ms                43200000   ;; segments max 12 hours
 :ktable-max-compaction-lag-ms     604800000  ;; compact at least weekly
 :ktable-min-cleanable-dirty-ratio 0.2}

;; Config ktables: pure "compact" — records are valid forever until replaced
:my-app/ktable-asserter:config
{:bootstrap-server              #ref [:my-app/common-kafka-config :bootstrap-server]
 :strategies                    [#ref [:my-app/common-kafka-config :auth/strategy]]
 :update-topics-with-bad-config true
 :topic-name-providers          [#ig/ref :my-app/config-topic-provider]
 :topic-delete-retention-ms     #ref [:my-app/common-ktable-config :topic-delete-retention-ms]
 :ktable-segment-ms             #ref [:my-app/common-ktable-config :ktable-segment-ms]
 :ktable-max-compaction-lag-ms  #ref [:my-app/common-ktable-config :ktable-max-compaction-lag-ms]
 :ktable-min-cleanable-dirty-ratio #ref [:my-app/common-ktable-config :ktable-min-cleanable-dirty-ratio]}
 ;; :ktable-compaction-policy defaults to "compact"

;; State ktables: "compact,delete" — records expire after retention period
:my-app/ktable-asserter:state
{:bootstrap-server                 #ref [:my-app/common-kafka-config :bootstrap-server]
 :strategies                       [#ref [:my-app/common-kafka-config :auth/strategy]]
 :update-topics-with-bad-config    true
 :increase-existing-partitions?    true
 :ktable-compaction-policy         "delete,compact"
 :topic-name-providers             [#ig/ref :my-app/state-topic-a-provider
                                    #ig/ref :my-app/state-topic-b-provider]
 :topic-delete-retention-ms        #ref [:my-app/common-ktable-config :topic-delete-retention-ms]
 ;; ... other compaction refs
 }
```

### Producer-consumer pipeline (consume → transform → produce)

Use `ProduceConsumerResultsWithProducer` in the consumer's `:strategies` to wire a producer as the sink. The consumer's `IConsumerClient` returns messages to be produced; the strategy handles the actual `produce!` call:

```clojure
:my-app/my-producer
{:bootstrap-server #ref [:my-app/common-kafka-config :bootstrap-server]
 :strategies       [#ref [:my-app/common-kafka-config :auth/strategy]
                    [:strategy/EdnSerializer :producer :value]
                    [:strategy/StringSerializer :producer :key]
                    [:strategy/ClientId "my-service"]
                    [:strategy/ProducerCompression "lz4"]
                    [:strategy/ProducerBatching :linger-ms 500]]}

:my-app/my-consumer
{:bootstrap-server           #ref [:my-app/common-kafka-config :bootstrap-server]
 :consumer/client            #ig/ref :my-app/my-consumer-client
 :service-health-trip-switch #ig/ref :afrolabs.components.health/component
 :strategies                 [[:strategy/ByteArraySerializer :consumer :both]
                               [:strategy/SubscribeWithTopicNameProvider
                                :topic-name-providers [#ig/ref :my-app/source-topics]]
                               [:strategy/OffsetReset "latest"]
                               [:strategy/ConsumerGroup "my-consumer-group"]
                               [:strategy/ClientId "my-service"]
                               #ref [:my-app/common-kafka-config :auth/strategy]
                               [:strategy/AutoCommitOffsets :commit-interval-ms 5000]
                               [:strategy/ReduceRebalanceFrequency
                                :request-timeout-ms 45000
                                :heartbeat-interval-ms 15000]
                               [:strategy/HighThroughput
                                :consumer-fetch-min-bytes 100000
                                :consumer-max-poll-records 1000]
                               [:strategy/ProduceConsumerResultsWithProducer
                                #ig/ref :my-app/my-producer]]}
```

### KTable with startup blocking and checkpoint storage

Set `:caught-up-once? true` so the component blocks until it has consumed up to the current end offset before declaring itself ready. Add `:ktable-checkpoint-storage` to resume from a persisted offset instead of replaying from the start:

```clojure
:my-app/my-ktable
{:ktable-id                  "my-ktable-id"
 :caught-up-once?            true
 :bootstrap-server           #ref [:my-app/common-kafka-config :bootstrap-server]
 :clock                      #ig/ref :afrolabs.components.time/system-time
 :strategies                 [#ref [:my-app/common-kafka-config :auth/strategy]
                               [:strategy/SubscribeWithTopicNameProvider
                                :topic-name-providers [#ig/ref :my-app/my-topic-provider]]
                               [:strategy/StringSerializer :consumer :key]
                               [:strategy/EdnSerializer :consumer :value
                                :parse-inst-as-java-time true]
                               [:strategy/HighThroughput
                                :consumer-fetch-min-bytes 1000000
                                :consumer-max-poll-records 10000]]
 :service-health-trip-switch #ig/ref :afrolabs.components.health/component
 :ktable-checkpoint-storage  #ig/ref :my-app/ktable-checkpoint-store
 :ktable-asserter            #ig/ref :my-app/ktable-asserter:state}
```

### Startup ordering via dependency keys

Consumers accept `:wait-for-topics-to-be-asserted` (and some components use `:create-topics-before-using-them`) to ensure topic asserters have run before the consumer starts polling. These keys are not part of the consumer's logic — they exist purely to establish an Integrant init-order dependency:

```clojure
:my-app/my-consumer
{;; ... normal consumer config ...
 :wait-for-topics-to-be-asserted [#ig/ref :my-app/ktable-asserter:state
                                   #ig/ref :my-app/topic-asserter]}
```

## Utility Functions

- `->millis-from-epoch x` — converts integer, string (ISO-8601), `Instant`, or `ZonedDateTime` to epoch millis
- `consumer-end-offsets consumer` — returns current end offsets as a set of `{:topic :partition :offset}` maps
- `consumer-current-offsets consumer` — returns current consumer position in the same format
- `consumer:fully-current? consumer timeout timeout-value` — returns current offsets if consumer is fully caught up, else nil
- `msgs->topic-partition-maxoffsets msgs` — helper to find the max offset per topic-partition from a batch of consumed records
