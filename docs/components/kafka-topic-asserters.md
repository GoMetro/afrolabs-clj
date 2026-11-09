# kafka-topic-asserters

## NAME

`:afrolabs.components.kafka/topic-asserter`,
`:afrolabs.components.kafka/ktable-asserter` — ensure Kafka topics exist
with the desired partitions and topic config.

## SYNOPSIS

```clojure
(require '[afrolabs.components.kafka :as -kafka])

;; integrant keys
:afrolabs.components.kafka/topic-asserter
:afrolabs.components.kafka/ktable-asserter

;; additional instances
(-kafka/redeclare-topic-asserter ::my-topics)
(-kafka/redeclare-ktable-asserter ::my-ktable-topics)

;; as a function
(-kafka/assert-topics! admin-client ["orders" "payments"]
                       :nr-of-partitions              12
                       :topic-config                  {"retention.ms" 604800000}
                       :update-topics-with-bad-config true)
```

## DESCRIPTION

Asserts the topics named by `:topic-name-providers` at initialisation.

Missing topics are created with `:nr-of-partitions`, the cluster default
replication factor, and the effective topic config.

Existing topics are compared with the effective topic config. With
`:update-topics-with-bad-config` true, differing keys are set on the topic;
otherwise each difference is logged at info and the topic is left
unchanged. Keys not in the effective config are never changed.

`ktable-asserter` is `topic-asserter` for topics backing ktables: it always
sets `cleanup.policy`, and refuses to start on a topic with the wrong
`cleanup.policy` unless it may update it.

The component value is its config map. Reference it from consumers to
order startup after the topics exist.

## CONFIGURATION

Connection, one of:

`:admin-client`
: Admin client component.

`:bootstrap-server`, `:strategies`
: Kafka bootstrap servers and connection strategies. An admin client is
  created for initialisation and closed after.

Required:

`:topic-name-providers`
: Collection of `ITopicNameProvider`, e.g. `:afrolabs.components.kafka/list-of-topics`.

Optional, both asserters:

`:topic-config`
: Map of Kafka topic config name to value. String keys, values string,
  number or boolean. Default `{}`.

`:update-topics-with-bad-config`
: Boolean. Set differing config on existing topics. Default `false`.

`:nr-of-partitions`
: Partitions for new topics. Integer or numeric string. Default: cluster
  default.

`:increase-existing-partitions?`
: Boolean. Raise existing topics' partitions to `:nr-of-partitions`.
  Partitions are never reduced. Default `false`.

`:disabled?`
: Boolean. Skip the assertion.

Optional, `ktable-asserter` only:

`:ktable-compaction-policy`
: `"compact"`, `"compact,delete"` or `"delete,compact"`. Sets
  `cleanup.policy`. Default `"compact"`.

Deprecated, `ktable-asserter` only. Use `:topic-config`:

| option                              | topic config                 |
|-------------------------------------|------------------------------|
| `:topic-delete-retention-ms`        | `delete.retention.ms`        |
| `:ktable-segment-ms`                | `segment.ms`                 |
| `:ktable-min-cleanable-dirty-ratio` | `min.cleanable.dirty.ratio`  |
| `:ktable-max-compaction-lag-ms`     | `max.compaction.lag.ms`      |
| `:recreate-topics-with-bad-config`  | — alias of `:update-topics-with-bad-config` |

## EFFECTIVE TOPIC CONFIG

`topic-asserter`
: `:topic-config`.

`ktable-asserter`
: `:topic-config`, then each deprecated option set, then `cleanup.policy`
  from `:ktable-compaction-policy`. Later entries win.

`cleanup.policy` values compare without regard to order:
`"delete,compact"` equals `"compact,delete"`.

## EXAMPLES

### Topics with a retention period

```clojure
:my-app/event-topics
{:topics ["orders" "payments"]}

:my-app/topic-asserter
{:bootstrap-server              #ref [:my-app/common-kafka-config :bootstrap-server]
 :strategies                    [#ref [:my-app/common-kafka-config :auth/strategy]]
 :topic-name-providers          [#ig/ref :my-app/event-topics]
 :nr-of-partitions              12
 :topic-config                  {"retention.ms" 604800000}
 :update-topics-with-bad-config true}
```

`:my-app/event-topics` is a redeclared `:afrolabs.components.kafka/list-of-topics`.

### State ktables that expire

```clojure
:my-app/ktable-asserter:state
{:admin-client                  #ig/ref :afrolabs.components.kafka/admin-client
 :topic-name-providers          [#ig/ref :my-app/state-topics]
 :ktable-compaction-policy      "compact,delete"
 :topic-config                  {"retention.ms"              86400000
                                 "segment.ms"                3600000
                                 "delete.retention.ms"       86400000
                                 "min.cleanable.dirty.ratio" 0.2}
 :update-topics-with-bad-config true}
```

### Order a consumer after its topics

```clojure
:my-app/orders-consumer
{;; ... consumer config ...
 :wait-for-topics-to-be-asserted [#ig/ref :my-app/topic-asserter]}
```

## CAVEATS

`ktable-asserter` with `:update-topics-with-bad-config` false throws at
initialisation if an existing topic's `cleanup.policy` differs.

With `:update-topics-with-bad-config` true, initialisation throws if the
brokers reject a config change.

Values compare as strings. A value the broker reports in another form,
e.g. a ratio written as `0.20`, is reported as different on every start.

Replication factor is always the cluster default.

## SEE ALSO

`afrolabs.components.kafka` — `ITopicNameProvider`, `list-of-topics`,
`admin-client`, `ktable`.

[kafka-consumer-registry](kafka-consumer-registry.md)
