# kafka-consumer-registry

## NAME

`afrolabs.components.kafka.consumer-registry` — registry of which process
owns each `[consumer-group-id topic partition]`.

## SYNOPSIS

```clojure
(require '[afrolabs.components.kafka.consumer-registry :as -registry])

;; integrant key
:afrolabs.components.kafka.consumer-registry/component

;; additional instances
(-registry/redeclare-component ::my-registry)

;; all advertisements
@registry
```

## DESCRIPTION

Advertises the partitions assigned to this process's consumers, and
returns the advertisements of every process sharing the registry topic.

Register a consumer by passing the component in its subscribe strategy's
`:consumer-rebalance-listeners`. One registry serves any number of
consumers. Registered consumers must not set `group.instance.id`.

A partition's entry names its most recent owner. Entries are not removed
when a partition is revoked or a process stops; they are replaced by the
next owner, or disappear 90 minutes after their last refresh.

An entry overwritten by a former owner is rewritten by the current owner as
soon as the current owner reads it.

The registry topic is created on start. An existing topic with a different
configuration is updated.

Initialisation blocks until the registry has loaded.

## CONFIGURATION

Required:

`:bootstrap-server`
: Kafka bootstrap servers.

`:registry-topic`
: Name of the registry topic. Non-empty string. Processes that should see
  each other must use the same topic.

`:advertised-url`
: URL other processes use to reach this process. Non-empty string.
  Published verbatim.

`:clock`
: Clock component, e.g. `:afrolabs.components.time/system-time`.

`:service-health-trip-switch`
: Health component, e.g. `:afrolabs.components.health/component`.

Optional:

`:strategies`
: Connection strategies (`ConfluentCloud`, `MSK`, `AdhocConfig`, ...). Do
  not pass serializer or subscribe strategies. Default `[]`.

`:admin-client`
: Admin client component. Created internally when absent.

## DEREF

```clojure
@registry
;=> {[consumer-group-id topic partition] advertisement}
```

Return the advertisements refreshed within the last 90 minutes. Does not
block.

advertisement:

| key           | value                                              |
|---------------|----------------------------------------------------|
| `:url`        | `:advertised-url` of the owning process            |
| `:process-id` | UUID of the owning process, new on every start     |
| `:member-id`  | consumer group member id of the owner              |
| `:generation` | consumer group generation of the assignment        |

## METRICS

`::publish-attempts{outcome}`
: Counter. Advertisement publishes, `outcome` is `ok` or `failed`.

`::last-publish-success`
: Gauge. Epoch seconds of the last successful publish.

`::corrections{outcome}`
: Counter. Owned entries found overwritten by another process. `outcome`
  is `corrected` (rewritten) or `deferred` (the other process has a newer
  group generation; this consumer has likely been removed from its group).

## EXAMPLES

### Register a consumer

`config.edn`:

```clojure
{:afrolabs.components.time/system-time {}

 :afrolabs.components.health/component {}

 :afrolabs.components.kafka.consumer-registry/component
 {:bootstrap-server           #parameter KAFKA_BOOTSTRAP_SERVER
  :registry-topic             "consumer-registry"
  :advertised-url             #parameter ADVERTISED_URL
  :clock                      #ig/ref :afrolabs.components.time/system-time
  :service-health-trip-switch #ig/ref :afrolabs.components.health/component}

 :my.service/orders-consumer
 {:bootstrap-server           #parameter KAFKA_BOOTSTRAP_SERVER
  :service-health-trip-switch #ig/ref :afrolabs.components.health/component
  :consumer/client            #ig/ref :my.service/orders-client
  :strategies                 [[:strategy/EdnSerializer :consumer :both]
                               [:strategy/ConsumerGroup "orders"]
                               [:strategy/SubscribeWithTopicsCollection
                                ["orders"]
                                [#ig/ref :afrolabs.components.kafka.consumer-registry/component]]]}}
```

`:my.service/orders-consumer` is a redeclared
`:afrolabs.components.kafka/kafka-consumer`.

### Confluent Cloud

```clojure
:afrolabs.components.kafka.consumer-registry/component
{:bootstrap-server           #parameter KAFKA_BOOTSTRAP_SERVER
 :registry-topic             "consumer-registry"
 :advertised-url             #parameter ADVERTISED_URL
 :clock                      #ig/ref :afrolabs.components.time/system-time
 :service-health-trip-switch #ig/ref :afrolabs.components.health/component
 :admin-client               #ig/ref :afrolabs.components.kafka/admin-client
 :strategies                 [[:strategy/ConfluentCloud
                               :api-key    #parameter KAFKA_API_KEY
                               :api-secret #parameter KAFKA_API_SECRET]]}
```

### Look up a partition's advertisement

```clojure
(get @registry ["orders" "orders" 3])
;=> {:url        "http://10.0.4.17:8080"
;    :process-id #uuid "6f1c..."
;    :member-id  "orders-consumer-1-9b2e..."
;    :generation 42}
```

### All processes serving a consumer group

```clojure
(->> @registry
     (filter (fn [[[group-id _ _] _]] (= "orders" group-id)))
     (map (comp :url val))
     distinct)
;=> ("http://10.0.4.17:8080" "http://10.0.4.23:8080")
```

## CAVEATS

An entry may name a process that no longer owns the partition: the new
owner's advertisement may not be loaded yet, a former owner may have
overwritten it and not yet been corrected, or the old process may have
stopped without a successor.

A partition revoke may take up to 40 seconds longer while pending
advertisements are written.

A registered consumer with static group membership (`group.instance.id`)
throws on its first partition assignment; its service becomes unhealthy.

## SEE ALSO

`afrolabs.components.kafka` — `SubscribeWithTopicsCollection`,
`SubscribeWithTopicNameProvider`, `IConsumerAwareRebalanceListener`.

[kafka-topic-asserters](kafka-topic-asserters.md)
