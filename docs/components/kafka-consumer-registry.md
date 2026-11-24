# kafka-consumer-registry

## NAME

`afrolabs.components.kafka.consumer-registry` — registry of which process
owns each `[consumer-group-id topic partition]`, and queries addressed to
every process in a consumer group.

## SYNOPSIS

```clojure
(require '[afrolabs.components.kafka.consumer-registry :as -registry])

;; integrant key
:afrolabs.components.kafka.consumer-registry/component

;; additional instances
(-registry/redeclare-component ::my-registry)

;; all advertisements
@registry

;; ask every process in a consumer group
(-registry/query registry {:group-id "orders" :query {:order-id 17}})

;; answer queries, in a consumer's :strategies
[:strategy/ConsumerRegistryQueryHandler registry handler]
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

`query` asks every process advertising a consumer group and returns their
answers. Each process passes the query to every handler registered for that
group in it. Attach the component to an HTTP server's `:handlers` to
receive queries.

## CONFIGURATION

Required:

`:bootstrap-server`
: Kafka bootstrap servers.

`:registry-topic`
: Name of the registry topic. Non-empty string. Processes that should see
  each other must use the same topic.

`:advertised-url`
: Base URL other processes use to reach the HTTP server this component is
  attached to. Non-empty string. Published verbatim.

`:query-path`
: Path queries are served on. Starts with `/`, does not end with `/`, e.g.
  `"/consumer-registry/query"`.

`:query-token`
: Shared secret. Queries without it are rejected. Non-empty string.

`:clock`
: Clock component, e.g. `:afrolabs.components.time/system-time`.

`:service-health-trip-switch`
: Health component, e.g. `:afrolabs.components.health/component`.

Optional:

`:query-timeout-ms`
: Default `query` timeout. Positive integer. Default `20000`.

`:strategies`
: Connection strategies (`ConfluentCloud`, `MSK`, `AdhocConfig`, ...). Do
  not pass serializer or subscribe strategies. Default `[]`.

`:admin-client`
: Admin client component. Created internally when absent.

Processes sharing a registry topic must agree on `:query-path` and
`:query-token`.

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

## QUERIES

### query

```clojure
(-registry/query registry {:group-id   "orders"
                           :query      {:order-id 17}
                           :timeout-ms 5000})
;=> [answer ...]
```

Send `:query` to every process advertising `:group-id`. Block until all
have responded or `:timeout-ms` elapses (default `:query-timeout-ms`).

Return a vector of every answer received in time; `[]` if none. A process
that fails or does not respond in time contributes no answers.

### IConsumerGroupQueryHandler

```clojure
(defprotocol IConsumerGroupQueryHandler
  :extend-via-metadata true
  (handle-query [_ group-id query]))
```

Return the answer to `query`, or `nil` for none. Must return within the
sender's timeout. A throw contributes no answer.

### ConsumerRegistryQueryHandler

```clojure
[:strategy/ConsumerRegistryQueryHandler registry handler]
```

Consumer strategy. `handler` receives queries to the consumer's group while
the consumer runs. `handler` must implement `IConsumerGroupQueryHandler`.

### register-query-handler!, deregister-query-handler!

```clojure
(def handle (-registry/register-query-handler! registry "orders" handler))
(-registry/deregister-query-handler! registry handle)
```

Register and remove a handler by hand. Any number of handlers may share a
group; each receives every query to it.

## METRICS

`::publish-attempts{outcome}`
: Counter. Advertisement publishes, `outcome` is `ok` or `failed`.

`::last-publish-success`
: Gauge. Epoch seconds of the last successful publish.

`::corrections{outcome}`
: Counter. Owned entries found overwritten by another process. `outcome`
  is `corrected` (rewritten) or `deferred` (the other process has a newer
  group generation; this consumer has likely been removed from its group).

`::queries-sent`
: Counter. `query` calls.

`::query-responses{status}`
: Counter. Per-process outcomes of `query` calls: the HTTP status, `error`
  (no response) or `timeout`.

`::queries-handled{outcome}`
: Counter. Queries received. `outcome` is `answered`, `no-answer`,
  `no-handler`, `unauthorized` or `bad-request`.

`::query-handler-errors`
: Counter. Handler calls that threw.

## EXAMPLES

### Register a consumer and answer queries

`config.edn`:

```clojure
{:afrolabs.components.time/system-time {}

 :afrolabs.components.health/component {}

 :afrolabs.components.kafka.consumer-registry/component
 {:bootstrap-server           #parameter KAFKA_BOOTSTRAP_SERVER
  :registry-topic             "consumer-registry"
  :advertised-url             #parameter ADVERTISED_URL
  :query-path                 "/consumer-registry/query"
  :query-token                #parameter CONSUMER_REGISTRY_QUERY_TOKEN
  :clock                      #ig/ref :afrolabs.components.time/system-time
  :service-health-trip-switch #ig/ref :afrolabs.components.health/component}

 :my.service/http-internal
 {:handlers [#ig/ref :afrolabs.components.kafka.consumer-registry/component]
  :port     8080
  :ip       "0.0.0.0"}

 :my.service/orders-client {}

 :my.service/orders-consumer
 {:bootstrap-server           #parameter KAFKA_BOOTSTRAP_SERVER
  :service-health-trip-switch #ig/ref :afrolabs.components.health/component
  :consumer/client            #ig/ref :my.service/orders-client
  :strategies                 [[:strategy/EdnSerializer :consumer :both]
                               [:strategy/ConsumerGroup "orders"]
                               [:strategy/SubscribeWithTopicsCollection
                                ["orders"]
                                [#ig/ref :afrolabs.components.kafka.consumer-registry/component]]
                               [:strategy/ConsumerRegistryQueryHandler
                                #ig/ref :afrolabs.components.kafka.consumer-registry/component
                                #ig/ref :my.service/orders-client]]}}
```

`:my.service/http-internal` is a redeclared
`:afrolabs.components.http/service`; `ADVERTISED_URL` points at it, e.g.
`http://10.0.4.17:8080`. `:my.service/orders-consumer` is a redeclared
`:afrolabs.components.kafka/kafka-consumer`. The registry appears twice in
the consumer's strategies: as a rebalance listener, to advertise, and in
`ConsumerRegistryQueryHandler`, to answer queries.

The consumer client:

```clojure
(s/def ::orders-client-cfg map?)

(-comp/defcomponent {::-comp/config-spec ::orders-client-cfg
                     ::-comp/ig-kw       ::orders-client}
  [_]
  (let [orders (atom {})]
    (reify
      -kafka/IConsumerClient
      (consume-messages [_ msgs]
        (swap! orders into (map (juxt :key :value)) msgs)
        nil)

      -registry/IConsumerGroupQueryHandler
      (handle-query [_ _ {:keys [order-id]}]
        (get @orders order-id)))))
```

### Query a consumer group

```clojure
(-registry/query registry {:group-id "orders" :query {:order-id 17}})
;=> [{:order-id 17 :status :shipped}]
```

### Confluent Cloud

```clojure
:afrolabs.components.kafka.consumer-registry/component
{:bootstrap-server           #parameter KAFKA_BOOTSTRAP_SERVER
 :registry-topic             "consumer-registry"
 :advertised-url             #parameter ADVERTISED_URL
 :query-path                 "/consumer-registry/query"
 :query-token                #parameter CONSUMER_REGISTRY_QUERY_TOKEN
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

A query is also sent to processes that stopped within the last 90 minutes;
`query` then blocks until the timeout. A process that has left the group
receives queries until its entries are replaced or expire.

Queries and answers must be Clojure data or `java.time` values.

Serve queries on an internal HTTP server; `:query-token` is their only
protection.

A partition revoke may take up to 40 seconds longer while pending
advertisements are written.

A registered consumer with static group membership (`group.instance.id`)
throws on its first partition assignment; its service becomes unhealthy.

## SEE ALSO

`afrolabs.components.kafka` — `SubscribeWithTopicsCollection`,
`SubscribeWithTopicNameProvider`, `IConsumerAwareRebalanceListener`.

`afrolabs.components.http` — `IHttpRequestHandler`.

[kafka-topic-asserters](kafka-topic-asserters.md)
