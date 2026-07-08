(ns afrolabs.components.kafka.ktable-operations
  "A broadcast \"command channel\" (control plane) for components that manage stateful
  aggregations backed by ktable (state) topics.

  The problem this solves: aggregating components keep their write-side authority in
  in-process state (typically an atom) which is only (re)loaded from the ktable topic on
  partition assignment. Operational interventions (like clearing a poisoned entry) cannot
  be achieved by writing to the state topic directly — the live instance's in-memory state
  shadows the topic. The intervention has to happen *inside* the live process.

  This component provides the missing in-process hook:

  - It consumes a dedicated command topic (intended to have a SINGLE partition, so that
    commands have a total order).
  - Every process instance consumes with a UNIQUE (but identifiable, via `:group-id-prefix`)
    consumer group-id => broadcast semantics; every live instance sees every command.
  - Consumption starts at LATEST. Commands are ephemeral, live triggers — not durable state.
    An instance that is down when a command is issued will never see it; the durable
    consequence of a command (e.g. a corrective tombstone) belongs on the state topic,
    produced by the handler. Operators are expected to \"fire and watch\" (check logs /
    the ktable) and re-issue if necessary.
  - Offset commits are disabled; the one-shot consumer group leaves no residue.

  Interested components (e.g. aggregators) share this component via integrant refs and
  register command-handler fns with `register-command-handler!`. Every well-formed command
  is dispatched to EVERY registered handler; handlers decide for themselves whether a
  command applies to them (command type + target ownership) and no-op otherwise.

  Command maps are EDN, with an open schema (see `:ktable-ops/command`). This namespace
  does not interpret `:ktable-ops.command/type` — applications define command semantics.

  NB: handlers are invoked synchronously on the command consumer's poll thread. They may
  block (e.g. awaiting produce acks or ktable catch-up) but must complete well within the
  consumer's `max.poll.interval.ms`. Handler exceptions are isolated & logged; they never
  trip the service health switch."
  (:require
   [afrolabs.components :as -comp]
   [afrolabs.components.kafka :as -kafka]
   [afrolabs.spec :as -spec]
   [clojure.core.async :as csp]
   [clojure.spec.alpha :as s]
   [taoensso.timbre :as log]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol IKTableOperationsHandlerRegistry
  "Registration point for in-process command handlers.

  `handler-fn` is a 1-arity fn accepting the command map. Its return value is ignored.
  Handlers must decide applicability themselves (dispatch is broadcast).
  `handler-key` must be unique per registrant; components should use their
  `:afrolabs.components/component-kw`."
  (register-command-handler!   [_ handler-key handler-fn])
  (deregister-command-handler! [_ handler-key]))

(s/def ::ktable-operations #(satisfies? IKTableOperationsHandlerRegistry %))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; command schema (open; applications add their own keys)

(s/def :ktable-ops.command/type qualified-keyword?)
(s/def :ktable-ops.command/id (s/and string? (comp pos? count)))
;; NOTE: an ISO-8601 *string*, not a java.time.Instant — the EDN value serializer is
;; `pr-str`-based and cannot round-trip Instant instances.
(s/def :ktable-ops.command/at string?)
(s/def :ktable-ops.command/issued-by string?)

(s/def :ktable-ops/command
  (s/keys :req [:ktable-ops.command/type]
          :opt [:ktable-ops.command/id
                :ktable-ops.command/at
                :ktable-ops.command/issued-by]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; dispatch

(defn command-msg-value->command
  "Validates the shape of a consumed command-topic message value.
  Returns the command map, or nil (with a warning log) when malformed.
  (A value that failed EDN deserialization arrives here as nil.)"
  [value]
  (if (and (map? value)
           (qualified-keyword? (:ktable-ops.command/type value)))
    value
    (do (log/warn "Ignoring malformed ktable-operations command message."
                  {:value value})
        nil)))

(defn dispatch-command!
  "Dispatches one command map to every handler in `handlers-map`, isolating
  (catching & logging) per-handler exceptions so one broken handler/command
  cannot affect the others (nor the consumer thread)."
  [handlers-map command]
  (doseq [[handler-key handler-fn] handlers-map]
    (log/with-context+ {:command/id   (:ktable-ops.command/id command)
                        :command/type (:ktable-ops.command/type command)
                        :handler-key  handler-key}
      (try
        (handler-fn command)
        (catch Throwable t
          (log/error t "ktable-operations command handler threw an exception."))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; component

(defn create-ktable-operations
  [{:as   cfg
    :keys [command-topic
           group-id-prefix
           bootstrap-server]
    :or   {group-id-prefix "ktable-ops"}}]
  (let [handlers (atom {})

        producer-cfg
        (-> cfg
            (update :strategies
                    concat [(-kafka/StringSerializer :producer :key)
                            (-kafka/EdnSerializer :producer :value)]))
        producer
        (-kafka/make-producer producer-cfg)

        consumer-client
        (reify
          -kafka/IConsumerClient
          (consume-messages [_ msgs]
            (doseq [{:keys [value]} msgs
                    :let [command (command-msg-value->command value)]
                    :when command]
              (dispatch-command! @handlers command))
            ;; a command consumer never produces records itself
            nil))

        consumer-cfg
        (-> cfg
            ;; NOTE: these strategies are appended *after* the config-supplied ones
            ;; (typically just auth), so the component's serdes/group semantics win.
            ;; They are the component's protocol contract, not deployment config.
            (update :strategies
                    concat [(-kafka/FreshConsumerGroup :group-id-prepend group-id-prefix)
                            (-kafka/OffsetReset "latest")
                            (-kafka/AutoCommitOffsets :disabled true)
                            (-kafka/StringSerializer :consumer :key)
                            (-kafka/EdnSerializer :consumer :value)
                            (-kafka/SubscribeWithTopicsCollection [command-topic])])
            (assoc :consumer/client consumer-client))
        consumer (-kafka/make-consumer consumer-cfg)]

    (reify
      clojure.lang.IDeref
      (deref [_]
        {:producer      producer
         :consumer      consumer
         :handlers      handlers
         :command-topic command-topic})

      IKTableOperationsHandlerRegistry
      (register-command-handler! [_ handler-key handler-fn]
        (log/debug "Registering ktable-operations command handler." {:handler-key handler-key})
        (swap! handlers assoc handler-key handler-fn))
      (deregister-command-handler! [_ handler-key]
        (log/debug "Deregistering ktable-operations command handler." {:handler-key handler-key})
        (swap! handlers dissoc handler-key))

      -comp/IHaltable
      (halt [_]
        (-comp/halt producer)
        (-comp/halt consumer)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::command-topic (s/and string? (comp pos? count)))
(s/def ::group-id-prefix (s/and string? (comp pos? count)))

(s/def ::ktable-operations-cfg
  (s/and ::-kafka/clientless-consumer
         (s/keys :req-un [::command-topic]
                 :opt-un [::group-id-prefix])))

(-comp/defcomponent {::-comp/ig-kw              ::ktable-operations
                     ::-comp/config-spec        ::ktable-operations-cfg
                     ::-comp/supports:disabled? true}
  [cfg] (create-ktable-operations cfg))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; operator/REPL send-side

(defn prepare-command
  "Enriches a command map for sending (correlation id & timestamp, when absent)
  and asserts it against `:ktable-ops/command`. Returns the enriched command."
  [command]
  (let [command (cond-> command
                  (not (:ktable-ops.command/id command))
                  (assoc :ktable-ops.command/id (str (random-uuid)))

                  (not (:ktable-ops.command/at command))
                  (assoc :ktable-ops.command/at (str (java.time.Instant/now))))]
    (-spec/assert! :ktable-ops/command command)
    command))

(defn send-command!
  "Operator/REPL helper. Enriches (`prepare-command`), produces the command onto the
  command topic. Returns the enriched command (so the
  operator can correlate log lines by `:ktable-ops.command/id`); throws when delivery fails."
  [ops-component command]
  (let [{:keys [command-topic producer]} @ops-component

        command       (prepare-command command)]
    (let [msg {:topic        command-topic
               :key          (str (:ktable-ops.command/type command))
               :value        command
               :delivered-ch (csp/chan)}
          _ (-kafka/produce! producer [msg])
          delivery-result (csp/<!! (:delivered-ch msg))]
      (when (instance? Throwable delivery-result)
        (throw (ex-info "Failed to deliver ktable-operations command."
                        {:command command}
                        delivery-result)))
      command)))
