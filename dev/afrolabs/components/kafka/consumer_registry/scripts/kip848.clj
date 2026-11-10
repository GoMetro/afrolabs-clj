(ns afrolabs.components.kafka.consumer-registry.scripts.kip848
  "Check, against a real Kafka broker, that the consumer registry still picks
  the right owner for a partition when consumers use Kafka's newer consumer
  group protocol.

  WHY THIS SCRIPT EXISTS

  The consumer registry records, for every partition of every consumer group,
  which process currently owns it. Sometimes two processes write an entry for
  the same partition and disagree. The usual cause is a process that has been
  removed from its group but has not noticed yet, and keeps writing. The
  registry has to decide which of the two is the real owner.

  It decides with a number Kafka gives to every consumer in a group, called
  the generation. Kafka increases it as the group's partitions are handed out
  again. Every registry entry carries the generation its writer had. When a
  process finds that an entry for a partition it owns was overwritten by
  another writer, it compares the two generations:

  - If the other writer's generation is higher, the process assumes that it
    has itself been removed from the group. It leaves the entry alone and logs
    a warning. This is called deferring.
  - Otherwise it treats the other writer as out of date and writes its own
    entry again. This is called correcting.

  The process records its generation each time Kafka notifies it that
  partitions have been assigned to it.

  Kafka has two consumer group protocols. With the original protocol
  (`group.protocol=classic`), the whole group shares one generation, and
  every member is notified every time it changes. The recorded generation is
  therefore always the member's current one.

  The newer protocol (`group.protocol=consumer`), introduced by Kafka
  Improvement Proposal 848, works differently. Each member has its own
  generation, and a member is not always notified when its generation goes
  up. The concern was that this breaks the comparison above: a real owner
  could defer to a process that no longer owns the partition, or a process
  that no longer owns it could win.

  WHAT THE SCRIPT DOES

  `run-scenario!` runs one consumer group protocol through a fixed sequence.
  It creates its own topics and consumer groups, named after a random run id,
  and deletes them when it finishes. It plays three processes, called A, B
  and C, each with its own registry instance and its own consumer, in one
  group.

  1. A joins the group alone and receives every partition.
  2. B joins. Kafka moves some partitions from A to B.
  3. The script writes a false entry over one of A's partitions, pretending
     to be a process that used to own it. The false entry's generation is one
     lower than A's.
  4. C joins. Kafka moves partitions from A and B to C. Under the newer
     protocol A's entries are behind A's real generation at this point, so
     this is a handover from an owner whose entry lags.
  5. B leaves. Kafka moves B's partitions to A and C.

  After each step the script waits until both the assignment and the registry
  have settled, then records checks. It also records every notification each
  consumer received, together with the generation the consumer reported at
  that moment.

  WHAT IT CHECKS, AND THE RESULT NEEDED

  :owners-match
    Every partition's entry names the consumer that holds that partition.
    Must pass under both protocols.

  :handover-generation
    When a partition moves from one consumer to another, the new owner's entry
    carries a higher generation than the previous owner held while it owned
    the partition: higher than its entry, than the generation it last
    reported before the move, and than the generation it reported when it
    gave the partition up. This is the property the registry depends on; see
    EFFECT ON THE CONSUMER REGISTRY. The check's `:from-lagging-owner` detail
    counts moved partitions whose previous entry was behind its owner's real
    generation. Must pass under both protocols.

  :zombie-corrected
    A notices the false entry from step 3 and writes its own entry again.
    Must pass under both protocols.

  :no-deferrals
    No registry deferred at any point. In this scenario every owner is a real
    member of the group, so a deferral would mean the comparison chose wrong.
    Must pass under both protocols.

  :healthy
    No consumer or registry reported itself unhealthy. Must pass under both
    protocols.

  :generation-current
    Every entry carries its owner's current generation. Informational only;
    it does not count toward the result's `:ok?`. It passes under the
    original protocol. It is expected to fail under the newer protocol, and
    that failure does no harm, for the reason explained below. Once only
    generations differ the script waits `:generation-grace-ms` (default 5000)
    and moves on.

  WHAT WAS FOUND

  Against an Apache Kafka 4.3.1 broker, every required check passes under
  both protocols. `:generation-current` passes under the original protocol.
  Under the newer one it fails after B joins and after C joins.

  The notifications show why. When B joins, A gives up some of its
  partitions. Kafka notifies A of its new, smaller assignment, and only
  afterwards raises A's generation. The generation A reports in that
  notification is still the old one, so A's entries for the partitions it
  kept stay one generation behind until A's next assignment notification.

  So the concern was half right. Under the newer protocol a process's
  recorded generation can fall behind its real one. That does not lead to a
  wrong decision: `:handover-generation` passes for partitions handed over by
  an owner whose entry lagged (A to C in step 4, B to A and C in step 5).

  EFFECT ON THE CONSUMER REGISTRY

  The registry only ever compares two entries for the same partition: one
  from the partition's current owner, and one from a process that owned it
  before. Its decision is right as long as the current owner's entry carries
  the higher generation.

  That always holds, under both protocols:

  - Kafka gives a partition to a new owner only after the previous owner has
    given it up. At the moment the new owner receives it, the new owner's
    generation is higher than any generation the previous owner had while it
    owned the partition. The `:handover-generation` check confirms this
    against a real broker.
  - The new owner records its generation at the moment it receives the
    partition, because Kafka notifies it. Later the recorded value can only
    rise, never fall.
  - A former owner can only write the generation it had while it owned the
    partition.

  A recorded generation that falls behind the real one therefore still beats
  every former owner. The real owner corrects former owners and never defers
  to them. A former owner that is still writing defers to the real owner.

  The implementation must keep this true:

  - Record the generation every time Kafka notifies a consumer that
    partitions have been assigned. The value recorded for a partition must
    never be lower than the generation the consumer had when it received that
    partition. Recording on notifications that assign nothing new narrows the
    lag but cannot close it.
  - Compare generations only between entries for the same partition. A
    process's recorded generation may be behind its real one, so generations
    from different partitions, or from different members of a group, say
    nothing about each other.
  - Do not treat a recorded generation as the consumer's current generation,
    for example to decide whether a process is still alive.
  - Keep refusing static group membership (`group.instance.id`). A static
    member that restarts can come back with the same generation as the
    process it replaces, and then neither entry would be higher.

  RUNNING IT

  Needs a Kafka broker of version 4.0 or later that is used only for this
  purpose. The script creates and deletes topics and consumer groups on it.
  The Kafka serializers must be compiled into target/classes
  (`clojure -T:build/task compile-aot`).

  Run one scenario at a time, in a JVM that runs no other consumer registry.
  `:zombie-corrected` and `:no-deferrals` read counters shared by every
  registry in the process.

  This script is not part of the unit tests, and loading the namespace does
  not run it. Run it from the comment block at the bottom of this namespace.
  Each protocol takes under a minute when the required checks pass."
  (:require [afrolabs.components :as -comp]
            [afrolabs.components.health :as -health]
            [afrolabs.components.kafka :as -kafka]
            [afrolabs.components.kafka.consumer-registry :as -registry]
            [afrolabs.components.time :as -time]
            [clojure.core.async :as csp]
            [clojure.string :as str]
            [iapetos.core :as prom]
            [taoensso.timbre :as log])
  (:import [org.apache.kafka.clients.admin Admin GroupListing]
           [org.apache.kafka.clients.consumer Consumer]
           [org.apache.kafka.common TopicPartition]))

(defn- tp->vec
  [^TopicPartition tp]
  [(.topic tp) (.partition tp)])

(defn- group-state
  [^Consumer consumer]
  (let [gm (.groupMetadata consumer)]
    {:member-id  (.memberId gm)
     :generation (.generationId gm)}))

(defn- corrections-count
  [outcome]
  (prom/value (-registry/get-counter-corrections {:outcome outcome})))

(defn- wait-for
  "Return the first truthy `(f)`, polling every 250 ms, or nil after
  `timeout-ms`."
  [timeout-ms f]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (or (f)
          (when (< (System/currentTimeMillis) deadline)
            (Thread/sleep 250)
            (recur))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; processes

(defn- observer
  "Return {:strategy :listener} recording `member`'s state.

  After every poll, `observed` gets {member {:member-id :generation
  :assignment}}. Every rebalance callback appends to `events`, with the group
  state the consumer reports inside the callback."
  [member observed events]
  (letfn [(event! [kind consumer partitions]
            (swap! events conj (merge {:member     member
                                       :event      kind
                                       :partitions (into (sorted-set) (map tp->vec) partitions)}
                                      (group-state consumer))))]
    {:strategy (reify -kafka/IPostConsumeHook
                 (post-consume-hook [_ consumer _]
                   (swap! observed assoc member
                          (assoc (group-state consumer)
                                 :assignment (into #{} (map tp->vec) (.assignment ^Consumer consumer))))))
     :listener (reify -kafka/IConsumerAwareRebalanceListener
                 (on-partitions-assigned [_ c ps] (event! :assigned c ps))
                 (on-partitions-revoked [_ c ps] (event! :revoked c ps))
                 (on-partitions-lost [_ c ps] (event! :lost c ps)))}))

(defn- start-registry!
  [{:keys [bootstrap-server registry-topic trip-switch]} process]
  (-registry/init-fn-component ::registry
                               {:bootstrap-server           bootstrap-server
                                :registry-topic             registry-topic
                                :advertised-url             (str "http://" process ".kip848.invalid")
                                :query-path                 "/kip848"
                                :query-token                "kip848"
                                :clock                      -time/system-time-instance
                                :service-health-trip-switch trip-switch}))

(defn- start-consumer!
  [{:keys [bootstrap-server topic group-id protocol trip-switch observed events]} member registry]
  (let [{:keys [strategy listener]} (observer member observed events)]
    (-kafka/make-consumer
     {:bootstrap-server           bootstrap-server
      :service-health-trip-switch trip-switch
      :consumer/client            (reify -kafka/IConsumerClient
                                    (consume-messages [_ _] nil))
      :strategies                 [(-kafka/ConsumerGroup group-id)
                                   (-kafka/AdhocConfig "group.protocol" protocol)
                                   (-kafka/ClientId (str "kip848-" (name member)))
                                   (-kafka/StringSerializer :consumer :both)
                                   (-kafka/SubscribeWithTopicsCollection [topic] [registry listener])
                                   strategy]})))

(defn- write-former-owner!
  "Write an entry for `k` from a former owner, one generation below `entry`.
  Return the record's offset."
  [{:keys [bootstrap-server registry-topic]} k entry]
  (let [producer  (-kafka/make-producer {:bootstrap-server bootstrap-server
                                         :strategies       [(-kafka/EdnSerializer :producer :both)]})
        delivered (csp/promise-chan)]
    (try
      (-kafka/produce! producer [{:topic        registry-topic
                                  :key          k
                                  :value        {:url        "http://former-owner.kip848.invalid"
                                                 :process-id (random-uuid)
                                                 :member-id  "former-owner"
                                                 :generation (dec (:generation entry))}
                                  :delivered-ch delivered}])
      (let [r (csp/<!! delivered)]
        (when (instance? Throwable r)
          (throw r))
        (:offset r))
      (finally
        (-comp/halt producer)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; observations

(defn- holders
  "Return {[topic partition] member} when every member of `members` holds a
  partition and together they hold every partition of `topic` exactly once;
  else nil."
  [observed members topic partitions]
  (let [held     (for [m members, tp (get-in observed [m :assignment])] [tp m])
        expected (into #{} (map #(vector topic %)) (range partitions))]
    (when (and (every? #(seq (get-in observed [% :assignment])) members)
               (= (count held) (count expected))
               (= expected (into #{} (map first) held)))
      (into {} held))))

(defn- mismatches
  "Return {[topic partition] {:holder :holder-state :entry}} for partitions
  whose registry entry differs from the holder's member id or generation."
  [observed holders entries group-id]
  (into (sorted-map)
        (keep (fn [[[t p] m]]
                (let [want (select-keys (get observed m) [:member-id :generation])
                      got  (select-keys (get entries [group-id t p]) [:member-id :generation])]
                  (when (not= want got)
                    [[t p] {:holder m :holder-state want :entry got}]))))
        holders))

(defn- settle!
  "Wait until each of `members` holds a partition, together they hold every
  partition, and the registry names each holder with its current generation.
  Give up `generation-grace-ms` after only generations differ, or after
  `settle-timeout-ms`. Return the last state seen: {:holders :entries
  :observed :mismatches}. `:holders` is nil when the assignments never
  settled."
  [{:keys [observed topic partitions group-id settle-timeout-ms generation-grace-ms]} members registry]
  (let [deadline (+ (System/currentTimeMillis) settle-timeout-ms)]
    (loop [owners-matched-at nil]
      (let [now               (System/currentTimeMillis)
            o                 @observed
            hs                (holders o members topic partitions)
            entries           @registry
            state             {:holders    hs
                               :entries    entries
                               :observed   (select-keys o members)
                               :mismatches (when hs (mismatches o hs entries group-id))}
            owners-match?     (and hs
                                   (every? (fn [{:keys [holder-state entry]}]
                                             (= (:member-id holder-state) (:member-id entry)))
                                           (vals (:mismatches state))))
            owners-matched-at (when owners-match? (or owners-matched-at now))]
        (if (or (and hs (empty? (:mismatches state)))
                (and owners-matched-at (>= (- now owners-matched-at) generation-grace-ms))
                (> now deadline))
          state
          (do (Thread/sleep 250)
              (recur owners-matched-at)))))))

(defn- settle-checks
  [step {:keys [holders mismatches observed]}]
  (if-not holders
    [{:check   :owners-match
      :step    step
      :ok?     false
      :details {:reason "assignments never settled" :observed observed}}]
    (let [differing (fn [k]
                      (into (sorted-map)
                            (remove (fn [[_ {:keys [holder-state entry]}]]
                                      (= (k holder-state) (k entry))))
                            mismatches))]
      [{:check :owners-match :step step :ok? (empty? (differing :member-id)) :details (differing :member-id)}
       {:check         :generation-current
        :step          step
        :ok?           (empty? (differing :generation))
        :informational true
        :details       (differing :generation)}])))

(defn- handover-check
  "Return the :handover-generation check for partitions whose entry names a
  different member in settled state `after` than in `before`.

  A previous owner's `:held-generation` is the highest of its entry's
  generation, the generation it reported at `before`, and any it reported in
  a callback giving the partition up. `:from-lagging-owner` counts moved
  partitions whose previous entry was below it."
  [step before after events]
  (let [reported (into {}
                       (map (juxt :member-id :generation))
                       (vals (:observed before)))
        released (reduce (fn [acc {:keys [event member-id generation partitions]}]
                           (if (#{:revoked :lost} event)
                             (reduce #(update %1 [member-id %2] (fnil max generation) generation)
                                     acc
                                     partitions)
                             acc))
                         {}
                         events)
        moved    (into (sorted-map)
                       (keep (fn [[[_ t p :as k] e]]
                               (let [{:keys [member-id generation] :as b} (get (:entries before) k)]
                                 (when (and b (not= member-id (:member-id e)))
                                   [k {:before {:member-id       member-id
                                                :generation      generation
                                                :held-generation (max generation
                                                                      (get reported member-id generation)
                                                                      (get released [member-id [t p]] generation))}
                                       :after  (select-keys e [:member-id :generation])}]))))
                       (:entries after))
        bad      (into (sorted-map)
                       (remove (fn [[_ {:keys [before after]}]]
                                 (> (:generation after) (:held-generation before))))
                       moved)]
    {:check   :handover-generation
     :step    step
     :ok?     (boolean (and (seq moved) (empty? bad)))
     :details {:moved              (count moved)
               :from-lagging-owner (count (filter (fn [[_ {:keys [before]}]]
                                                    (> (:held-generation before) (:generation before)))
                                                  moved))
               :not-increasing     bad}}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; cleanup

(defn- attempt!
  "Call `f`. Log a throw instead of propagating it."
  [what f]
  (try (f)
       (catch Throwable t
         (log/warn t (str "kip848 cleanup step failed: " what)))))

(defn- delete-run-groups!
  "Delete every consumer group whose id contains `run-id`."
  [^Admin admin run-id]
  (let [group-ids (into []
                        (comp (map #(.groupId ^GroupListing %))
                              (filter #(str/includes? % run-id)))
                        (.get (.all (.listGroups admin))))]
    (when (seq group-ids)
      (.get (.all (.deleteConsumerGroups admin group-ids))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; scenario

(defn run-scenario!
  "Run the scenario in the namespace doc against `bootstrap-server`, with the
  consumers on `protocol`, \"classic\" or \"consumer\". Block until done;
  takes up to a few minutes when checks fail.

  Return {:protocol :ok? :checks [{:check :step :ok? :details}] :events}.
  `:ok?` ignores checks marked `:informational`."
  [{:keys [bootstrap-server protocol partitions settle-timeout-ms generation-grace-ms]
    :or   {partitions          6
           settle-timeout-ms   60000
           generation-grace-ms 5000}}]
  (let [run-id     (str "kip848-" protocol "-" (subs (str (random-uuid)) 0 8))
        tripped    (atom [])
        ctx        {:bootstrap-server    bootstrap-server
                    :protocol            protocol
                    :partitions          partitions
                    :settle-timeout-ms   settle-timeout-ms
                    :generation-grace-ms generation-grace-ms
                    :topic               (str run-id "-data")
                    :registry-topic      (str run-id "-registry")
                    :group-id            (str run-id "-group")
                    :observed            (atom {})
                    :events              (atom [])
                    :trip-switch         (reify -health/IServiceHealthTripSwitch
                                           (indicate-unhealthy! [_ subsystem] (swap! tripped conj subsystem))
                                           (wait-while-healthy [_] nil)
                                           (healthy? [_] (empty? @tripped)))}
        checks     (atom [])
        consumers  (atom {})
        registries (atom [])
        admin      (-kafka/make-admin-client {:bootstrap-server bootstrap-server :strategies []})]
    (try
      (-kafka/assert-topics! @admin [(:topic ctx)] :nr-of-partitions partitions)
      (let [ra               (doto (start-registry! ctx "a") (->> (swap! registries conj)))
            rb               (doto (start-registry! ctx "b") (->> (swap! registries conj)))
            rc               (doto (start-registry! ctx "c") (->> (swap! registries conj)))
            deferred-at-start (corrections-count "deferred")
            handover!        (fn [step before after]
                               (swap! checks conj (handover-check step before after @(:events ctx))))

            _  (swap! consumers assoc :a (start-consumer! ctx :a ra))
            s1 (settle! ctx [:a] ra)
            _  (swap! checks into (settle-checks :a-joins s1))

            _  (swap! consumers assoc :b (start-consumer! ctx :b rb))
            s2 (settle! ctx [:a :b] ra)
            _  (swap! checks into (settle-checks :b-joins s2))
            _  (handover! :b-joins s1 s2)

            a-member         (get-in s2 [:observed :a :member-id])
            [k entry]        (first (filter (fn [[_ e]] (= a-member (:member-id e))) @ra))
            corrected-before (corrections-count "corrected")
            former-offset    (when k (write-former-owner! ctx k entry))
            restored         (when former-offset
                               (wait-for 30000
                                         #(let [e (get @ra k)]
                                            (when (and (= a-member (:member-id e))
                                                       (< former-offset (:offset (meta e))))
                                              e))))
            _  (swap! checks conj {:check   :zombie-corrected
                                   :step    :former-owner-writes
                                   :ok?     (boolean (and restored (< corrected-before (corrections-count "corrected"))))
                                   :details {:key k :former-owner-offset former-offset :entry (get @ra k)}})

            s3 (settle! ctx [:a :b] ra)
            _  (swap! consumers assoc :c (start-consumer! ctx :c rc))
            s4 (settle! ctx [:a :b :c] ra)
            _  (swap! checks into (settle-checks :c-joins s4))
            _  (handover! :c-joins s3 s4)

            _  (-comp/halt (:b @consumers))
            _  (swap! consumers dissoc :b)
            s5 (settle! ctx [:a :c] ra)
            _  (swap! checks into (settle-checks :b-leaves s5))
            _  (handover! :b-leaves s4 s5)]
        (swap! checks conj {:check   :no-deferrals
                            :step    :run
                            :ok?     (= deferred-at-start (corrections-count "deferred"))
                            :details {:deferred (- (corrections-count "deferred") deferred-at-start)}})
        (swap! checks conj {:check :healthy :step :run :ok? (empty? @tripped) :details @tripped})
        {:protocol protocol
         :ok?      (every? :ok? (remove :informational @checks))
         :checks   @checks
         :events   @(:events ctx)})
      (finally
        (doseq [c (vals @consumers)] (attempt! "halt consumer" #(-comp/halt c)))
        (doseq [r @registries] (attempt! "halt registry" #(-comp/halt r)))
        (attempt! "delete topics" #(-kafka/delete-topics! @admin [(:topic ctx) (:registry-topic ctx)]))
        (attempt! "delete consumer groups" #(delete-run-groups! @admin run-id))
        (attempt! "close admin client" #(-comp/halt admin))))))

(comment

  (def bootstrap-server "localhost:9092")

  (def classic (run-scenario! {:bootstrap-server bootstrap-server :protocol "classic"}))
  (def kip848 (run-scenario! {:bootstrap-server bootstrap-server :protocol "consumer"}))

  (map (juxt :protocol :ok?) [classic kip848])

  ;; failed checks, with details
  (remove :ok? (:checks kip848))

  ;; rebalance callbacks per member, with the generation reported in each
  (:events kip848)

  )
