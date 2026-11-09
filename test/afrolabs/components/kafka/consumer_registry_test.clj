(ns afrolabs.components.kafka.consumer-registry-test
  (:require [afrolabs.components.kafka :as -kafka]
            [afrolabs.components.kafka.consumer-registry :as sut]
            [clojure.core.async :as csp]
            [clojure.test :refer [deftest is testing]]
            [java-time.api :as t])
  (:import [org.apache.kafka.clients.consumer ConsumerGroupMetadata]
           [org.apache.kafka.common TopicPartition]))

(def ^:private assign #'sut/assign)
(def ^:private disown #'sut/disown)
(def ^:private ownership->records #'sut/ownership->records)
(def ^:private retry-delay-ms #'sut/retry-delay-ms)
(def ^:private publish-snapshot! #'sut/publish-snapshot!)
(def ^:private live-advertisements #'sut/live-advertisements)
(def ^:private corrections #'sut/corrections)
(def ^:private refuse-static-membership! #'sut/refuse-static-membership!)

(defn- tps [& tp-pairs]
  (map (fn [[t p]] (TopicPartition. t (int p))) tp-pairs))

(defn- group-metadata
  ([group-id generation member-id]
   (group-metadata group-id generation member-id nil))
  ([group-id generation member-id instance-id]
   (ConsumerGroupMetadata. group-id (int generation) member-id (java.util.Optional/ofNullable instance-id))))

(deftest ownership-tracks-assignment-and-revocation
  (let [c  (Object.)
        o1 (assign {} c (group-metadata "g" 1 "m1") (tps ["t" 0] ["t" 1]))
        o2 (assign o1 c (group-metadata "g" 2 "m1") (tps ["t" 2]))]
    (is (= {:group-id "g" :member-id "m1" :generation 2 :owned #{["t" 0] ["t" 1] ["t" 2]}}
           (get o2 c)))
    (testing "revoking keeps the remainder"
      (is (= #{["t" 2]} (get-in (disown o2 c (tps ["t" 0] ["t" 1])) [c :owned]))))
    (testing "a consumer that owns nothing is dropped"
      (is (= {} (disown o2 c (tps ["t" 0] ["t" 1] ["t" 2])))))))

(deftest records-are-keyed-by-group-topic-partition
  (let [c (Object.)
        o (assign {} c (group-metadata "g" 7 "m") (tps ["t" 3]))]
    (is (= [{:topic "registry"
             :key   ["g" "t" 3]
             :value {:url "http://x" :process-id 1 :member-id "m" :generation 7}}]
           (ownership->records "registry" {:url "http://x" :process-id 1} o)))))

(deftest retry-backoff
  (is (= [5000 10000 55000 60000 60000]
         (map retry-delay-ms [1 2 11 12 100]))))

(defn- fake-producer
  "Return an IProducer that answers every delivered-ch with `(result msg)`,
  recording sent messages in `sent`."
  [sent result]
  (reify -kafka/IProducer
    (produce! [_ msgs]
      (swap! sent into msgs)
      (doseq [{:keys [delivered-ch] :as msg} msgs]
        (csp/put! delivered-ch (result msg))))
    (get-producer [_] nil)))

(defn- with-offsets
  "Return a delivery result fn that assigns increasing offsets from `start`."
  [start]
  (let [o (atom (dec start))]
    #(assoc % :offset (swap! o inc))))

(defn- registry
  [ownership make-producer]
  {:fence          (Object.)
   :ownership      (atom ownership)
   :acked          (atom {})
   :producer       (atom nil)
   :make-producer  make-producer
   :registry-topic "registry"
   :advertisement  {:url "http://x" :process-id 1}})

(deftest publish-snapshot
  (let [c (Object.)
        o (assign {} c (group-metadata "g" 1 "m") (tps ["t" 0] ["t" 1]))]

    (testing "all acknowledged"
      (let [sent (atom [])]
        (is (true? (publish-snapshot! (registry o (constantly (fake-producer sent identity))))))
        (is (= #{["g" "t" 0] ["g" "t" 1]} (set (map :key @sent))))))

    (testing "one failed delivery fails the publish"
      (let [sent (atom [])
            p    (fake-producer sent #(if (= ["g" "t" 1] (:key %)) (ex-info "nope" {}) %))]
        (is (false? (publish-snapshot! (registry o (constantly p)))))))

    (testing "a throwing send fails the publish and releases the fence"
      (let [r (registry o (constantly (reify -kafka/IProducer
                                        (produce! [_ _] (throw (ex-info "max.block.ms" {})))
                                        (get-producer [_] nil))))]
        (is (false? (publish-snapshot! r)))
        (is (true? (deref (future (locking (:fence r) true)) 1000 :blocked)))))

    (testing "empty ownership succeeds without creating a producer"
      (let [r (registry {} #(throw (ex-info "must not create a producer" {})))]
        (is (true? (publish-snapshot! r)))
        (is (nil? @(:producer r)))))

    (testing "a failed producer creation is retried on the next publish"
      (let [sent     (atom [])
            attempts (atom 0)
            r        (registry o (fn []
                                   (when (= 1 (swap! attempts inc))
                                     (throw (ex-info "no resolvable bootstrap urls" {})))
                                   (fake-producer sent identity)))]
        (is (false? (publish-snapshot! r)))
        (is (nil? @(:producer r)))
        (is (true? (publish-snapshot! r)))
        (is (= 2 @attempts))
        (is (= 2 (count @sent)))))

    (testing "acknowledged offsets are recorded, failed ones keep their previous offset"
      (let [r (registry o (constantly (fake-producer (atom []) (with-offsets 10))))]
        (is (true? (publish-snapshot! r)))
        (is (= #{["g" "t" 0] ["g" "t" 1]} (set (keys @(:acked r)))))
        (is (= #{10 11} (set (vals @(:acked r)))))
        (let [before @(:acked r)]
          (reset! (:producer r) (fake-producer (atom []) #(if (= ["g" "t" 1] (:key %))
                                                            (ex-info "nope" {})
                                                            (assoc % :offset 20))))
          (is (false? (publish-snapshot! r)))
          (is (= (assoc before ["g" "t" 0] 20) @(:acked r))))))

    (testing "acknowledged offsets keep only the snapshot's keys"
      (let [r (registry o (constantly (fake-producer (atom []) (with-offsets 0))))]
        (publish-snapshot! r)
        (swap! (:ownership r) disown c (tps ["t" 0]))
        (publish-snapshot! r)
        (is (= #{["g" "t" 1]} (set (keys @(:acked r)))))))))

(deftest corrections-decide-per-owned-key
  (let [k        ["g" "t" 0]
        ours     {:url "http://x" :process-id 1 :member-id "m" :generation 5}
        expected {k ours}
        entry    (fn [v offset] {k (with-meta v {:offset offset})})
        other    (fn [generation] {:url "http://y" :process-id 2 :member-id "n" :generation generation})
        none     {:correct [] :defer []}]

    (testing "nothing acknowledged yet"
      (is (= none (corrections expected {} (entry (other 4) 7)))))

    (testing "no entry, or an entry without an offset"
      (is (= none (corrections expected {k 3} {})))
      (is (= none (corrections expected {k 3} {k (other 4)}))))

    (testing "the ktable has not read our write yet"
      (is (= none (corrections expected {k 7} (entry (other 4) 7))))
      (is (= none (corrections expected {k 7} (entry (other 4) 6)))))

    (testing "the entry is ours"
      (is (= none (corrections expected {k 3} (entry ours 7)))))

    (testing "an older or equal generation written after us is corrected"
      (is (= {:correct [k] :defer []} (corrections expected {k 3} (entry (other 4) 7))))
      (is (= {:correct [k] :defer []} (corrections expected {k 3} (entry (other 5) 7)))))

    (testing "our own record from an earlier ownership period is corrected"
      (is (= {:correct [k] :defer []}
             (corrections expected {k 3} (entry (assoc ours :generation 2) 7)))))

    (testing "a newer generation is deferred to"
      (is (= {:correct [] :defer [k]} (corrections expected {k 3} (entry (other 6) 7)))))

    (testing "keys are decided independently"
      (let [k2 ["g2" "t" 0]]
        (is (= {:correct [k2] :defer [k]}
               (corrections (assoc expected k2 (assoc ours :generation 1))
                            {k 3 k2 3}
                            (merge (entry (other 6) 7)
                                   {k2 (with-meta (other 1) {:offset 8})}))))))))

(deftest static-membership-is-refused
  (is (nil? (refuse-static-membership! (group-metadata "g" 1 "m"))))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"static group membership"
                        (refuse-static-membership! (group-metadata "g" 1 "m" "instance-0")))))

(deftest live-advertisements-drop-entries-older-than-retention
  (let [now   (t/instant "2026-11-09T12:00:00Z")
        at    (fn [m minutes-ago] (with-meta m {:timestamp (t/- now (t/minutes minutes-ago))}))
        ads   {["g" "t" 0] (at {:url "fresh"} 10)
               ["g" "t" 1] (at {:url "edge"} 90)
               ["g" "t" 2] (at {:url "stale"} 91)
               ["g" "t" 3] {:url "no-timestamp"}}]
    (is (= #{["g" "t" 0] ["g" "t" 1] ["g" "t" 3]}
           (set (keys (live-advertisements now ads)))))))
