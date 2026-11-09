(ns afrolabs.components.kafka.consumer-registry-test
  (:require [afrolabs.components.kafka :as -kafka]
            [afrolabs.components.kafka.consumer-registry :as sut]
            [clojure.core.async :as csp]
            [clojure.test :refer [deftest is testing]])
  (:import [org.apache.kafka.clients.consumer ConsumerGroupMetadata]
           [org.apache.kafka.common TopicPartition]))

(def ^:private assign #'sut/assign)
(def ^:private disown #'sut/disown)
(def ^:private ownership->records #'sut/ownership->records)
(def ^:private retry-delay-ms #'sut/retry-delay-ms)
(def ^:private publish-snapshot! #'sut/publish-snapshot!)

(defn- tps [& tp-pairs]
  (map (fn [[t p]] (TopicPartition. t (int p))) tp-pairs))

(defn- group-metadata [group-id generation member-id]
  (ConsumerGroupMetadata. group-id (int generation) member-id (java.util.Optional/empty)))

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

(defn- registry [ownership producer]
  {:fence          (Object.)
   :ownership      (atom ownership)
   :producer       (delay producer)
   :registry-topic "registry"
   :advertisement  {:url "http://x" :process-id 1}})

(deftest publish-snapshot
  (let [c (Object.)
        o (assign {} c (group-metadata "g" 1 "m") (tps ["t" 0] ["t" 1]))]

    (testing "all acknowledged"
      (let [sent (atom [])]
        (is (true? (publish-snapshot! (registry o (fake-producer sent identity)))))
        (is (= #{["g" "t" 0] ["g" "t" 1]} (set (map :key @sent))))))

    (testing "one failed delivery fails the publish"
      (let [sent (atom [])
            p    (fake-producer sent #(if (= ["g" "t" 1] (:key %)) (ex-info "nope" {}) %))]
        (is (false? (publish-snapshot! (registry o p))))))

    (testing "a throwing send fails the publish and releases the fence"
      (let [r (registry o (reify -kafka/IProducer
                            (produce! [_ _] (throw (ex-info "max.block.ms" {})))
                            (get-producer [_] nil)))]
        (is (false? (publish-snapshot! r)))
        (is (true? (deref (future (locking (:fence r) true)) 1000 :blocked)))))

    (testing "empty ownership succeeds without creating a producer"
      (let [r (registry {} nil)]
        (is (true? (publish-snapshot! r)))
        (is (not (realized? (:producer r))))))))
