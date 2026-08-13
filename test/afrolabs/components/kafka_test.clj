(ns afrolabs.components.kafka-test
  (:require [afrolabs.components.kafka :as sut]
            [afrolabs.prometheus :as -prom]
            [clojure.test :as t :refer [deftest is testing]]
            [java-time.api :as jt]))

(deftest AdhocConfig-throw-on-uneven-config
  (is (thrown? clojure.lang.ExceptionInfo
               (sut/AdhocConfig "oeu" "oeu" "uu"))))

(deftest AdhocConfig-reject-invalid-keys
  (let [ah-cfg (sut/AdhocConfig "bootstrap.servers" "localhost:9092"
                                "client.dns.lookup" "1"
                                "group.id" "group_id"
                                "retries" "1"
                                )]
    (is (= {"bootstrap.servers" "localhost:9092"
            "client.dns.lookup" "1"
            "group.id"          "group_id"
            "something"         "else"}
           (sut/update-consumer-cfg-hook ah-cfg
                                         {"something" "else"})))
    (is (= {"bootstrap.servers" "localhost:9092"
            "client.dns.lookup" "1"
            "retries"           "1"
            "something"         "else"}
           (sut/update-producer-cfg-hook ah-cfg
                                         {"something" "else"})))
    (is (= {"bootstrap.servers" "localhost:9092"
            "client.dns.lookup" "1"
            "retries"           "1"
            "something"         "else"}
           (sut/update-admin-client-cfg-hook ah-cfg
                                             {"something" "else"})))))

(deftest consumer-record-timestamp->instant-maps-non-positive-to-nil
  ;; kafka uses -1 (RecordBatch/NO_TIMESTAMP) for records without a timestamp
  (is (nil? (sut/consumer-record-timestamp->instant -1)))
  (is (nil? (sut/consumer-record-timestamp->instant 0)))
  (is (= (jt/instant 1752134400000)
         (sut/consumer-record-timestamp->instant 1752134400000))))

(deftest merge-updates-with-ktable-retention-tolerates-records-without-a-timestamp
  (let [now       (jt/instant "2026-07-10T10:00:00Z")
        recent-ts (jt/instant "2026-07-09T12:00:00Z") ;; within the 24h retention window
        msg       (fn [k v ts] (cond-> {:topic     "topic"
                                        :key       k
                                        :value     v
                                        :partition 0
                                        :offset    1}
                                 ts (assoc :timestamp ts)))
        ktable    (sut/merge-updates-with-ktable nil
                                                 [(msg "no-ts" {:a 1} nil)
                                                  (msg "recent" {:a 2} recent-ts)
                                                  (msg "fresh" {:a 3} now)]
                                                 {:retention-ms (jt/as (jt/duration 24 :hours) :millis)})]
    (is (= {:a 2} (get-in ktable ["topic" "recent"]))
        "A record with a timestamp within retention (relative to the max encountered) is kept.")
    (is (= {:a 3} (get-in ktable ["topic" "fresh"])))
    (is (= {:a 1} (get-in ktable ["topic" "no-ts"]))
        "A record without a timestamp must not throw and is never expired.")))

(deftest merge-updates-with-ktable-retention-still-expunges-stale-records
  (let [now      (jt/instant "2026-07-10T10:00:00Z")
        stale-ts (jt/instant "2026-07-01T10:00:00Z")
        ktable   (sut/merge-updates-with-ktable nil
                                                [{:topic "topic" :key "stale" :value {:a 1}
                                                  :partition 0 :offset 1 :timestamp stale-ts}
                                                 {:topic "topic" :key "no-ts" :value {:a 2}
                                                  :partition 0 :offset 2}
                                                 {:topic "topic" :key "fresh" :value {:a 3}
                                                  :partition 0 :offset 3 :timestamp now}]
                                                {:retention-ms (jt/as (jt/duration 1 :hours) :millis)})]
    (is (nil? (get-in ktable ["topic" "stale"]))
        "A record older than retention-ms (relative to the max encountered timestamp) is expunged.")
    (is (= {:a 2} (get-in ktable ["topic" "no-ts"])))
    (is (= {:a 3} (get-in ktable ["topic" "fresh"])))))

;;;; ktable-atom-wait-for-catchup ;;;;
;;
;; This is the "caught-up" detector that drives the ktable's first-caught-up signal
;; (and, downstream, the ktable-startup-duration-secs gauge). It works purely off an
;; atom whose value carries :ktable/topic-partition-offsets metadata, so it can be
;; tested without a broker. `topic-partition-offset` and the offsets metadata are both
;; {topic {partition offset}}.

(defn- ktable-with-offsets
  "A ktable atom value (empty map) whose metadata advertises the given progress offsets."
  [offsets]
  (with-meta {} {:ktable/topic-partition-offsets offsets}))

(deftest ktable-wait-returns-immediately-when-already-caught-up
  (let [offsets {"topic" {0 100}}
        a       (atom (ktable-with-offsets offsets))
        result  (sut/ktable-atom-wait-for-catchup a
                                                  {"topic" {0 50}}
                                                  (jt/duration 1 :seconds)
                                                  ::timeout)]
    (is (not= ::timeout result)
        "Progress (100) already exceeds the target (50), so it must not time out.")
    (is (= offsets result)
        "When already caught up it returns the ktable's current progress offsets.")))

(deftest ktable-wait-times-out-when-target-not-reached
  (let [a      (atom (ktable-with-offsets {"topic" {0 10}}))
        result (sut/ktable-atom-wait-for-catchup a
                                                 {"topic" {0 1000}}
                                                 (jt/duration 200 :millis)
                                                 ::timeout)]
    (is (= ::timeout result)
        "Progress (10) never reaches the target (1000) and the atom never changes.")))

(deftest ktable-wait-returns-once-offsets-advance
  (let [a (atom (ktable-with-offsets {"topic" {0 5}}))]
    ;; advance progress past the target shortly after the wait begins
    (future (Thread/sleep 100)
            (swap! a vary-meta assoc-in [:ktable/topic-partition-offsets "topic" 0] 500))
    (let [result (sut/ktable-atom-wait-for-catchup a
                                                   {"topic" {0 100}}
                                                   (jt/duration 5 :seconds)
                                                   ::timeout)]
      (is (not= ::timeout result)
          "The background advance to 500 crosses the target (100) before the timeout.")
      (is (= {"topic" {0 500}} result)
          "It returns the progress offsets observed at the moment it caught up."))))

(deftest ktable-wait-ignores-offsets-for-untracked-topics
  (let [offsets {"tracked" {0 100}}
        a       (atom (ktable-with-offsets offsets))
        result  (sut/ktable-atom-wait-for-catchup a
                                                  {"tracked"   {0 50}
                                                   "untracked" {0 999999}}
                                                  (jt/duration 1 :seconds)
                                                  ::timeout)]
    (is (= offsets result)
        "An offset for a topic the ktable does not consume must not block catch-up.")))

;;;; ktable-entry-count ;;;;
;;
;; `publish-ktable-entry-counts!` is the only writer of the ::ktable-entry-count gauge. It is
;; called twice: once at ktable init from the restored checkpoint value, and after every consumed
;; batch. The init call is the reason this exists -- a checkpoint-restored ktable on a near-silent
;; topic used to publish nothing at all, so its size was invisible until a message happened to
;; arrive (which for a config topic can be a day away).
;;
;; It needs no broker, so we drive it directly and read the gauge back through the same
;; sample-extraction path the /metrics endpoint uses.

(def ^:private publish-entry-counts! #'sut/publish-ktable-entry-counts!)

(defn- entry-counts
  "Scrape the live exporter for ::ktable-entry-count children of `ktable-id`, as {topic -> value}."
  [ktable-id]
  (->> (-prom/extract-samples #".*ktable_entry_count")
       (mapcat :samples)
       (filter #(= ktable-id (get-in % [:labels "ktable_id"])))
       (map (juxt #(get-in % [:labels "topic"]) :value))
       (into (sorted-map))))

(defn- clear-entry-counts!
  "Idempotently excise our children, so a test starts clean even if a previous run in the same JVM
   was interrupted before its cleanup. The registry is JVM-global."
  [ktable-id topics]
  (doseq [t topics]
    (sut/remove-gauge-ktable-entry-count {:ktable-id ktable-id :topic t})))

(deftest publish-ktable-entry-counts-publishes-one-child-per-topic
  (let [ktable-id "kafka-test-entry-count"
        topics    ["topic-a" "topic-b" "topic-c"]]
    (clear-entry-counts! ktable-id topics)
    (try
      (testing "one child per topic, valued by the number of live keys in that topic"
        (publish-entry-counts! ktable-id {"topic-a" {"k1" {:a 1} "k2" {:a 2}}
                                          "topic-b" {"k1" {:b 1}}})
        (is (= {"topic-a" 2.0 "topic-b" 1.0} (entry-counts ktable-id))))

      (testing "an emptied sub-map (tombstones, retention) publishes 0, not nothing"
        (publish-entry-counts! ktable-id {"topic-a" {"k1" {:a 1} "k2" {:a 2}}
                                          "topic-b" {}
                                          "topic-c" {}})
        (is (= {"topic-a" 2.0 "topic-b" 0.0 "topic-c" 0.0} (entry-counts ktable-id))))
      (finally
        (clear-entry-counts! ktable-id topics)))))

(deftest publish-ktable-entry-counts-on-an-empty-value-publishes-nothing
  ;; The init call site passes the restored checkpoint value, which is `{}` when
  ;; `retrieve-latest-checkpoint` returned nil, or when there is no checkpoint storage at all.
  (let [ktable-id "kafka-test-entry-count-empty"]
    (publish-entry-counts! ktable-id {})
    (is (= {} (entry-counts ktable-id)))
    (publish-entry-counts! ktable-id nil)
    (is (= {} (entry-counts ktable-id))
        "A nil ktable value must neither throw nor publish.")))

(deftest publish-ktable-entry-counts-ignores-ktable-meta-data
  ;; All `:ktable/...` data is meta-data, never a map key. If it leaked into the map it would be
  ;; published as a bogus `topic` label -- and, worse, survive halt's removal loop.
  (let [ktable-id "kafka-test-entry-count-meta"
        value     (with-meta {"topic-a" {"k1" {:a 1}}}
                    {:ktable/topic-partition-offsets {"topic-a" {0 42}}
                     :ktable/record-data             {"topic-a" {"k1" {:offset 42 :partition 0}}}
                     :ktable/record-headers          {"topic-a" {"k1" {}}}})]
    (clear-entry-counts! ktable-id ["topic-a"])
    (try
      (publish-entry-counts! ktable-id value)
      (is (= {"topic-a" 1.0} (entry-counts ktable-id))
          "Only real topics become `topic` labels; meta-data keys must not leak.")
      (finally
        (clear-entry-counts! ktable-id ["topic-a"])))))

(deftest publish-ktable-entry-counts-matches-a-value-built-by-merge-updates
  ;; End-to-end on the shape: the counts published are the counts a real ktable value carries,
  ;; including after a tombstone has emptied a topic. That a topic key is never dissoc'd is what
  ;; keeps the init-time publish paired with halt's removal loop.
  (let [ktable-id "kafka-test-entry-count-merged"
        msg       (fn [t k v] {:topic t :key k :value v :partition 0 :offset 1})
        value     (-> (sut/merge-updates-with-ktable nil [(msg "t1" "a" {:x 1})
                                                          (msg "t1" "b" {:x 2})
                                                          (msg "t2" "c" {:x 3})])
                      (sut/merge-updates-with-ktable [(msg "t2" "c" nil)]))]
    (clear-entry-counts! ktable-id ["t1" "t2"])
    (try
      (is (= #{"t1" "t2"} (set (keys value)))
          "A tombstone empties a topic's map but must not remove the topic key.")
      (publish-entry-counts! ktable-id value)
      (is (= {"t1" 2.0 "t2" 0.0} (entry-counts ktable-id))
          "A topic emptied by a tombstone stays a key and reports 0.")
      (finally
        (clear-entry-counts! ktable-id ["t1" "t2"])))))
