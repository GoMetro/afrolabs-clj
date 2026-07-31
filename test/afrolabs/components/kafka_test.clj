(ns afrolabs.components.kafka-test
  (:require [afrolabs.components.kafka :as sut]
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
