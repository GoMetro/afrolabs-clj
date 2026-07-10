(ns afrolabs.components.kafka-test
  (:require [afrolabs.components.kafka :as sut]
            [clojure.test :as t :refer [deftest is]]
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
