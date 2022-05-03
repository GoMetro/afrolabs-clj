(ns afrolabs.components.kafka-test
  (:require [afrolabs.components.kafka :as sut]
            [clojure.test :as t :refer [deftest is]]))

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
