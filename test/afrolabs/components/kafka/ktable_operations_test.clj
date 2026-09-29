(ns afrolabs.components.kafka.ktable-operations-test
  (:require [afrolabs.components.kafka.ktable-operations :as sut]
            [clojure.test :as t :refer [deftest is testing]]))

(deftest command-msg-value->command-test
  (testing "well-formed command maps pass through"
    (let [command {:ktable-ops.command/type :ktable/zero-out-entry
                   :ktable-ops.command/id   "some-id"}]
      (is (= command (sut/command-msg-value->command command)))))

  (testing "malformed values are rejected (nil returned)"
    (is (nil? (sut/command-msg-value->command nil))) ;; failed EDN deserialization arrives as nil
    (is (nil? (sut/command-msg-value->command "not-a-map")))
    (is (nil? (sut/command-msg-value->command [:a :vector])))
    (is (nil? (sut/command-msg-value->command {:no-command-type "here"})))
    (is (nil? (sut/command-msg-value->command {:ktable-ops.command/type "not-a-keyword"})))
    (is (nil? (sut/command-msg-value->command {:ktable-ops.command/type :unqualified})))))

(deftest dispatch-command!-test
  (let [command {:ktable-ops.command/type :ktable/zero-out-entry
                 :ktable-ops.command/id   "cmd-1"}]

    (testing "every registered handler receives the command"
      (let [received (atom [])]
        (sut/dispatch-command! {:handler-a #(swap! received conj [:a %])
                                :handler-b #(swap! received conj [:b %])}
                               command)
        (is (= #{[:a command] [:b command]}
               (set @received)))))

    (testing "a throwing handler is isolated; other handlers still run and nothing escapes"
      (let [received (atom [])]
        (sut/dispatch-command! (into (sorted-map)
                                     {:handler-a (fn [_] (throw (ex-info "boom" {})))
                                      :handler-b #(swap! received conj [:b %])})
                               command)
        (is (= [[:b command]] @received))))

    (testing "no handlers registered is a no-op"
      (is (nil? (sut/dispatch-command! {} command))))))

(deftest prepare-command-test
  (testing "generates a correlation id and timestamp when absent"
    (let [prepared (sut/prepare-command {:ktable-ops.command/type :ktable/zero-out-entry})]
      (is (string? (:ktable-ops.command/id prepared)))
      (is (pos? (count (:ktable-ops.command/id prepared))))
      (is (string? (:ktable-ops.command/at prepared)))
      ;; must parse as an instant (and thus round-trips through EDN as a string)
      (is (java.time.Instant/parse (:ktable-ops.command/at prepared)))))

  (testing "preserves a caller-supplied id"
    (is (= "my-id"
           (:ktable-ops.command/id
            (sut/prepare-command {:ktable-ops.command/type :ktable/zero-out-entry
                                  :ktable-ops.command/id   "my-id"})))))

  (testing "extra app-level keys are preserved (open schema)"
    (let [prepared (sut/prepare-command {:ktable-ops.command/type :ktable/zero-out-entry
                                         :ktable/state-topic      "some-topic"
                                         :ktable/record-key       "some-key"})]
      (is (= "some-topic" (:ktable/state-topic prepared)))
      (is (= "some-key" (:ktable/record-key prepared)))))

  (testing "invalid commands throw"
    (is (thrown? Exception (sut/prepare-command {})))
    (is (thrown? Exception (sut/prepare-command {:ktable-ops.command/type "not-a-keyword"})))))
