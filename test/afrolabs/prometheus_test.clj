(ns afrolabs.prometheus-test
  (:require [afrolabs.prometheus :as sut]
            [iapetos.core :as ip]
            [clojure.test :as t :refer [deftest is testing]]))

;; `register-metric` is a top-level macro: it registers into the JVM-global
;; `afrolabs.prometheus/registry` and defs accessor vars, so we exercise it exactly
;; the way real namespaces do — at the top level of this test namespace.
;;
;; A labeled metric must yield BOTH a get-* accessor and (this is the feature under
;; test) a remove-* companion that excises a single labeled child.
(sut/register-metric (ip/gauge ::test-lag
                               {:description "unit-test mirror of consumer-partition-lag"
                                :labels      [:consumer-group-id :topic :partition]}))

;; An UNlabeled metric has no removable children, so it must NOT get a remove-* var.
(sut/register-metric (ip/counter ::test-plain
                                 {:description "unit-test unlabeled metric"}))

(defn- lag-children
  "Scrape the live exporter for our test gauge and return a {partition-string -> value}
   map, restricted to the given consumer-group-id. This reads through the exact same
   sample-extraction path the /metrics endpoint uses, so it reflects what Prometheus
   would actually see on the wire."
  [consumer-group-id]
  (->> (sut/extract-samples #".*test_lag")
       (mapcat :samples)
       (filter #(= consumer-group-id (get-in % [:labels "consumer_group_id"])))
       (map (juxt #(get-in % [:labels "partition"]) :value))
       (into (sorted-map))))

(defn- clear-test-children!
  "Idempotently excise our test children so a test starts from a clean slate even if a
   previous run in the same JVM was interrupted before its cleanup."
  [consumer-group-id]
  (doseq [p ["0" "1" "2"]]
    (remove-gauge-test-lag {:consumer-group-id consumer-group-id
                            :topic             "t"
                            :partition         (Long/parseLong p)})))

(deftest labeled-metric-generates-both-accessors
  (testing "register-metric defs the get-* accessor for a labeled metric"
    (is (some? (resolve 'afrolabs.prometheus-test/get-gauge-test-lag))))
  (testing "register-metric ALSO defs the remove-* companion for a labeled metric"
    (is (some? (resolve 'afrolabs.prometheus-test/remove-gauge-test-lag)))
    (is (fn? (deref (resolve 'afrolabs.prometheus-test/remove-gauge-test-lag))))))

(deftest unlabeled-metric-has-no-remove-accessor
  (testing "a metric without :labels still gets its get-* accessor"
    (is (some? (resolve 'afrolabs.prometheus-test/get-counter-test-plain))))
  (testing "...but no remove-* companion, since it has no labeled children to excise"
    (is (nil? (resolve 'afrolabs.prometheus-test/remove-counter-test-plain)))))

(deftest remove-by-label-excises-single-child
  (let [cg "prometheus-test-remove"]
    (clear-test-children! cg)
    (try
      (testing "both children are exported after being set"
        (ip/set (get-gauge-test-lag {:consumer-group-id cg :topic "t" :partition 0}) 42)
        (ip/set (get-gauge-test-lag {:consumer-group-id cg :topic "t" :partition 1}) 7)
        (is (= {"0" 42.0 "1" 7.0} (lag-children cg))))

      (testing "remove-* excises exactly the targeted child; siblings survive"
        (remove-gauge-test-lag {:consumer-group-id cg :topic "t" :partition 0})
        (is (= {"1" 7.0} (lag-children cg))))

      (testing "a later set cleanly re-creates the previously-removed child"
        (ip/set (get-gauge-test-lag {:consumer-group-id cg :topic "t" :partition 0}) 99)
        (is (= {"0" 99.0 "1" 7.0} (lag-children cg))))
      (finally
        (clear-test-children! cg)))))

(deftest remove-with-nonmatching-labels-is-silent-noop
  (let [cg "prometheus-test-noop"]
    (clear-test-children! cg)
    (try
      (ip/set (get-gauge-test-lag {:consumer-group-id cg :topic "t" :partition 0}) 42)

      (testing "a partial label map (missing :partition) removes nothing"
        (remove-gauge-test-lag {:consumer-group-id cg :topic "t"})
        (is (= {"0" 42.0} (lag-children cg))))

      (testing "a fully-specified but non-existent label combo removes nothing"
        (remove-gauge-test-lag {:consumer-group-id cg :topic "t" :partition 99})
        (is (= {"0" 42.0} (lag-children cg))))
      (finally
        (clear-test-children! cg)))))
