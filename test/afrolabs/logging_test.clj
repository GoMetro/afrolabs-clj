(ns afrolabs.logging-test
  (:require [afrolabs.logging :as sut]
            [clojure.test :as t :refer [deftest is testing]]
            [taoensso.timbre :as timbre]))

(defn- with-root-config
  "Run `f` with `config` as timbre's root config, restoring the previous one after."
  [config f]
  (let [previous timbre/*config*]
    (try
      (timbre/set-config! config)
      (f)
      (finally
        (timbre/set-config! previous)))))

(defn- recording-appender
  [seen id]
  {:enabled? true
   :fn       (fn [data] (swap! seen conj [id (force (:msg_ data))]))})

(deftest skip-appenders
  (let [seen (atom [])]
    (with-root-config {:min-level :trace
                       :appenders {:a (recording-appender seen :a)
                                   :b (recording-appender seen :b)}}
      #(do (sut/install-skip-appenders!)
           (timbre/info "both")
           (timbre/with-context+ {::sut/skip-appenders #{:b}}
             (timbre/fatal "only a"))
           (timbre/with-context+ {::sut/skip-appenders #{:a :b}}
             (timbre/warn "neither"))))
    (is (= #{[:a "both"] [:b "both"] [:a "only a"]}
           (set @seen)))
    (is (= 3 (count @seen)))))

(deftest install-is-idempotent
  (let [seen (atom [])]
    (with-root-config {:min-level :trace
                       :appenders {:a (recording-appender seen :a)}}
      #(let [_       (sut/install-skip-appenders!)
             wrapped (get-in timbre/*config* [:appenders :a :fn])]
         (sut/install-skip-appenders!)
         (testing "a second install leaves the wrapper in place"
           (is (identical? wrapped (get-in timbre/*config* [:appenders :a :fn]))))
         (timbre/with-context+ {::sut/skip-appenders #{:a}}
           (timbre/info "skipped"))
         (timbre/info "logged")))
    (is (= [[:a "logged"]] @seen))))
