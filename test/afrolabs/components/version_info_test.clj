(ns afrolabs.components.version-info-test
  (:require [afrolabs.components.http :as -http]
            [afrolabs.components.version-info :as sut]
            [clojure.spec.alpha :as s]
            [clojure.test :as t :refer [deftest is testing]]
            [integrant.core :as ig]))

(def ^:private resource "afrolabs/components/version_info_test.edn")

(deftest read-version-info
  (is (= {:git-ref "refs/heads/main" :git-sha "f7ba5fa0d8bfd1c8a078cae583f8b4cda39b59c3"}
         (sut/read-version-info resource)))
  (testing "falls back when the resource is missing"
    (is (every? string? (vals (sut/read-version-info "does/not/exist.edn"))))))

(deftest component
  (let [system (ig/init {::sut/version-info {:version-info-resource resource}})]
    (is (= (sut/read-version-info resource)
           (sut/version-info (::sut/version-info system))))
    (ig/halt! system)))

(defn- version-headers
  [middleware-provider]
  (let [[mw]    (-http/get-middleware middleware-provider)
        handler (mw (constantly {:status 200 :headers {}}))]
    (select-keys (:headers (handler {})) ["X-Version-GitRef" "X-Version-GitSHA"])))

(deftest git-version-middleware
  (let [expected {"X-Version-GitRef" "refs/heads/main"
                  "X-Version-GitSHA" "f7ba5fa0d8bfd1c8a078cae583f8b4cda39b59c3"}]
    (testing ":version-info-resource"
      (is (= expected (version-headers (-http/create-git-version-middleware
                                        {:version-info-resource resource})))))
    (testing ":version-info"
      (is (= expected (version-headers (-http/create-git-version-middleware
                                        {:version-info (sut/make-version-info resource)})))))
    (testing "exactly one of the two"
      (is (not (s/valid? ::-http/git-version-middleware-cfg {})))
      (is (not (s/valid? ::-http/git-version-middleware-cfg
                         {:version-info-resource resource
                          :version-info          (sut/make-version-info resource)}))))))

(deftest health-endpoint-cfg
  (let [health (reify afrolabs.components.health/IServiceHealthTripSwitch)]
    (is (s/valid? ::-http/health-endpoint-cfg {:health-component health :endpoint "/health"}))
    (is (s/valid? ::-http/health-endpoint-cfg {:health-component health :endpoint "/health"
                                               :version-info     (sut/make-version-info resource)}))
    (is (not (s/valid? ::-http/health-endpoint-cfg {:health-component      health
                                                    :endpoint              "/health"
                                                    :version-info-resource resource
                                                    :version-info          (sut/make-version-info resource)})))))
