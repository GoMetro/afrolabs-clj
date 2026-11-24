(ns afrolabs.components.sentry-test
  (:require [afrolabs.components :as -comp]
            [afrolabs.components.sentry :as sut]
            [afrolabs.components.version-info :as -version-info]
            [clojure.test :as t :refer [deftest is testing]]
            [integrant.core :as ig]
            [taoensso.timbre :as timbre])
  (:import [io.sentry SentryEvent]))

(def ^:private dsn "https://key@localhost/1")

(defn- capturing-client
  "Return [client events-atom]. Events are captured by `:before-send` and dropped."
  ([] (capturing-client {}))
  ([cfg]
   (let [events (atom [])]
     [(sut/make-client (merge {:dsn         dsn
                               :before-send (fn [e _] (swap! events conj e) nil)}
                              cfg))
      events])))

(defn- contexts
  [^SentryEvent e]
  (let [c (.getContexts e)]
    (into {}
          (keep (fn [k] (when (#{"log-context" "ex-data" "exception"} k)
                          [k (.get c k)])))
          (iterator-seq (.keys c)))))

(defn- summary
  [^SentryEvent e]
  {:level      (str (.getLevel e))
   :message    (some-> e .getMessage .getMessage)
   :logger     (.getLogger e)
   :tags       (into {} (.getTags e))
   :release    (.getRelease e)
   :exceptions (count (.getExceptions e))
   :contexts   (contexts e)})

(defn- log-to
  [client appender-opts f]
  (timbre/with-config {:min-level :trace
                       :appenders {:sentry (sut/timbre-appender client appender-opts)}}
    (f)))

(defmacro ^:private log!
  "Log as timbre does from namespace `ns-str`."
  [level ns-str & args]
  `(timbre/log! {:level ~level :msg-type :p :args [~@args] :loc {:ns ~ns-str}}))

(deftest appender-exception-event
  (let [[client events] (capturing-client)]
    (log-to client {}
            #(timbre/with-context {:request-id "r-1" :order/id 42}
               (log! :error "my.service"
                     (ex-info "Order rejected" {:order/id 42 :reason :stock}
                              (ex-info "Upstream" {:status 503}))
                     "Failed to place order")))
    (let [[e & more] (map summary @events)]
      (is (nil? more))
      (is (= "ERROR" (:level e)))
      (is (= "Failed to place order" (:message e)))
      (is (= "my.service" (:logger e)))
      (is (= 2 (:exceptions e)))
      (is (= {"request-id" "r-1"} (:tags e)))
      (testing "ex-data of the logged ExceptionInfo"
        (is (= {":order/id" 42 ":reason" ":stock"}
               (get-in e [:contexts "ex-data"]))))
      (testing "Throwable->map without :trace"
        (let [ex (get-in e [:contexts "exception"])]
          (is (= ["clojure.lang.ExceptionInfo" "clojure.lang.ExceptionInfo"]
                 (map #(get % ":type") (get ex ":via"))))
          (is (= {":status" 503} (-> ex (get ":via") (nth 1) (get ":data"))))
          (is (= "Upstream" (get ex ":cause")))
          (is (not (contains? ex ":trace")))))
      (is (= {":request-id" "r-1" ":order/id" 42}
             (get-in e [:contexts "log-context"]))))))

(deftest appender-message-event
  (let [[client events] (capturing-client)]
    (log-to client {}
            #(timbre/with-context {:request-id "r-2" :nested {:a [1 :b]}}
               (log! :warn "my.service" "Static message")))
    (let [[e] (map summary @events)]
      (is (= "WARNING" (:level e)))
      (is (= "Static message" (:message e)))
      (is (zero? (:exceptions e)))
      (is (= {"log-context" {":request-id" "r-2" ":nested" {":a" [1 ":b"]}}}
             (:contexts e))))))

(deftest ex-data-from-cause
  (let [[client events] (capturing-client)]
    (sut/capture-exception! client (RuntimeException. "wrapper" (ex-info "inner" {:k 1})))
    (is (= {":k" 1} (get-in (summary (first @events)) [:contexts "ex-data"])))))

(deftest no-ex-data-without-ex-info
  (let [[client events] (capturing-client)]
    (sut/capture-exception! client (RuntimeException. "plain"))
    (is (= #{"exception"} (set (keys (:contexts (summary (first @events)))))))))

(deftest level-mapping
  (let [[client events] (capturing-client)]
    (log-to client {:min-level :trace}
            #(doseq [l [:trace :debug :info :warn :error :fatal :report]]
               (log! l "my.service" (name l))))
    (is (= ["DEBUG" "DEBUG" "INFO" "WARNING" "ERROR" "FATAL" "FATAL"]
           (map (comp :level summary) @events)))))

(deftest min-level
  (testing "defaults to :warn"
    (let [[client events] (capturing-client)]
      (log-to client {} #(do (log! :info "my.service" "info")
                             (log! :warn "my.service" "warn")))
      (is (= ["warn"] (map (comp :message summary) @events)))))
  (testing "namespace-filter form"
    (let [[client events] (capturing-client)]
      (log-to client {:min-level [[#{"noisy.*"} :error] ["*" :warn]]}
              #(do (log! :warn "noisy.lib" "noisy warn")
                   (log! :error "noisy.lib" "noisy error")
                   (log! :warn "my.service" "warn")))
      (is (= ["noisy error" "warn"] (map (comp :message summary) @events))))))

(deftest tags-and-context-do-not-leak
  (let [[client events] (capturing-client {:tags {"service" "svc"}})]
    (sut/capture-message! client "first" :warn {:tags {:tenant "a"} :log-context {:x 1}})
    (sut/capture-message! client "second" :warn)
    (let [[e1 e2] (map summary @events)]
      (is (= {"service" "svc" "tenant" "a"} (:tags e1)))
      (is (= {"service" "svc"} (:tags e2)))
      (is (= {} (:contexts e2))))))

(deftest release
  (let [vi (with-meta {} {`-version-info/version-info (fn [_] {:git-sha "abc123" :git-ref "refs/heads/main"})})]
    (testing "from version-info"
      (let [[client events] (capturing-client {:version-info vi})]
        (sut/capture-message! client "m" :warn)
        (is (= "abc123" (:release (summary (first @events)))))))
    (testing "explicit :release wins"
      (let [[client events] (capturing-client {:version-info vi :release "1.2.3"})]
        (sut/capture-message! client "m" :warn)
        (is (= "1.2.3" (:release (summary (first @events)))))))))

(deftest disabled
  (testing "nil DSN"
    (let [client (sut/make-client {:dsn nil})]
      (is (not (sut/enabled? client)))
      (is (nil? (sut/capture-message! client "m" :warn)))
      (is (nil? (sut/capture-exception! client (Exception. "e"))))
      (is (nil? ((:fn (sut/timbre-appender client)) {:level :error :msg_ (delay "m")})))
      (-comp/halt client)))
  (testing "nil client"
    (is (not (sut/enabled? nil)))
    (is (nil? (sut/capture-message! nil "m" :warn)))
    (is (nil? ((:fn (sut/timbre-appender nil)) {:level :error :msg_ (delay "m")}))))
  (testing ":disabled? component"
    (let [system (ig/init {::sut/client {:disabled? true :dsn dsn}})]
      (is (nil? (::sut/client system)))
      (ig/halt! system))))

(deftest invalid-dsn-throws
  (is (thrown? IllegalArgumentException (sut/make-client {:dsn "not a dsn"}))))

(sut/redeclare-client ::other-client)

(deftest independent-clients
  (let [events-a (atom [])
        events-b (atom [])
        system   (ig/init {::sut/client   {:dsn dsn :before-send (fn [e _] (swap! events-a conj e) nil)}
                           ::other-client {:dsn dsn :before-send (fn [e _] (swap! events-b conj e) nil)}})]
    (try
      (sut/capture-message! (::sut/client system) "to a" :warn)
      (sut/capture-message! (::other-client system) "to b" :warn)
      (is (= ["to a"] (map (comp :message summary) @events-a)))
      (is (= ["to b"] (map (comp :message summary) @events-b)))
      (finally (ig/halt! system)))))
