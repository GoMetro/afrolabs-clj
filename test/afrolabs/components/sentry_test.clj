(ns afrolabs.components.sentry-test
  (:require [afrolabs.components.sentry :as sut]
            [afrolabs.components.version-info :as -version-info]
            [clojure.string :as str]
            [clojure.test :as t :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
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

(defn- version-info
  [m]
  (with-meta {} {`-version-info/version-info (fn [_] m)}))

(defn- release-of
  "Return the release of an event sent by a client with `cfg`."
  [cfg]
  (let [[client events] (capturing-client cfg)]
    (try
      (sut/capture-message! client "m" :warn)
      (:release (summary (first @events)))
      (finally (.close (sut/scopes client))))))

(defn- release-for
  [git-ref git-sha]
  (release-of {:version-info (version-info (cond-> {}
                                             git-ref (assoc :git-ref git-ref)
                                             git-sha (assoc :git-sha git-sha)))}))

(defn- valid-sentry-release?
  "Sentry's release name rules."
  [r]
  (and (string? r)
       (not (str/blank? r))
       (<= (count r) 200)
       (not (re-find #"[/\\\n\r\t]" r))
       (not (#{"." ".." "latest"} r))))

(def ^:private gen-hex-sha
  (gen/fmap str/join (gen/vector (gen/elements "0123456789abcdef") 40)))

(def ^:private gen-name
  "A tag or branch name segment, as git allows them: not starting with `.`."
  (gen/such-that #(not (str/starts-with? % "."))
                 (gen/fmap str/join (gen/vector (gen/elements "abcxyzABC0189-._") 1 40))))

(def ^:private gen-git-ref
  (gen/one-of [(gen/fmap #(str "refs/tags/" %) gen-name)
               (gen/fmap #(str "refs/heads/" (str/join "/" %)) (gen/vector gen-name 1 4))
               (gen/fmap #(str "refs/" %) gen/string)
               gen/string
               (gen/return nil)]))

(def ^:private gen-git-sha
  (gen/one-of [gen-hex-sha
               (gen/fmap #(str % ":DIRTY") gen-hex-sha)
               gen/string
               (gen/return nil)]))

(deftest explicit-release-wins
  (is (= "1.2.3" (release-of {:version-info (version-info {:git-sha "abc123" :git-ref "refs/heads/main"})
                              :release      "1.2.3"}))))

(defspec release-is-absent-or-valid 200
  (prop/for-all [git-ref gen-git-ref
                 git-sha gen-git-sha]
    (let [r (release-for git-ref git-sha)]
      (or (nil? r) (valid-sentry-release? r)))))

(defspec release-present-when-sha-or-ref-is 100
  (prop/for-all [git-ref (gen/one-of [(gen/fmap #(str "refs/tags/" %) gen-name) (gen/return nil)])
                 git-sha (gen/one-of [gen-hex-sha (gen/return nil)])]
    (let [r (release-for git-ref git-sha)]
      (if (or git-ref git-sha) (some? r) (nil? r)))))

(defspec release-names-the-commit 100
  (prop/for-all [git-ref gen-git-ref
                 git-sha gen-hex-sha
                 dirty?  gen/boolean]
    (let [r (release-for git-ref (cond-> git-sha dirty? (str ":DIRTY")))]
      (and (str/includes? r (subs git-sha 0 8))
           (= dirty? (str/ends-with? r ":DIRTY"))))))

(defspec release-names-the-tag 100
  (prop/for-all [tag     gen-name
                 git-sha gen-hex-sha]
    (str/includes? (release-for (str "refs/tags/" tag) git-sha) tag)))

(defspec release-tells-builds-apart 100
  (prop/for-all [[tag-a tag-b] (gen/such-that (fn [[a b]] (not= a b)) (gen/tuple gen-name gen-name))
                 [sha-a sha-b] (gen/such-that (fn [[a b]] (not= (subs a 0 8) (subs b 0 8)))
                                              (gen/tuple gen-hex-sha gen-hex-sha))]
    (and (not= (release-for (str "refs/tags/" tag-a) sha-a)
               (release-for (str "refs/tags/" tag-b) sha-a))
         (not= (release-for (str "refs/tags/" tag-a) sha-a)
               (release-for (str "refs/tags/" tag-a) sha-b)))))

(deftest disabled
  (testing "nil DSN"
    (let [client (sut/make-client {:dsn nil})]
      (is (not (sut/enabled? client)))
      (is (nil? (sut/capture-message! client "m" :warn)))
      (is (nil? (sut/capture-exception! client (Exception. "e"))))
      (is (nil? ((:fn (sut/timbre-appender client)) {:level :error :msg_ (delay "m")})))))
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
      (finally (ig/halt! system)))
    (testing "halt closes the scopes"
      (is (not (.isEnabled (sut/scopes (::sut/client system))))))))
