(ns afrolabs.websocket-test
  "Behaviour of `afrolabs.websocket` against two peers: an http-kit server for a well-behaved peer,
  and a raw socket that completes the upgrade and then stays silent and never reads, for the
  stalled and unresponsive cases http-kit cannot produce."
  (:require
   [afrolabs.websocket :as ws]
   [clojure.core.async :as a]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [org.httpkit.server :as hk])
  (:import
   (java.io BufferedReader InputStreamReader)
   (java.net ConnectException InetSocketAddress ServerSocket Socket)
   (java.net.http HttpClient)
   (java.security MessageDigest)
   (java.util Base64)
   (java.util.concurrent Executor Executors)))

;;; Peers

(def ^:private server-closes
  "Close statuses http-kit reported, in order."
  (atom []))

(defn- echo-handler
  "Echo each message, except the text commands `close`, `burst <n>` (send 0 … n-1) and
  `big <n>` (send n chars)."
  [ch]
  (fn [m]
    (if-let [[_ cmd n] (and (string? m) (re-matches #"(burst|big) (\d+)" m))]
      (let [n (parse-long n)]
        (if (= "burst" cmd)
          (dotimes [i n] (hk/send! ch (str i)))
          (hk/send! ch (apply str (repeat n \x)))))
      (if (= "close" m)
        (hk/close ch)
        (hk/send! ch m)))))

(defn- handler [req]
  (let [on-msg (promise)]
    (hk/as-channel req
                   {:on-open    (fn [ch] (deliver on-msg (echo-handler ch)))
                    :on-receive (fn [_ m] (@on-msg m))
                    :on-close   (fn [_ status] (swap! server-closes conj status))})))

(def ^:private server (atom nil))

(defn- echo-uri [] (str "ws://localhost:" (hk/server-port @server) "/echo"))

(defn- accept-key [k]
  (.encodeToString (Base64/getEncoder)
                   (.digest (MessageDigest/getInstance "SHA-1")
                            (.getBytes (str k "258EAFA5-E914-47DA-95CA-C5AB0DC85B11") "ISO-8859-1"))))

(def ^:private cleanups (atom []))

(defn- raw-peer!
  "Start a peer that completes one upgrade, then never reads or writes. Return its ws uri.

  Its small receive buffer makes a few MB of sends stall."
  []
  (let [ss (doto (ServerSocket.)
             (.setReceiveBufferSize 4096)
             (.bind (InetSocketAddress. "localhost" 0)))
        s  (future
             (let [^Socket s (.accept ss)
                   r         (BufferedReader. (InputStreamReader. (.getInputStream s) "ISO-8859-1"))
                   k         (loop [k nil]
                               (let [l (.readLine r)]
                                 (if (str/blank? l)
                                   k
                                   (recur (or (second (re-find #"(?i)^sec-websocket-key:\s*(\S+)" l))
                                              k)))))]
               (doto (.getOutputStream s)
                 (.write (.getBytes (str "HTTP/1.1 101 Switching Protocols\r\n"
                                         "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                                         "Sec-WebSocket-Accept: " (accept-key k) "\r\n\r\n")
                                    "ISO-8859-1"))
                 (.flush))
               s))]
    (swap! cleanups conj #(do (.close ss)
                              (when (realized? s) (.close ^Socket @s))))
    (str "ws://localhost:" (.getLocalPort ss) "/")))

(defn- closed-port []
  (with-open [ss (ServerSocket. 0)]
    (.getLocalPort ss)))

;;; Helpers

(defn- take!!
  "Take from `ch`, or return `::timeout` after `ms` (default 2000)."
  ([ch] (take!! ch 2000))
  ([ch ms] (a/alt!! ch ([v] v) (a/timeout ms) ::timeout)))

(defn- open!
  "Connect, aborting the connection after the test."
  ([uri] (open! uri {}))
  ([uri opts]
   (let [conn (take!! (ws/connect! uri opts))]
     (when (:in conn)
       (swap! cleanups conj #(ws/abort! conn)))
     conn)))

(defn- drain!! [ch]
  (loop [acc []]
    (let [m (take!! ch)]
      (if (or (nil? m) (= ::timeout m))
        acc
        (recur (conj acc m))))))

(defn- server-close-status []
  (loop [n 20]
    (or (last @server-closes)
        (when (pos? n)
          (Thread/sleep 50)
          (recur (dec n))))))

(def ^:private mb (byte-array (* 1024 1024)))

(defn- stall!
  "Send 32 MB to a raw peer: far more than socket buffers hold, so later sends never complete."
  [conn]
  (mapv (fn [_] (ws/send! conn mb)) (range 32)))

(defn- outcome
  "`(:closed conn)`'s value, its error replaced by the message."
  [conn]
  (let [v (take!! (:closed conn) 5000)]
    (cond-> v (:error v) (update :error ex-message))))

(use-fixtures :once
  (fn [t]
    (reset! server (hk/run-server handler {:port 0 :legacy-return-value? false}))
    (try (t) (finally (hk/server-stop! @server)))))

(use-fixtures :each
  (fn [t]
    (reset! server-closes [])
    (try (t)
         (finally
           (run! #(%) @cleanups)
           (reset! cleanups [])))))

;;; connect!

(deftest echoes-text-and-binary
  (let [conn (open! (echo-uri))]
    (is (= :ok (take!! (ws/send! conn "hello"))))
    (is (= "hello" (take!! (:in conn))))
    (ws/send! conn (byte-array [1 2 3]))
    (is (= [1 2 3] (vec (take!! (:in conn)))))))

(deftest send-copies-byte-arrays
  (let [conn (open! (echo-uri))
        b    (byte-array [1 2 3])]
    (ws/send! conn b)
    (aset-byte b 0 9)
    (is (= [1 2 3] (vec (take!! (:in conn)))))))

(deftest slow-consumer-loses-nothing
  (let [conn (open! (echo-uri) {:max-msgs-in-queue 2})]
    (ws/send! conn "burst 500")
    (Thread/sleep 300)
    (is (= (map str (range 500)) (repeatedly 500 #(take!! (:in conn)))))))

(deftest connect-fails-with-error
  (is (instance? ConnectException
                 (:error (open! (str "ws://localhost:" (closed-port) "/"))))))

(deftest connect-throws-on-bad-arguments
  (testing "non-ws scheme"
    (is (thrown? IllegalArgumentException (ws/connect! (str/replace (echo-uri) "ws:" "http:")))))
  (testing "malformed uri"
    (is (thrown? IllegalArgumentException (ws/connect! "ws://local host/"))))
  (testing "unknown :on-full-send-queue"
    (is (thrown? clojure.lang.ExceptionInfo
                 (ws/connect! (echo-uri) {:on-full-send-queue :drop})))))

(deftest uses-given-http-client
  (let [runs   (atom 0)
        pool   (Executors/newSingleThreadExecutor)
        client (-> (HttpClient/newBuilder)
                   (.executor (reify Executor
                                (execute [_ r] (swap! runs inc) (.execute pool r))))
                   (.build))]
    (try
      (let [conn (open! (echo-uri) {:http-client client})]
        (ws/send! conn "x")
        (is (= "x" (take!! (:in conn))))
        (is (pos? @runs)))
      (finally (.shutdownNow pool)))))

;;; Ending the connection

(deftest client-close-is-normal
  (let [conn (open! (echo-uri))]
    (is (= {:closed :normal :status 1000 :reason ""} (take!! (ws/close! conn))))
    (is (nil? (take!! (:in conn))))
    (is (= :normal (server-close-status)))))

(deftest server-close-is-normal
  (let [conn (open! (echo-uri))]
    (ws/send! conn "close")
    (is (= :normal (:closed (outcome conn))))
    (is (nil? (take!! (:in conn))))))

(deftest abort-skips-the-handshake
  (let [conn (open! (echo-uri))]
    (is (= {:closed :aborted} (take!! (ws/abort! conn))))
    (is (nil? (take!! (:in conn))))))

(deftest closing-in-closes-on-next-message
  (let [conn (open! (echo-uri))]
    (a/close! (:in conn))
    (is (= ::timeout (take!! (:closed conn) 300)) "nothing happens until a message arrives")
    (ws/send! conn "wake")
    (is (= :normal (:closed (outcome conn))))
    (is (= :normal (server-close-status)))))

(deftest illegal-close-status-is-a-send-failure
  (let [conn (open! (echo-uri))]
    (ws/close! conn 1009 "")
    (is (= {:closed :error :cause :send-failed :error "statusCode"} (outcome conn)))))

(deftest oversized-message-ends-the-connection
  (let [conn (open! (echo-uri) {:max-message-size 100})]
    (ws/send! conn "big 100")
    (is (= 100 (count (take!! (:in conn)))) "at the limit is accepted")
    (ws/send! conn "big 101")
    (is (= {:closed :error :cause :message-too-big :error "Message too big."} (outcome conn)))
    (is (= :policy-violation (server-close-status)))))

(deftest messages-before-the-end-are-still-read
  (let [conn (open! (echo-uri))]
    (ws/send! conn "burst 3")
    (ws/send! conn "close")
    (take!! (:closed conn))
    (is (= ["0" "1" "2"] (drain!! (:in conn))))))

;;; send!

(deftest send-after-close-fails
  (let [conn (open! (echo-uri))]
    (take!! (ws/close! conn))
    (is (= "Connection closed." (ex-message (:error (take!! (ws/send! conn "x"))))))))

(deftest send-rejects-other-types
  (let [conn (open! (echo-uri))]
    (is (thrown? clojure.lang.ExceptionInfo (ws/send! conn 42)))))

(deftest full-send-queue-blocks-by-default
  (let [conn   (open! (raw-peer!) {:ping-interval nil :max-msgs-out-queue 2})
        sender (future (dotimes [_ 32] (ws/send! conn mb)) :returned)]
    (is (= ::blocked (deref sender 1000 ::blocked)))
    (ws/abort! conn)
    (is (= :returned (deref sender 2000 ::blocked)) "the connection's end releases the sender")))

(deftest full-send-queue-aborts-when-asked
  (let [conn (open! (raw-peer!) {:ping-interval             nil
                                 :max-msgs-out-queue        2
                                 :on-full-send-queue        :abort-on-full-send-queue})]
    (dotimes [_ 32] (ws/send! conn mb))
    (is (= {:closed :error :cause :send-queue-full :error "Send queue full."} (outcome conn)))))

;;; Stalled and silent peers

(deftest close-times-out-behind-stalled-sends
  (let [conn  (open! (raw-peer!) {:ping-interval nil :close-timeout 300 :max-msgs-out-queue 64})
        sends (stall! conn)
        t0    (System/currentTimeMillis)]
    (is (= {:closed :timeout} (take!! (ws/close! conn))))
    (is (< (- (System/currentTimeMillis) t0) 1000) "the timeout runs from the call")
    (is (every? #(not= ::timeout (take!! % 1000)) sends) "every send is answered")))

(deftest close-times-out-when-unanswered
  (let [conn (open! (raw-peer!) {:ping-interval nil :close-timeout 300})]
    (is (= {:closed :timeout} (take!! (ws/close! conn))))))

(deftest silent-peer-is-unresponsive
  (let [conn (open! (raw-peer!) {:ping-interval 100 :pong-timeout 100})]
    (is (= :unresponsive (:cause (outcome conn))))))

(deftest stalled-peer-is-unresponsive
  (let [conn (open! (raw-peer!) {:ping-interval 100 :pong-timeout 100 :max-msgs-out-queue 64})]
    (stall! conn)
    (is (= :unresponsive (:cause (outcome conn))))))

(deftest answering-peer-stays-alive
  (let [conn (open! (echo-uri) {:ping-interval 50 :pong-timeout 50})]
    (is (= ::timeout (take!! (:closed conn) 1000)))))
