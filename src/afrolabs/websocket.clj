(ns afrolabs.websocket
  "A small core.async wrapper over the JDK websocket client, shaped after `hato.websocket` but
  lossless. Callers see maps, channels, strings and byte arrays, never JDK websocket or future
  types."
  (:require
   [clojure.core.async :as a])
  (:import
   (java.io ByteArrayOutputStream)
   (java.net URI)
   (java.net.http HttpClient WebSocket WebSocket$Builder WebSocket$Listener)
   (java.nio ByteBuffer)
   (java.time Duration)
   (java.util.concurrent CompletableFuture CompletionException Executors)
   (java.util.function BiConsumer)))

;; Frames that arrive during the upgrade are delivered on ForkJoinPool.commonPool regardless;
;; this executor only governs steady-state callbacks.
(defonce ^:private shared-http-client
  (-> (HttpClient/newBuilder)
      (.executor (Executors/newFixedThreadPool 2 (-> (Thread/ofPlatform)
                                                     (.name "afrolabs-websocket-" 0)
                                                     (.daemon true)
                                                     (.factory))))
      (.build)))

(def ^:private aborted {:closed :aborted})

(defn- unwrap [t]
  (if (and (instance? CompletionException t)
           (ex-cause t))
    (ex-cause t)
    t))

(defn- now [] (System/currentTimeMillis))

(defn- run-op
  "Apply `op` to `w`; once its future completes, put `:ok` or `{:error t}` on the promise-chan
  `reply`."
  [w op reply]
  (try
    (-> ^CompletableFuture (op w)
        (.whenComplete (reify BiConsumer
                         (accept [_ _ t] (a/put! reply (if t {:error (unwrap t)} :ok))))))
    (catch Exception e
      (a/put! reply {:error e}))))

(defn- run-sends!
  "Run the ops queued on `out` against `w` one at a time, until `out` closes and drains; call
  `failed!` with the error of each failure. The JDK fails a send issued while another is
  outstanding."
  [w out failed!]
  (a/go-loop []
    (when-let [[op reply] (a/<! out)]
      (run-op w op reply)
      (when-let [e (:error (a/<! reply))]
        (failed! e))
      (recur))))

(defn- keep-alive!
  "Ping `w` every `interval` ms until `closed` yields. At a tick where `quiet-since` is more than
  `interval` + `timeout` ms old, call `dead!` with `:unresponsive` instead and stop. A ping that
  fails calls `dead!` with `:send-failed`; `dead!` also takes the error.

  The loop does not wait on pings, so a stalled peer cannot stall the silence check. A ping
  refused because another ping or pong is still pending is skipped."
  [w closed quiet-since interval timeout dead!]
  (a/go-loop []
    (a/alt!
      closed nil
      (a/timeout interval)
      ([_]
       (let [t @quiet-since]
         (if (and t (> (- (now) t)
                       (+ interval timeout)))
           (dead! :unresponsive (ex-info "Peer unresponsive." {:silent-ms (- (now) t)}))
           (let [reply (a/promise-chan)]
             (run-op w #(.sendPing ^WebSocket % (ByteBuffer/allocate 0)) reply)
             (a/take! reply (fn [{:keys [error]}]
                              (when (and error (not (instance? IllegalStateException error)))
                                (dead! :send-failed error))))
             (recur))))))))

(defn- append-bytes!
  "Append the remaining bytes of `data` to `bin`. The JDK hands over direct buffers, so the bytes
  must be copied out first."
  [^ByteArrayOutputStream bin ^ByteBuffer data]
  (let [b (byte-array (.remaining data))]
    (.get data b)
    (.write bin b)))

(defn- listener
  "Return a listener that puts each complete message on `in` and reports the connection's end to
  `finish!`.

  The next frame is requested only once the previous message is on `in`. `quiet-since` holds when
  the next frame was requested, or nil while a message waits for the consumer, as silence then
  says nothing about the peer. A message over `max-size` (chars for text, bytes for binary) calls
  `too-big!` with the websocket. A message that finds `in` closed calls `close!` with the
  websocket; `close!` must ignore all but its first call. Either way the message is dropped and
  frames are still requested, so the peer's close still arrives."
  [in finish! close! too-big! quiet-since max-size]
  (let [text      (StringBuilder.)
        bin       (ByteArrayOutputStream.)
        dropping? (atom false)
        next!     (fn [^WebSocket w]
                    (reset! quiet-since (now))
                    (.request w 1))
        deliver!  (fn [^WebSocket w msg]
                    (reset! quiet-since nil)
                    (a/put! in msg (fn [open?]
                                     (when-not open?
                                       (close! w))
                                     (next! w))))
        overflow! (fn [w]
                    (reset! dropping? true)
                    (.setLength text 0)
                    (.reset bin)
                    (too-big! w)
                    (next! w))]
    (reify WebSocket$Listener
      (onOpen [_ w] (next! w))
      (onText [_ w data last?]
        (cond
          @dropping?                                     (next! w)
          (> (+ (.length text) (.length data)) max-size) (overflow! w)
          (and last? (zero? (.length text)))             (deliver! w (str data))
          last?                                          (let [s (.toString (.append text data))]
                                                           (.setLength text 0)
                                                           (deliver! w s))
          :else                                          (do (.append text data)
                                                             (next! w)))
        nil)
      (onBinary [_ w data last?]
        (cond
          @dropping?                                      (next! w)
          (> (+ (.size bin) (.remaining data)) max-size)  (overflow! w)
          (and last? (zero? (.size bin)))                 (let [b (byte-array (.remaining data))]
                                                            (.get data b)
                                                            (deliver! w b))
          last?                                           (do (append-bytes! bin data)
                                                              (let [b (.toByteArray bin)]
                                                                (.reset bin)
                                                                (deliver! w b)))
          :else                                           (do (append-bytes! bin data)
                                                              (next! w)))
        nil)
      (onPing [_ w _] (next! w) nil)
      (onPong [_ w _] (next! w) nil)
      (onClose [_ _ status reason]
        (finish! {:closed (if (= 1000 status) :normal :remote) :status status :reason reason})
        nil)
      (onError [_ _ t]
        (finish! {:closed :error :cause :network :error (unwrap t)})))))

(defn connect!
  "Open a websocket to `uri`. Return a promise-chan yielding the connection once open, or
  `{:error t}` if opening fails. Throw at once on a malformed `uri`, a non-ws(s) scheme, a
  restricted header or an unknown `:on-full-send-queue`.

  Options:
  - `:http-client`      `java.net.http.HttpClient`; default a shared client on two daemon
                        threads.
  - `:headers`          map of extra request headers.
  - `:connect-timeout`  ms; default 10000.
  - `:close-timeout`    ms from `close!` until the connection is aborted, unless it ended
                        first; default 5000.
  - `:ping-interval`    ms between pings; default 15000; an explicit nil disables liveness
                        checks.
  - `:pong-timeout`     ms past `:ping-interval` the peer may stay silent while messages are
                        being read; default 10000.
  - `:max-msgs-in-queue`   messages buffered on `:in`; default 10, a magic number higher than 1.
  - `:max-msgs-out-queue`  messages waiting to be sent; default 10, a magic number higher than 1.
  - `:on-full-send-queue`  what `send!` does when `:max-msgs-out-queue` messages already wait:
                           `:block-on-full-send-queue` (default) or `:abort-on-full-send-queue`.
  - `:max-message-size`    largest message accepted: chars for text, bytes for binary;
                           default 1048576.

  The connection is a map:
  - `:in`      complete messages: strings for text, byte arrays for binary. Frames are read only
               as fast as messages are taken, so a slow consumer slows the peer and nothing is
               dropped. Closes when the connection ends. Closing it also ends the connection, as
               `close!` would, but only once the next message arrives; prefer `close!`.
  - `:closed`  promise-chan; the one place the connection's end is reported. Yields:
               `{:closed :normal :status 1000 :reason s}`  close handshake, normal status
               `{:closed :remote :status n :reason s}`     close handshake, any other status
               `{:closed :aborted}`                        `abort!` called
               `{:closed :timeout}`                        `close!` did not end it in time
               `{:closed :error :cause c :error t}`        `c` is one of:
                 `:network`          network or protocol failure.
                 `:send-failed`      a send, ping or close failed or was refused.
                 `:send-queue-full`  `send!` found the queue full under
                                     `:abort-on-full-send-queue`.
                 `:message-too-big`  a message exceeded `:max-message-size`.
                 `:unresponsive`     the peer went silent."
  ([uri] (connect! uri {}))
  ([uri {:keys [http-client headers connect-timeout close-timeout ping-interval pong-timeout
                max-msgs-in-queue max-msgs-out-queue on-full-send-queue max-message-size]
         :or   {http-client        shared-http-client
                connect-timeout    10000 close-timeout      5000
                ping-interval      15000 pong-timeout       10000
                max-msgs-in-queue  10    max-msgs-out-queue 10
                on-full-send-queue :block-on-full-send-queue
                max-message-size   1048576}}]
   (when-not (#{:block-on-full-send-queue :abort-on-full-send-queue} on-full-send-queue)
     (throw (ex-info "Unknown :on-full-send-queue." {:on-full-send-queue on-full-send-queue})))
   (let [in          (a/chan max-msgs-in-queue)
         out         (a/chan max-msgs-out-queue)
         closed      (a/promise-chan)
         opened      (a/promise-chan)
         closing?    (atom false)
         quiet-since (atom nil)
         ;; `closed` is set before `out` closes: `send!` reads it once `out` refuses.
         finish!     (fn [outcome]
                       (a/put! closed outcome)
                       (a/close! in)
                       (a/close! out))
         abort-ws!   (fn [^WebSocket w outcome]
                       (finish! outcome)
                       (.abort w)
                       closed)
         fail-ws!    (fn [w cause t] (abort-ws! w {:closed :error :cause cause :error t}))
         ;; `put!`, not `offer!`: the close queues behind earlier sends even on a full queue.
         close-ws!   (fn [w status reason]
                       (when (and (compare-and-set! closing? false true)
                                  (a/put! out [#(.sendClose ^WebSocket % (int status) ^String reason)
                                               (a/promise-chan)]))
                         (a/go
                           (when-not (a/alt! closed true (a/timeout close-timeout) false)
                             (abort-ws! w {:closed :timeout}))))
                       closed)
         ;; The send queue is closed by `finish!`, so the close goes straight to the JDK. 1008, as
         ;; the JDK refuses to send 1009 (message too big) from a client.
         too-big!    (fn [^WebSocket w]
                       (reset! closing? true)
                       (finish! {:closed :error
                                 :cause  :message-too-big
                                 :error  (ex-info "Message too big."
                                                  {:limit max-message-size})})
                       (.sendClose w (int 1008) "Message too big.")
                       (a/go (a/<! (a/timeout close-timeout))
                             (.abort w)))
         ws-cf       (-> ^WebSocket$Builder (reduce-kv (fn [^WebSocket$Builder b k v] (.header b (name k) (str v)))
                                                       (.newWebSocketBuilder ^HttpClient http-client)
                                                       headers)
                         (.connectTimeout (Duration/ofMillis connect-timeout))
                         (.buildAsync (URI/create uri)
                                      (listener in finish! #(close-ws! % 1000 "") too-big!
                                                quiet-since max-message-size)))]
     ;; The JDK rejects a bad scheme or header with an already-failed future, not a throw.
     (when (.isCompletedExceptionally ^CompletableFuture ws-cf)
       (let [t (try (.join ^CompletableFuture ws-cf) (catch CompletionException e (unwrap e)))]
         (when (instance? IllegalArgumentException t)
           (throw t))))
     (.whenComplete ^CompletableFuture ws-cf
                    (reify BiConsumer
                      (accept [_ w t]
                        (if t
                          (do (finish! {:closed :error :cause :network :error (unwrap t)})
                              (a/put! opened {:error (unwrap t)}))
                          (let [fail! #(fail-ws! w %1 %2)]
                            (run-sends! w out #(fail! :send-failed %))
                            (when ping-interval
                              (keep-alive! w closed quiet-since ping-interval pong-timeout fail!))
                            (a/put! opened {:in                  in
                                            :closed              closed
                                            ::out                out
                                            ::on-full-send-queue on-full-send-queue
                                            ::fail!              fail!
                                            ::close              #(close-ws! w %1 %2)
                                            ::abort!             #(abort-ws! w aborted)}))))))
     opened)))

(defn send!
  "Queue `msg`, a string or byte array, behind earlier sends. Return a promise-chan yielding `:ok`
  once sent, or `{:error t}`. A byte array is copied, so the caller may reuse it at once.

  A failed send ends the connection with `{:closed :error :cause :send-failed}`. A send while
  `:max-msgs-out-queue` messages already wait follows `:on-full-send-queue`:
  - `:block-on-full-send-queue`  block the calling thread until the queue has room or drains
                                 after the connection ends. Do not call from a go block unless
                                 the queue has room.
  - `:abort-on-full-send-queue`  end the connection with
                                 `{:closed :error :cause :send-queue-full}`.

  Throw if `msg` is neither a string nor a byte array."
  [{:keys [closed] ::keys [out on-full-send-queue fail!]} msg]
  (let [reply (a/promise-chan)
        op    (cond
                (string? msg) #(.sendText ^WebSocket % ^CharSequence msg true)
                (bytes? msg)  (let [buf (ByteBuffer/wrap (aclone ^bytes msg))]
                                #(.sendBinary ^WebSocket % buf true))
                :else         (throw (ex-info "Message must be a string or byte array."
                                              {:type (type msg)})))]
    (cond
      (a/offer! out [op reply])
      nil

      (a/poll! closed)
      (a/put! reply {:error (ex-info "Connection closed." {})})

      (= :abort-on-full-send-queue on-full-send-queue)
      (let [e (ex-info "Send queue full." {})]
        (a/put! reply {:error e})
        (fail! :send-queue-full e))

      ;; A put still pending when `out` closes is taken, and failed, as `out` drains.
      (not (a/>!! out [op reply]))
      (a/put! reply {:error (ex-info "Connection closed." {})}))
    reply))

(defn close!
  "End the connection from this side: send a close with `status` and `reason` (1000 and \"\" by
  default) behind earlier sends; later calls send nothing. Return the `:closed` promise-chan.

  If the connection has not ended within `:close-timeout` of this call, e.g. because earlier sends
  are stalled or the peer does not answer, abort with `{:closed :timeout}`. A close the JDK
  refuses, e.g. an illegal `status`, ends the connection with
  `{:closed :error :cause :send-failed}`."
  ([conn] (close! conn 1000 ""))
  ([{::keys [close]} status reason] (close status reason)))

(defn abort!
  "Drop the connection without a close handshake. Return the `:closed` promise-chan, which yields
  `{:closed :aborted}` unless the connection had already ended."
  [{::keys [abort!]}]
  (abort!))
