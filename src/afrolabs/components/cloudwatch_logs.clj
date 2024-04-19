(ns afrolabs.components.cloudwatch-logs
  "Exports a component for integration with timbre that exports logs to cloudwatch. "
  (:require
   [afrolabs.components.aws.sso :as -aws-sso-profile-provider]
   [afrolabs.spec :as -spec]
   [clojure.core.async :as csp]
   [clojure.data.json :as json]
   [clojure.spec.alpha :as s]
   [cognitect.aws.client.api :as aws]
   [cognitect.aws.credentials :as aws-creds]
   [integrant.core :as ig]
   [java-time :as t]
   [taoensso.timbre :as log]
   [timbre-json-appender.core]
   ))

(comment

  (def cloudwatch
    (aws/client {:api                  :logs
                 :credentials-provider (-aws-sso-profile-provider/provider "AdministratorAccess-381491896970-GoMetro-Bridge-Dev")
                 :region               "af-south-1"}))
  (sort (keys (aws/ops cloudwatch)))

  (aws/doc cloudwatch :CreateLogStream)
  (aws/doc cloudwatch :PutLogEvents)


  )

(defn create-log-stream
  [client group stream]
  (aws/invoke client
              {:op      :CreateLogStream
               :request {:logGroupName  group
                         :logStreamName stream}}))

(defn put-log-events
  [client group stream log-events]
  (aws/invoke client
              {:op      :PutLogEvents
               :request {:logGroupName  group
                         :logStreamName stream
                         :logEvents     log-events}}))

(comment

  (create-log-stream cloudwatch "pieter-test" "test-1")
  (put-log-events cloudwatch "pieter-test" "test-1" [{:timestamp (.toEpochMilli ^java.time.Instant (t/instant))
                                                      :message (json/write-str {:hello "world"})}
                                                     {:timestamp (.toEpochMilli ^java.time.Instant (t/instant))
                                                      :message (json/write-str {:my "name"})}
                                                     {:timestamp (.toEpochMilli ^java.time.Instant (t/instant))
                                                      :message (json/write-str {:is "pieter"})}])

  )

;; from https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
(def ^:const max-payload-size 1045576)
(def ^:const max-messages-per-payload 10000)
(def ^:const max-message-size (* 256 1024))

;; we will batch the messages for the cloudwatch limits
;; - max batch size is 1 045 576 bytes, which is UTF8 of the message + 26 bytes per message
;; - max of 10 000 messages per batch
(defn make-batches
  [log-events]
  (let [{:keys [current-batch
                earlier-batches]}
        (reduce (fn [acc log-event']
                  (let [msg-bytes (.getBytes ^String (:message log-event') "UTF-8")
                        msg-size' (count msg-bytes)

                        [msg-size actual-message]
                        (if-not (< max-message-size msg-size')
                          [msg-size' (:message log-event')]
                          (do
                            (log/debugf "Attempt to log a message that has size (%d) which is larger than the max-message-size (%d). Message truncated."
                                        msg-size'
                                        max-message-size)
                            [max-message-size
                             (String. (into-array Byte/TYPE (take max-message-size msg-bytes)))]))

                        log-event (assoc log-event' :message actual-message)]
                    (if (or
                         ;; will the new message push the current batch over the payload size limit?
                         (<= max-payload-size
                             (+ (:current-batch-payload-size acc)
                                (+ 26 msg-size)))
                         ;; is this the max-messages-per-payload-th message in this batch?
                         (= max-messages-per-payload
                            (count (:current-batch acc))))
                      {:current-batch              [log-event]
                       :current-batch-payload-size (+ 26 msg-size)
                       :earlier-batches            (conj (:earlier-batches acc)
                                                         (:current-batch acc))}
                      (-> acc
                          (update :current-batch conj log-event)
                          (update :current-batch-payload-size + 26 msg-size)))))
                {:current-batch              []
                 :current-batch-payload-size 0
                 :earlier-batches            []}
                log-events)]
    (conj earlier-batches current-batch)))

(defn create-log-dispatcher
  "Returns a function, that accepts a single log-event.
  This log-event will be batched with others and uploaded to cloudwatch every batch-period ms.
  When called with `:stop` as a paremeter, will stop the internal thread and therefore stop uploading logs.
  Each log-event must have a string :message and :timestamp with epoch-ms."
  [client group stream & {:as   _opts
                          :keys [batch-period-ms]
                          :or   {batch-period-ms 1000}}]
  (let [enqued-log-events (atom [])
        stop-signal (csp/chan)]

    (csp/thread
      (loop [to (csp/timeout batch-period-ms)]
        ;; do uploading
        (let [[msgs _] (swap-vals! enqued-log-events (constantly []))
              batches (make-batches msgs)]
          (doseq [batch batches]
            (put-log-events client group stream batch)))
        ;; wait for batch-period of time, or until we receive the signal to stop
        (let [[_ ch] (csp/alts!! [stop-signal to])]
          (when-not (= ch stop-signal)
            (recur (csp/timeout batch-period-ms)))))
      (log/debug "Done with aws cloudwatch log dispatcher thread."))


    (fn [log-event]
      (if (not= log-event :stop)
        (swap! enqued-log-events conj log-event)
        (csp/close! stop-signal)))))

(comment

  (def stop
    (let [stop? (atom false)]
      (csp/thread
        (let [dispatch-log (create-log-dispatcher cloudwatch "pieter-test" "test-1")]
          (loop [x 0]
            (Thread/sleep 100)
            (dispatch-log {:message   (str "testing 2: " x)
                           :timestamp (.toEpochMilli ^java.time.Instant (t/instant))})
            (when (not @stop?)
              (recur (inc x))))
          (dispatch-log :stop)))
      (fn [] (reset! stop? true))))
  (stop)

  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; component
;;
;; There is probably no need to have more than once of this component, so no -comp/defcomponent for this one.

(s/def ::non-empty-string (s/and string?
                                 #(pos? (count %))))
(s/def ::log-group ::non-empty-string)
(s/def ::log-stream ::non-empty-string)
(s/def ::create-log-stream? boolean?)
(s/def ::disabled? boolean?)
(s/def ::aws-region ::non-empty-string)
(s/def ::aws-access-key-id (s/nilable ::non-empty-string))
(s/def ::aws-secret-access-key (s/nilable ::non-empty-string))

(s/def ::install-timbre-appender-cfg
  (s/keys :req-un [::log-group
                   ::log-stream
                   ::create-log-stream?
                   ::disabled?]
          :opt-un [::aws-access-key-id
                   ::aws-secret-access-key
                   ::aws-region]))

(defmethod ig/init-key ::install-timbre-appender
  [_ cfg]
  (-spec/assert! ::install-timbre-appender-cfg cfg)
  (let [{:keys [log-group
                log-stream
                create-log-stream?
                disabled?
                aws-region
                aws-access-key-id
                aws-secret-access-key]} cfg]
    (if disabled?
      {:dispatch-fn (constantly nil)}
      (let [client-cfg (cond-> {:api    :logs
                                :region aws-region}

                         (and aws-access-key-id
                              aws-secret-access-key)
                         (assoc :credentials-provider
                                (aws-creds/basic-credentials-provider
                                 {:access-key-id     aws-access-key-id
                                  :secret-access-key aws-secret-access-key}))

                         (not (and aws-access-key-id
                                   aws-secret-access-key))
                         (assoc :credentials-provider
                                (aws-creds/chain-credentials-provider
                                 [(aws-creds/default-credentials-provider (aws/default-http-client))
                                  ;; this crazy shit provides a work-around because
                                  ;; cognitect's profile credentials provider does not work for sso.
                                  ;; We are adding it at the end of the chain.
                                  (-aws-sso-profile-provider/provider (or (System/getenv "AWS_PROFILE")
                                                                          (System/getProperty "aws.profile")
                                                                          "default"))])))
            client (aws/client client-cfg)
            _ (when create-log-stream? (create-log-stream client log-group log-stream))
            dispatch-fn (create-log-dispatcher client log-group log-stream)]

        (log/merge-config! {:appenders {:aws {:enabled?  true
                                              :async?    false
                                              :min-level nil
                                              :output-fn (timbre-json-appender.core/make-json-output-fn {})
                                              :fn        (fn [{:keys [output_ ^java.util.Date instant]}]
                                                           (dispatch-fn {:message   output_
                                                                         :timestamp (.getTime instant)}))}}})
        (assoc cfg :dispatch-fn dispatch-fn)))))

(defmethod ig/halt-key! ::install-timbre-appender
  [_ {:as _state
      :keys [dispatch-fn]}]
  (dispatch-fn :stop)
  (log/merge-config! {:appenders {:aws nil}}))

(comment

  (def system (ig/init {::install-timbre-appender {:log-group "pieter-test"
                                                   :log-stream "test-2"
                                                   :create-log-stream? true
                                                   :disabled? false
                                                   :aws-region "af-south-1"}}))
  )
