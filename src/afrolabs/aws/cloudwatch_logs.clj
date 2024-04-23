(ns afrolabs.aws.cloudwatch-logs
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
  (aws/doc cloudwatch :DescribeLogStreams)


  )

(defn assert-log-stream
  "Creates a log stream if it does not exist."
  [client group stream]
  (let [{:as res
         :keys [cognitect.aws.error/code]}
        (aws/invoke client
                    {:op      :CreateLogStream
                     :request {:logGroupName  group
                               :logStreamName stream}})]
    (when-not (= code "ResourceAlreadyExistsException")
      res)))

(defn put-log-events
  [client group stream log-events]
  (aws/invoke client
              {:op      :PutLogEvents
               :request {:logGroupName  group
                         :logStreamName stream
                         :logEvents     log-events}}))

(comment

  (assert-log-stream cloudwatch "pieter-test" "test-1")
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::non-empty-string (s/and string?
                                 #(pos? (count %))))
(s/def ::log-group ::non-empty-string)
(s/def ::log-stream ::non-empty-string)
(s/def ::create-log-stream? boolean?)
(s/def ::batch-period-ms pos-int?)
(s/def ::client any?)

(s/def ::create-log-dispatcher-args
  (s/cat :client ::client
         :group  ::log-group
         :stream ::log-stream
         :opts   (s/keys :opt-un [::batch-period-ms
                                  ::create-log-stream?])))

(comment
  (s/valid? ::create-log-dispatcher-args
            [{} "group" "stream"
             {:batch-period-ms 1000
              :create-log-stream? true}])
  )


(defn create-log-dispatcher
  "Returns a function, that accepts a single log-event.
  This log-event will be batched with others and uploaded to cloudwatch every batch-period ms.
  When called with `:stop` as a paremeter, will stop the internal thread and therefore stop uploading logs.
  Each log-event must have a string :message and :timestamp with epoch-ms."
  [client group stream & {:as   opts
                          :keys [batch-period-ms
                                 create-log-stream?]
                          :or   {batch-period-ms    1000
                                 create-log-stream? true}}]
  (-spec/assert! ::create-log-dispatcher-args [client group stream opts])
  (let [enqued-log-events (atom [])
        stop-signal (csp/chan)]

    (when create-log-stream?
      (let [res (assert-log-stream client group stream)]
        (when (:cognitect.anomalies/category res)
          (throw (ex-info (format "Unable to create the log stream. Does the log-group (%s) exist?" group)
                          res)))))

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

