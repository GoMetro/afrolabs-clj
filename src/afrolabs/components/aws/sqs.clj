(ns afrolabs.components.aws.sqs
  (:require [afrolabs.components :as -comp]
            [afrolabs.components.aws :as -aws]
            [cognitect.aws.client.api :as aws]
            [clojure.spec.alpha :as s]
            [clojure.core.async :as csp]
            [taoensso.timbre :as log]))

(def default-visibility-time-seconds 30)

(defn upsert-queue!
  "Upserts an AWS SQS queue."
  [{:keys [sqs-client]}
   & {:keys [QueueName
             visibility-time-seconds
             fifo?]}]

  (let [{:keys [QueueUrl]}
        (aws/invoke @sqs-client
                    {:op      :CreateQueue
                     :request {:QueueName  QueueName
                               :Attributes (cond-> {"VisibilityTimeout" (str (or visibility-time-seconds
                                                                                 default-visibility-time-seconds))}
                                             fifo? (assoc "FifoQueue" "true"))}})

        {{:strs [QueueArn]} :Attributes}
        (aws/invoke @sqs-client
                    {:op      :GetQueueAttributes
                     :request {:QueueUrl       QueueUrl
                               :AttributeNames ["QueueArn"]}})]
    {:QueueUrl QueueUrl
     :QueueArn QueueArn}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol ISqsClient)
(s/def ::sqs-client #(satisfies? ISqsClient %))

;;;;;;;;;;;;;;;;;;;;

(s/def ::sqs-client-cfg (s/keys :req-un [::-aws/aws-creds-component
                                         ::-aws/aws-region-component]))

(-comp/defcomponent {::-comp/config-spec ::sqs-client-cfg
                     ::-comp/ig-kw       ::sqs-client}
  [{:as cfg
    :keys [aws-creds-component
           aws-region-component]}]

  (let [state (aws/client {:api                  :sqs
                           :credentials-provider aws-creds-component
                           :region               (:region aws-region-component)})]
    (reify
      -comp/IHaltable
      (halt [_] (aws/stop state))


      ;; marker protocol
      ISqsClient

      clojure.lang.IDeref
      (deref [_] state))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol ISqsConsumerClient
  "A protocol for a sqs message handler."
  (consume-message [_ msg] "Consumes a single message. When this method returns without exception, the message will be deleted."))

(s/def ::sqs-consumer-client #(satisfies? ISqsConsumerClient %))
(s/def ::QueueUrl (s/and string?
                         #(pos-int? (count %))))
(s/def ::wait-time-seconds int?)
(s/def ::max-nr-of-messages (s/and pos-int?
                                   #(> 11 %)))
(s/def ::sqs-consumer-cfg (s/keys :req-un [::sqs-consumer-client
                                           ::sqs-client
                                           ::QueueUrl]
                                  :opt-un [::wait-time-seconds
                                           ::max-nr-of-messages]))

(comment

  (s/check-asserts true)

  )

(defn consumer-main
  [must-run
   {:keys [sqs-client
           sqs-consumer-client
           QueueUrl
           wait-time-seconds
           max-nr-of-messages]
    :or   {wait-time-seconds  5 ;; 5 seconds is not really that long. could easily be 30 seconds and would save on compute resources
           max-nr-of-messages 5}
    :as   sqs-consumer-cfg}]
  (s/assert ::sqs-consumer-cfg sqs-consumer-cfg)
  (while @must-run
    (let [{msgs :Messages
           :as  sqs-invoke-result} ;; ask for messages from the queue with a long-polling http call
          (aws/invoke @sqs-client ;; consumer-main needs intimate knowledge of the sqs-client's dereffed state value to be able to pull this off
                      {:op      :ReceiveMessage
                       :request {:QueueUrl            QueueUrl
                                 :WaitTimeSeconds     wait-time-seconds
                                 :MaxNumberOfMessages max-nr-of-messages}})

          successes (->> msgs
                         (map #(assoc % :status
                                      (try (consume-message sqs-consumer-client
                                                            %)

                                        ;; we're still here, that means it was a :success
                                        :success

                                        (catch Throwable t

                                          (log/warn t "Error while consuming a message from sqs.")

                                          ;; ok, no, it was a failure instead
                                          ;; failures must not be deleted from the queue, they will just expire
                                          ;; and another process will pick this message up again for a retry
                                          :failure))))
                         (filter #(= (:status %)
                                     :success)))]

      ;; delete all the messages we just processed :success-fully
      (doseq [{receipt-handle :ReceiptHandle}
              successes]
        (aws/invoke @sqs-client
                    {:op      :DeleteMessage
                     :request {:QueueUrl      QueueUrl
                               :ReceiptHandle receipt-handle}})))))

(-comp/defcomponent {::-comp/config-spec ::sqs-consumer-cfg
                     ::-comp/ig-kw       ::sqs-consumer}
  [cfg]
  (let [must-run (atom true)
        worker (csp/thread (consumer-main must-run cfg)
                           nil)]
    (reify
      -comp/IHaltable
      (halt [_]
        (reset! must-run false)
        (csp/<!! worker)))))
