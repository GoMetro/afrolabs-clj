(ns afrolabs.components.aws.sns
  (:require [afrolabs.components :as -comp]
            [afrolabs.components.aws :as -aws]
            [cognitect.aws.client.api :as aws]
            [clojure.spec.alpha :as s]
            [clojure.core.async :as csp]))

(defn query-all-topics
  "Returns a sequence of all the sns topics that this actor has access to. Lazily and recursively pages through the results using NextToken.

  eg: (query-all-topics {:sns-client sns-client})"
  [{:keys [sns-client]}
   & {:keys [NextToken]}]
  (let [{:keys [Topics
                NextToken]}
        (-aws/backoff-and-retry-on-rate-exceeded
         (aws/invoke @sns-client (cond-> {:op :ListTopics}
                                   NextToken (assoc :request {:NextToken NextToken}))))]
    (if-not NextToken Topics
            (concat Topics
                    (lazy-seq (query-all-topics {:sns-client sns-client}
                                                :NextToken NextToken))))))

;;;;;;;;;;;;;;;;;;;;

(defn query-topic-subscriptions
  "Returns the subscriptions for a specific topic.

  eg: (query-topic-subscriptions {:sns-client sns-client} :TopicArn \"topic-arn\")"
  [{:keys [sns-client] :as cfg}
   & {:keys [NextToken
             TopicArn]}]
  (let [{:keys [Subscriptions
                NextToken]}      (-aws/throw-when-anomaly
                                  (-aws/backoff-and-retry-on-rate-exceeded
                                   (aws/invoke @sns-client
                                               {:op :ListSubscriptionsByTopic
                                                :request (cond-> {:TopicArn TopicArn}
                                                           NextToken (assoc :NextToken NextToken))})))]
    (if-not NextToken Subscriptions
            (concat Subscriptions
                    (lazy-seq (query-topic-subscriptions cfg
                                                         :TopicArn  TopicArn
                                                         :NextToken NextToken))))))

(comment

  (def subs (query-topic-subscriptions {:sns-client @(yoco.sns2kafka.core/sns-client)}
                                       :TopicArn "arn:aws:sns:eu-west-1:570343045431:camel-staging-Business-Update-Happened"))

  (aws/ops @(yoco.sns2kafka.core/sns-client))

  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::sns-client-cfg (s/keys :req-un [::-aws/aws-creds-component
                                         ::-aws/aws-region-component]))

(defn make-sns-client
  [{:keys [aws-creds-component
           aws-region-component]}]
  (let [state (aws/client {:api                  :sns
                           :credentials-provider aws-creds-component
                           :region               (:region aws-region-component)})]
    (reify
      -comp/IHaltable
      (halt [_] (aws/stop state))

      clojure.lang.IDeref
      (deref [_] state))))

(-comp/defcomponent {::-comp/config-spec ::sns-client-cfg
                     ::-comp/ig-kw       ::sns-client}
  [cfg] (make-sns-client cfg))
