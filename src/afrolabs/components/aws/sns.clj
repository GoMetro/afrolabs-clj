(ns afrolabs.components.aws.sns
  (:require [afrolabs.components :as -comp]
            [afrolabs.components.aws :as -aws]
            [cognitect.aws.client.api :as aws]
            [integrant.core :as ig]
            [clojure.string :as str]
            [net.cgrand.xforms :as x]
            [clojure.data.json :as json]
            [clojure.spec.alpha :as s]))

(defn query-all-topics
  "Returns a sequence of all the sns topics that this actor has access to. Lazily and recursively pages through the results using NextToken.

  eg: (query-all-topics sns-client)"
  [{:keys [sns-client]}
   & {:keys [NextToken]}]
  (let [{:keys [Topics
                NextToken]}
        (aws/invoke sns-client (cond-> {:op :ListTopics}
                                 NextToken (assoc :request {:NextToken NextToken})))]
    (if-not NextToken Topics
            (concat Topics
                    (lazy-seq (query-all-topics sns-client
                                                :NextToken NextToken))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::sns-client-cfg (s/keys :req-un [::-aws/aws-creds-component
                                         ::-aws/aws-region-component]))

(-comp/defcomponent {::-comp/config-spec ::sns-client-cfg
                     ::-comp/ig-kw       ::sns-client}
  [{:keys [aws-creds-component
           aws-region-component]}]
  (let [state {:sns-client (aws/client {:api                  :sns
                                        :credentials-provider aws-creds-component
                                        :region               (:region aws-region-component)})}]
    (reify
      -comp/IHaltable
      (halt [_] (aws/stop (:sns-client state)))

      clojure.lang.IDeref
      (deref [_] state))))
