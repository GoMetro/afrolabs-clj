(ns afrolabs.csp
  (:require [clojure.core.async :as csp]
            [taoensso.timbre :as log]
            [net.cgrand.xforms :as x]
            [afrolabs.components.kafka :as -kafka]
            [clojure.core.async.impl.protocols  :as csp-protocols]
            [afrolabs.components :as -comp]
            [taoensso.timbre :as log])
  (:import [org.apache.kafka.clients.consumer KafkaConsumer]
           [clojure.lang Counted]))

(defn partition-by-interval
  "Partitions the values on a channel based on time intervals.

  Takes an optional transducer which will transform the values"
  ([input-ch output-ch interval]
   (partition-by-interval input-ch nil output-ch interval))
  ([input-ch xform output-ch interval]

   (letfn [(wait-for-data
             []
             (csp/go
               (let [v (csp/<! input-ch)]
                 (when v [(list v) (csp/timeout interval)]))))

           (wait-for-more-data
             [data timeout]
             (csp/go-loop
                 [data data]
               (csp/alt!
                 timeout data
                 input-ch ([v] (if-not v data ;; v is nil when the input channel has been closed
                                       (recur (conj data v)))))))

           (control-loop
             []
             (csp/go-loop []
               (when-let [[data timeout] (csp/<! (wait-for-data))]
                 (let [data (csp/<! (wait-for-more-data data timeout))]
                   (if xform
                     (csp/<! ;; wait for onto-chan! to finish
                      (csp/onto-chan! output-ch
                                      (sequence xform
                                                (reverse data))
                                      false))
                     (csp/>! output-ch (reverse data)))
                   (recur)))))]

     (csp/go (csp/<! (control-loop))
             (csp/close! output-ch)))))


(comment

  (def input-ch (csp/chan))
  (def output-ch (csp/chan))

  (def worker-thread (csp/go-loop
                         [v (csp/<! output-ch)]
                       (when v
                         (log/info (with-out-str (clojure.pprint/pprint v)))
                         (recur (csp/<! output-ch)))))

  (csp/>!! output-ch [:a :b :c])

  (def partitioner (partition-by-interval input-ch
                                          output-ch
                                          10000))

  (def partitioner (partition-by-interval input-ch
                                          (x/reduce +)
                                          output-ch
                                          10000))

  (csp/go
    (loop
        [[h & r] (range 15)]
        (when h
          (csp/>! input-ch h)
          (csp/<! (csp/timeout 1000))
          (recur r)))
    (csp/close! input-ch)
    #_(log/info "closed the input."))

  ;;;;;;;;;;;;;;;;;;;;

  (csp/go (csp/<! partitioner)
          (log/debug "Partitioner completed."))

  (csp/close! input-ch)

  (csp/go (csp/<! worker-thread)
          (log/info "worker-thread done."))

  ;;;;;;;;;;;;;;;;;;;;

  (csp/go (if (csp/>! input-ch 1)
            (log/info "Placed the item")
            (log/info "Cannot place, channel closed.")))




  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; channel backed by kafka topic

(defn kafka-topic-buffer
  [& {:keys [bootstrap-server
             strategies]
      :as cfg}]
  (let [#_#_producer (-kafka/make-producer cfg)

        old-pause-consumer? (atom false)
        pause-consumer? (atom false)
        pause! (fn [] (reset! pause-consumer? true))
        unpause! (fn [] (reset! pause-consumer? false))

        consumer-msgs-ch (csp/chan)
        ready-to-remove-buffer (csp/buffer 1)
        ready-to-remove-ch (csp/chan ready-to-remove-buffer)

        transport-thread (csp/thread
                           (loop []
                             (when-let [msgs (csp/<!! consumer-msgs-ch)]
                               (log/debug "Received msgs...")
                               (csp/onto-chan!! ready-to-remove-ch msgs false)
                               (log/debug "Done with onto-chan!!")
                               (recur)))
                           (log/debug "Done with transport thread."))

        consumer-client (reify

                          -kafka/IPostConsumeHook
                          (post-consume-hook [_ consumer _]
                            (let [old-pause @old-pause-consumer?
                                  new-pause @pause-consumer?]
                              (when (not= old-pause new-pause)
                                (let [current-assignment (.assignment ^KafkaConsumer consumer)]
                                  (if new-pause
                                    (do
                                      (log/debug "Pausing on the kafkaconsumer...")
                                      (.pause ^KafkaConsumer consumer
                                              current-assignment))
                                    (do
                                      (log/debug "Unpausing on the kafkaconsumer...")
                                      (.resume ^KafkaConsumer consumer
                                                 current-assignment)))
                                  (reset! old-pause-consumer? new-pause)))))

                          -kafka/IConsumerClient
                          (consume-messages [_ msgs]
                            (when (seq msgs)
                              (log/debugf "Received %d msgs." (count msgs))
                              (let [placed (csp/go (csp/>! consumer-msgs-ch msgs))
                                    to (csp/timeout 1000)
                                    [v p] (csp/alts!! [placed to])]

                                (log/debug (condp = p
                                             placed "Placed"
                                             to "Timeout"))

                                (when (= p to)
                                  (do
                                    (log/debug "pausing...")
                                    (pause!)
                                    (csp/go (csp/<! placed)
                                            (log/debug "unpaused.")
                                            (unpause!))))))))
        consumer (-kafka/make-consumer (assoc cfg
                                              :consumer/client consumer-client))]

    (reify
      csp-protocols/UnblockingBuffer
      csp-protocols/Buffer

      (full? [_] false)
      (remove! [_] (csp-protocols/remove! ready-to-remove-buffer))
      (add!* [b itm] (throw (ex-info "Not implemented!" {})))
      (close-buf! [b]
        (-comp/halt consumer)
        (csp/close! consumer-msgs-ch)
        (csp/<!! transport-thread))

      Counted
      (count [_] (count ready-to-remove-buffer)))))

(comment

  (def b (kafka-topic-buffer :bootstrap-server "localhost:9092"
                             :strategies [[:strategy/JsonSerializer :consumer :both]
                                          [:strategy/SubscribeWithTopicsCollection ["buffer-topic"]]
                                          [:strategy/ConsumerGroup "buffer-topic-1"]
                                          [:strategy/AutoCommitOffsets]]))

  (csp-protocols/close-buf! b)

  (def ch (csp/chan b))

  (csp/go (println (str "Consumed with value: " (csp/<! ch))))


  )
