(ns afrolabs.csp
  (:require [clojure.core.async :as csp]
            [taoensso.timbre :as log]
            [net.cgrand.xforms :as x]))

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
