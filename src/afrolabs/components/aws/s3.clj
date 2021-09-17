(ns afrolabs.components.aws.s3
  (:require [afrolabs.components :as -comp]
            [afrolabs.components.aws :as -aws]
            [cognitect.aws.client.api :as aws]
            [clojure.spec.alpha :as s])
  (:import [java.security MessageDigest]
           [java.util Base64]))

;;;;;;;;;;;;;;;;;;;;

(defn aws-contentmd5
  [^String s]
  (let [md5-algo (MessageDigest/getInstance "MD5")]
    (as-> s $
        (.getBytes $ "UTF-8")
        (.digest md5-algo $)
        (.encodeToString (Base64/getEncoder) $))))

;;;;;;;;;;;;;;;;;;;;

(defn put-string-object
  "One simple implementation of AWS PutObject call, for uploading Strings to S3 keys."
  [s3-client bucket key ^String content content-type]
  (let [content-bytes (.getBytes content "UTF-8")]
    (-aws/throw-when-anomaly
     (aws/invoke @s3-client
                 {:op      :PutObject
                  :request {:Bucket        bucket
                            :Key           key
                            :ContentType   content-type
                            :ContentLength (count content-bytes)
                            :ContentMD5    (aws-contentmd5 content)
                            :Body          content}}))))

(defn put-object-streaming
  "Uploads an inputstream to a key in S3."
  [s3-client bucket key input-stream content-type]
  (-aws/throw-when-anomaly
   (aws/invoke @s3-client
               {:op      :PutObject
                :request {:Bucket      bucket
                          :Key         key
                          :ContentType content-type
                          :Body        input-stream
                          ;; TODO - If you stream to a file first, you can calculate the length and the md5
                          ;; but if you just want to stream directly (eg you are creating the stream as you go)
                          ;; then you cannot calculate the md5 and the length in advance
                          ;; :ContentLength (count content-bytes)
                          ;; :ContentMD5    (aws-contentmd5 content)
                          }})))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(comment

  (aws/doc @(yoco.sns2kafka.core/s3-client) :PutObject   )

  (-aws/throw-when-anomaly
   (put-string-object (yoco.sns2kafka.core/s3-client)
                      "aws-euw1-dev-kagera-kafka-poc"
                      "test.txt"
                      "testing testing"
                      "text/plain"
                      ))

  )

;;;;;;;;;;;;;;;;;;;;


(s/def ::s3-client-cfg (s/keys :req-un [::-aws/aws-creds-component
                                        ::-aws/aws-region-component]))

(-comp/defcomponent {::-comp/ig-kw       ::s3-client
                     ::-comp/config-spec ::s3-client-cfg}
  [{:keys [aws-creds-component
           aws-region-component]}]
  (let [state (aws/client {:api                  :s3
                           :credentials-provider aws-creds-component
                           :region               (:region aws-region-component)})]
    (reify
      -comp/IHaltable
      (halt [_] (aws/stop state))

      clojure.lang.IDeref
      (deref [_] state))))


