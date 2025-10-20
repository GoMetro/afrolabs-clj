(ns spicyrun.ninja.lambda.utils
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [ring.middleware.cookies]
   [ring.util.codec])
  (:import
   [java.util Base64]))

(defn base64:decode
  [s]
  (let [enc (Base64/getDecoder)]
    (String. (.decode enc (.getBytes s)))))

(defn event->request
  "Transform lambda input to Ring requests. Has two extra properties:
   :event - the lambda input
   :context - an instance of a lambda context
              http://docs.aws.amazon.com/lambda/latest/dg/java-context-object.html"
  [event context]
  {:server-port    (:server-port event)
   :body           (when-let [b (:body event)]
                     (let [b' (if (:isBase64Encoded event)
                               (base64:decode b)
                               b)]
                       (io/input-stream (.getBytes b'))))
   :server-name    (get-in event [:headers :host])
   :remote-addr    (get-in event [:requestContext :http :sourceIp] "")
   :uri            (:rawPath event)
   :query-string   (:rawQueryString event)
   :scheme         (keyword (get-in event [:headers :x-forwarded-proto]))
   :request-method (keyword (str/lower-case (get-in event [:requestContext :http :method] "")))
   :protocol       (-> event :requestContext :http :protocol)
   :headers        (update-keys (:headers event)
                                name)

   ;; lambda cookies wtf
   :cookies        (let [request-cookies (:cookies event)
                         parsed-cookies
                         (into {}
                               (map (fn [c]
                                      (#'ring.middleware.cookies/parse-cookies {:headers {"cookie" c}}
                                                                               ring.util.codec/form-decode-str)))
                               request-cookies)]
                     parsed-cookies)
   :event          event
   :context        context})
