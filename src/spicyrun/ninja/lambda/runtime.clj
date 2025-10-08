(ns spicyrun.ninja.lambda.runtime
  (:gen-class)
  (:require
    [clojure.string :as str]
    [cheshire.core :as json]
    [org.httpkit.client :as http]
    [taoensso.timbre :as log]))

(defn- runtime-url
  [runtime-api path]
  (str "http://" runtime-api "/2018-06-01/runtime/invocation/" path))

(defn runtime-request-map
  [{:as   _request
    :keys [body
           path]} host]
  (cond-> {:method  (if body
                      :post
                      :get)
           :url     (runtime-url host path)
           :timeout 900000}
    body (assoc :body (json/encode body)
                :headers {"Content-Type" "application/json"})))

(defn get-runtime-api
  [host]
  (fn [path & [body]]
    (-> {:path path :body body}
        (runtime-request-map host)
        http/request
        deref
        (update :body json/decode keyword))))

(defn request-id [req]
  (get-in req [:headers :lambda-runtime-aws-request-id]))

(defn- send-error [runtime-api req error]
  (when-let [id (request-id req)]
    (runtime-api (str id "/error")
                 (Throwable->map error))))

(defn- get-lambda-invocation-request [runtime-api]
  (let [lambda-request (runtime-api "next")]
    (when-let [error (:error lambda-request)]
      (send-error runtime-api lambda-request (ex-info "Lambda runtime error, bad request."
                                                      {:error error}))
      ;; if we get an error at this point something is drastically wrong and we
      ;; should just abandon this and let the process crash
      (throw (Exception. (str error))))
    lambda-request))

(defn- send-response [runtime-api req response-body]
  (runtime-api (str (request-id req) "/response") response-body))

(defn init [handler-fn context]
  (let [runtime-api (get-runtime-api (System/getenv "AWS_LAMBDA_RUNTIME_API"))]
    (loop [{:as req :keys [body]} (get-lambda-invocation-request runtime-api)]
      (try
        (send-response runtime-api req
                       (try
                         (handler-fn body context)
                         (catch Exception e
                           ;; Do something with the stacktrace here.
                           ;; (clojure.stacktrace/print-stack-trace e)
                           (throw (ex-info "Handler Exception" {:exception (Throwable->map e)
                                                                :event     body
                                                                :context   context})))))
        (catch Exception e
          (send-error runtime-api req e)))
      (recur (get-lambda-invocation-request runtime-api)))))

;;;;;;;;;;;;;;;;;;;;

;; for babashka
(defn resolve-handler-fn [handler code-dir]
  (let [[namespace fn-name] (str/split handler #"/")
        file-path (str code-dir
                       "/"
                       (-> namespace
                           (str/replace "." "/")
                           (str/replace "-" "_")
                           (str ".clj")))
        _ (load-file file-path)]
    (get (ns-publics (symbol namespace)) (symbol fn-name))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn -read-non-empty-var [envvar]
  (let [s (System/getenv envvar)]
    (when (and s (not (zero? (count s))))
      s)))

;; this assumes that the code can be loaded as in JVM
(defn -main [& _]
  (let [handler-s (or (-read-non-empty-var "_HANDLER")
                      (-read-non-empty-var "__HANDLER"))
        _ (log/debug (str (System/getenv)))
        _ (when-not handler-s
            (throw (ex-info "This lambda requires _HANDLER environment variable."
                            {})))
        _ (log/info (str "Using '" handler-s "' as the symbol for the handler."))
        handler-fn (requiring-resolve (symbol handler-s))

        ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
        ;; for babashka
        ;; code-dir (or (System/getenv "LAMBDA_TASK_ROOT") "./")
        ;; ;; add the function's zip folder/file to the classpath
        ;; ;; we do this so that relative require's may work
        ;; _ (cp/add-classpath code-dir)
        ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

        context (System/getenv)]
    (if handler-fn
      (init handler-fn context)
      (throw (Exception. (format "Lambda handler '%s' not found" handler-s))))))
