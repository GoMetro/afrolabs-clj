(ns afrolabs.components.aws.athena
  "Athena query client component.

  Wraps a cognitect aws-api Athena client as an Integrant component and carries the
  query defaults shared across a service: the workgroup, the S3 result output location,
  and the default catalog and database.

  Athena runs SQL over data in S3 (typically parquet) against the external-table
  definitions registered in the Glue data catalog."
  (:require
   [afrolabs.components :as -comp]
   [afrolabs.components.aws :as -aws]
   [cognitect.aws.client.api :as aws]
   [clojure.spec.alpha :as s]
   [taoensso.timbre :as log]
   [clojure.core.async :as csp]
   [failjure.core :as f])
  (:import
   [java.time LocalDate LocalDateTime ZoneOffset]
   [java.time.format DateTimeFormatterBuilder]
   [java.time.temporal ChronoField]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Configuration

(s/def ::aws-client-config (s/and map? #(:credentials-provider %)))
(s/def ::workgroup (s/nilable (s/and string? (comp pos? count))))
(s/def ::output-location (s/nilable (s/and string? (comp pos? count))))
(s/def ::catalog (s/nilable (s/and string? (comp pos? count))))
(s/def ::database (s/nilable (s/and string? (comp pos? count))))

(s/def ::athena-client-cfg
  (s/keys :req-un [::aws-client-config]
          :opt-un [::workgroup
                   ::output-location
                   ::catalog
                   ::database]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(defrecord AthenaClient [client workgroup output-location catalog database]
  -comp/IHaltable
  (halt [_] (aws/stop client))

  ;; Derefs to the underlying aws-api client, so callers can invoke Athena operations
  ;; this namespace does not wrap.
  clojure.lang.IDeref
  (deref [_] client))

(defn make-athena-client
  "Build the Athena client component from its resolved configuration."
  [{:keys [aws-client-config workgroup output-location catalog database]}]
  (log/info "Creating Athena client component.")
  (map->AthenaClient
   {:client          (aws/client (merge aws-client-config {:api :athena}))
    :workgroup       workgroup
    :output-location output-location
    :catalog         catalog
    :database        database}))

(-comp/defcomponent {::-comp/ig-kw              ::athena-client
                     ::-comp/config-spec        ::athena-client-cfg
                     ::-comp/supports:disabled? true}
  [cfg] (#'make-athena-client cfg))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Value coercion
;;
;; Athena returns every result cell as a string; a column's real type is reported
;; separately in the result metadata. The definitions below turn a reported Athena type
;; into the corresponding Clojure value.

(def ^:private athena-timestamp-formatter
  "Formatter for Athena's zone-less timestamp rendering, `yyyy-MM-dd HH:mm:ss`, with
  optional fractional seconds up to nanosecond precision."
  (-> (DateTimeFormatterBuilder.)
      (.appendPattern "yyyy-MM-dd HH:mm:ss")
      (.appendFraction ChronoField/NANO_OF_SECOND 0 9 true)
      (.toFormatter)))

(defn- parse-athena-timestamp
  "Parse an Athena (UTC assumed) timestamp string and return an Instant."
  [s]
  (-> (LocalDateTime/parse s athena-timestamp-formatter)
      (.toInstant ZoneOffset/UTC)))

(def ^:private athena-type->coerce-fn
  "Map an Athena column type to a function that parses a cell string into the matching clojure value."
  {"boolean"   #(Boolean/parseBoolean %)
   "integer"   parse-long
   "bigint"    parse-long
   "double"    parse-double
   "date"      #(LocalDate/parse %)
   "timestamp" parse-athena-timestamp})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Queries

(defn start-query-execution!
  "Submit an SQL query for execution and return its query-execution id.

  `execution-parameters`, when given, is a sequence of strings bound positionally to the
  `?` placeholders in `sql`."
  ([component sql] (start-query-execution! component sql nil))
  ([{:keys [client workgroup output-location catalog database]} sql execution-parameters]
   (let [request (cond-> {:QueryString sql}
                   workgroup       (assoc :WorkGroup workgroup)
                   output-location (assoc :ResultConfiguration {:OutputLocation output-location})
                   (seq execution-parameters)
                   (assoc :ExecutionParameters (vec execution-parameters))
                   (or catalog database)
                   (assoc :QueryExecutionContext
                          (cond-> {}
                            catalog  (assoc :Catalog catalog)
                            database (assoc :Database database))))]
     (-> (-aws/throw-when-anomaly
          (aws/invoke client {:op :StartQueryExecution :request request}))
         :QueryExecutionId))))

(defn query-execution
  "Return the execution record for a query-execution id.

  Its [:Status :State] is one of QUEUED, RUNNING, SUCCEEDED, FAILED or CANCELLED, and it
  also reports timing, data-scanned statistics and, on failure, the error reason. Poll
  this to wait for a query to finish before reading its results."
  [{:keys [client]} query-execution-id]
  (-> (-aws/throw-when-anomaly
       (aws/invoke client {:op      :GetQueryExecution
                           :request {:QueryExecutionId query-execution-id}}))
      :QueryExecution))

(def query-terminal-states #{"SUCCEEDED" "FAILED" "CANCELLED"})
(def magic-wait-time 691) ;; it's a magic number
(defn await-query-execution
  "Block until the query reaches a terminal state; return the terminal state value of the `:QueryExecution` record.
  Return `timeout-value` instead if `max-wait` (a `Duration`) elapses first."
  [component query-execution-id ^java.time.Duration max-wait timeout-value]
  (let [result      (csp/chan)
        deadline-ms (+ (System/currentTimeMillis)
                                (.toMillis max-wait))
        poller-thread
        (csp/io-thread
         (loop []
           (let [execution (f/try-all [execution (query-execution component query-execution-id)]
                             (get-in execution [:Status :State])
                             (f/when-failed [e]
                               (log/with-context+ {:failure (f/message e)}
                                 (log/warn "Unable to query athena."))
                               :call-failure))]
             (if (contains? query-terminal-states execution)
               (csp/>!! result execution)
               (let [remaining (- deadline-ms (System/currentTimeMillis))]
                 (if (pos? remaining)
                   (do (csp/<!! (csp/timeout (min remaining
                                                  magic-wait-time)))
                       (recur))
                   (csp/>!! result timeout-value)))))))]
    (csp/<!! result)))

(defn query-results
  "Return the rows of a completed query as a lazy sequence of maps.

  The query must have reached the SUCCEEDED state."
  [{:keys [client]} query-execution-id]
  (let [pages        (iteration (fn [next-token]
                                  (-aws/throw-when-anomaly
                                   (aws/invoke client {:op      :GetQueryResults
                                                       :request (cond-> {:QueryExecutionId query-execution-id}
                                                                  next-token (assoc :NextToken next-token))})))
                                :vf    :ResultSet
                                :kf    :NextToken
                                :initk nil)
        page-seq     (seq pages)
        column-infos (some->> page-seq
                              first
                              :ResultSetMetadata
                              :ColumnInfo)
        columns      (mapv :Name column-infos)
        coercers     (mapv (fn [{t :Type}]
                             (get athena-type->coerce-fn t identity))
                           column-infos)
        row->raw     (fn [row] (mapv :VarCharValue (:Data row)))
        coerce-cell  (fn [coerce raw] (when (some? raw) (coerce raw)))
        all-rows     (mapcat :Rows page-seq)
        ;; A SELECT repeats its column names as the first result row; skip it.
        data-rows    (if (and (seq all-rows)
                              (= columns
                                 (row->raw (first all-rows))))
                       (rest all-rows)
                       all-rows)]
    (map (fn [row]
           (zipmap columns
                   (map coerce-cell coercers (row->raw row))))
         data-rows)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(comment

  ;; Fetch the running component from a dev system (adapt the accessor to your app).
  (defn athena
    []
    (::athena-client @user/dev-system))

  ;; Inspect the operations the underlying client supports.
  (aws/ops @(athena))
  (aws/doc @(athena) :StartQueryExecution)

  ;; Run a query, then poll it until it completes.
  (def qid (start-query-execution! (athena) "SELECT count(*) FROM some_table"))
  (get-in (query-execution (athena) qid) [:Status :State])

  (query-execution (athena) qid)
  (query-results (athena) qid)

  ;; Read the typed rows once the query has succeeded.
  (vec (query-results (athena) qid)))
