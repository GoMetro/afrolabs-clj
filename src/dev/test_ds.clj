(ns dev.test-ds
  "Benchmark namespace to verify that the :parallel? flag on sort-by-column
  actually changes behaviour, and to measure the cost of mapseq-parser
  materialisation vs sort in the parquet sink's export! path."
  (:require
   [clojure.spec.alpha     :as s]
   [clojure.spec.gen.alpha :as gen]
   [tech.v3.dataset        :as ds]
   [tech.v3.libs.parquet   :as _ds-parquet])  ; side-effect: registers :parquet read/write
  (:import
   [java.time Instant]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Spec

(def ^:private epoch-2020 (.toEpochMilli (Instant/parse "2020-01-01T00:00:00Z")))
(def ^:private epoch-2026 (.toEpochMilli (Instant/parse "2026-01-01T00:00:00Z")))

(s/def ::id
  pos-int?)

(s/def ::value-a
  (s/double-in :NaN? false :infinite? false :min -1.0e9 :max 1.0e9))

(s/def ::value-b
  (s/double-in :NaN? false :infinite? false :min -1.0e9 :max 1.0e9))

(s/def ::count-x
  (s/with-gen int?
              #(gen/large-integer* {:min -100000 :max 100000})))

(s/def ::label
  (s/with-gen (s/and string? #(<= (count %) 20))
              #(gen/fmap (fn [s] (subs s 0 (min 20 (count s))))
                         (gen/string-alphanumeric))))

(s/def ::category
  (s/with-gen (s/and string? #(<= (count %) 10))
              #(gen/fmap (fn [s] (subs s 0 (min 10 (count s))))
                         (gen/string-alphanumeric))))

(s/def ::timestamp
  (s/with-gen #(instance? Instant %)
              #(gen/fmap (fn [^long ms] (Instant/ofEpochMilli ms))
                         (gen/large-integer* {:min epoch-2020 :max epoch-2026}))))

(s/def ::record
  (s/keys :req-un [::id
                   ::value-a
                   ::value-b
                   ::count-x
                   ::label
                   ::category
                   ::timestamp]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Data generation

(defn generate-seed
  "Generate a seed batch of n spec-valid records. Expensive — call once."
  [n]
  (vec (gen/sample (s/gen ::record) n)))

(defn records-from-seed
  "Return n records by cycling through seed. Fast — use for large n."
  [seed n]
  (vec (take n (cycle seed))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Instrumentation helpers

(defmacro timed
  "Evaluates body, returns {:result _ :elapsed-ms _}."
  [& body]
  `(let [start#  (System/nanoTime)
         result# (do ~@body)]
     {:result     result#
      :elapsed-ms (/ (double (- (System/nanoTime) start#)) 1.0e6)}))

(defn bench
  "Run thunk, print label and elapsed time, return result."
  [label thunk]
  (let [{:keys [result elapsed-ms]} (timed (thunk))]
    (println (format "%-50s  %8.1f ms" label elapsed-ms))
    result))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; The two operations under test, mirroring export! in parquet.clj

(defn bench-materialise
  "Feed records into a fresh mapseq-parser, then time the (parser) call.
  Returns the materialised dataset."
  [records]
  (let [parser (ds/mapseq-parser)]
    (run! parser records)
    (bench (format "(dataset) — %d records" (count records)) parser)))

(defn bench-sort
  "Time sort-by-column on ds with both :parallel? false and :parallel? true.
  Runs each twice so the JIT gets a chance to settle."
  [ds colname]
  (let [n (ds/row-count ds)]
    (doseq [run [1 2]]
      (doseq [[label opts] [["serial  :parallel? false" {:parallel? false}]
                            ["parallel :parallel? true " {:parallel? true}]]]
        (bench (format "sort-by-column %-28s run %d  n=%d" label run n)
               #(ds/sort-by-column ds colname nil opts)))))
  nil)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(comment

  ;; ── 1. Generate data ─────────────────────────────────────────────────────
  ;; Generate seed once (slow), then tile it cheaply to get a large dataset.
  (def seed (generate-seed 2000))
  (count seed)

  (def records-100k  (records-from-seed seed 100000))
  (def records-500k  (records-from-seed seed 500000))
  (def records-5M  (records-from-seed seed 5000000))
  (def records-50M  (records-from-seed seed 50000000))

  ;; ── 2. Benchmark materialisation ─────────────────────────────────────────
  ;; This mirrors the (dataset) call in export!
  (def ds-100k  (bench-materialise records-100k))
  (def ds-500k  (bench-materialise records-500k))
  (def ds-5M    (bench-materialise records-5M))
  (def ds-50M   (bench-materialise records-50M))

  ;; Sanity-check the dataset
  (ds/row-count ds-100k)
  (ds/column-names ds-100k)
  (ds/head ds-100k 3)

  ;; ── 3. Benchmark sort — the fix under test ────────────────────────────────
  ;; Expect serial ≈ parallel on single-core machines;
  ;; on multi-core, parallel should be faster for large n,
  ;; at the cost of maxing out all CPUs during the sort.
  (bench-sort ds-100k  :timestamp)
  (bench-sort ds-500k  :timestamp)
  (bench-sort ds-5M :timestamp)
  (bench-sort ds-50M :timestamp)


  )
