(ns afrolabs.utils
  (:require
   [clojure.string :as str]
   [taoensso.timbre :as log]
   )
  (:import
   [java.net URLEncoder]))

(defmacro condas->
  "A mixture of cond-> and as-> allowing more flexibility in the test and step forms"
  [expr name & clauses]
  (assert (even? (count clauses)))
  (let [pstep (fn [[test step]] `(if ~test ~step ~name))]
    `(let [~name ~expr
           ~@(interleave (repeat name) (map pstep (partition 2 clauses)))]
       ~name)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmacro opt-assoc
  "Helper macro for a repeating pattern in the codebase.

  Useful to assoc optional attributes into a map, if the value referenced by the symbol is non-nil.

  Each symbol must have the same name as the attribute being assoc-ed into the map.

  Accepts a map, that will be the starting value of `(cond-> <map> ...)`.
  Accepts a list of symbols. For each symbol `x`, will expand to the following:
  `x (assoc \"x\" x)`

  `opts?` is an options map:
  :attr-name -- accepts one of arg [:keyword | :string], default :string.
                When `:keyword` expansion becomes `x (assoc :x x)`
  "
  {:style/indent 1}
  [opts? into-map & xs]
  (let [;; the test if whether into-map is a map (or a symbol).
        ;; This determines if `opts?` was the source map or optional options-map
        [opts into-map' xs']
        (if (map? into-map)
          [opts? into-map xs]
          [nil opts? (concat [into-map] xs)])

        {:keys [attr-name]
         :or   {attr-name :string}}
        (or opts {})

        x->attr-name
        (case attr-name
          :string  name
          :keyword #(keyword (name %)))

        expressions
        (into []
              (mapcat identity)
              (for [x xs']
                `[~x (assoc ~(x->attr-name x) ~x)]
                ))]
    `(cond-> ~into-map'
       ~@expressions)))

(comment

  (macroexpand-1 '(opt-assoc {:attr-name :keyword}
                    {:source "value"}
                    a
                    b))
  ;; (clojure.core/cond-> {:source "value"}
  ;;   a (clojure.core/assoc :a a)
  ;;   b (clojure.core/assoc :b b))

  (macroexpand-1 '(opt-assoc {:attr-name :string} {:source "value"}
                             a b))
  ;; (clojure.core/cond-> {:source "value"}
  ;;   a (clojure.core/assoc "a" a)
  ;;   b (clojure.core/assoc "b" b))


  )

(defn param-url-encode
  ([x]
   (some-> x
           (URLEncoder/encode)
           (str/replace #"\+" "%20")))
  )

(defmacro time
  "Evaluates expr and LOGS the time it took.  Returns the value of
  expr.

  Default log-level is :trace.
  Overloads allow to specify the log-level and an additional string to prepend to the normal \"Elasped time: \"...

  This is a modified clone of the stdlb time macro."
  ([expr] `(time :trace nil ~expr))
  ([log-msg-or-level expr]
   `(time (or (when (#'log/valid-level? ~log-msg-or-level)
                ~log-msg-or-level)
              :trace)
          (or (when-not (#'log/valid-level? ~log-msg-or-level)
                ~log-msg-or-level)
              nil)
          ~expr))
  ([log-level log-msg expr]
   `(let [start# (. System (nanoTime))
          ret# ~expr]
      (log/log ~log-level
        (str (when ~log-msg (str ~log-msg " || "))
             "Elapsed time: " (/ (double (- (. System (nanoTime)) start#)) 1000000.0) " msecs"))
      ret#)))

(comment

  (macroexpand '(time (println 1)))
  (time :debug  "info" (println 1))
  (time :debug  (println 1))
  (time "debug" (println 1))
  (time (println 1))

  )
