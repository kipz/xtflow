(ns xtflow.fuzzing.utils
  "Shared utilities for fuzzing tests.")

;;; ============================================================================
;;; Failure Reporting
;;; ============================================================================

(defn format-failure
  "Format a test failure for reporting."
  [test-case naive-result dataflow-result diff]
  {:timestamp (System/currentTimeMillis)
   :test-case test-case
   :naive-result naive-result
   :dataflow-result dataflow-result
   :diff diff})

(defn save-failure
  "Save test failure to file for later analysis."
  [failure]
  (let [failures-dir (clojure.java.io/file "target/fuzzing-failures")
        timestamp (:timestamp failure)
        filename (str "failure-" timestamp ".edn")]
    (.mkdirs failures-dir)
    (spit (clojure.java.io/file failures-dir filename)
          (pr-str failure))
    (str "Saved failure to target/fuzzing-failures/" filename)))

(defn reproduction-command
  "Generate command to reproduce a failure."
  [seed]
  (format "clojure -X:fuzzing-quick :seed %d" seed))

;;; ============================================================================
;;; Query Formatting
;;; ============================================================================

(defn format-query-spec
  "Format query spec for human-readable output."
  [query-spec]
  (str "Table: " (:table query-spec) "\n"
       "Operators:\n"
       (clojure.string/join "\n"
                            (map-indexed
                             (fn [idx op]
                               (format "  %d. %s %s"
                                       (inc idx)
                                       (name (:op op))
                                       (dissoc op :op)))
                             (:operators query-spec)))))

;;; ============================================================================
;;; Data Inspection
;;; ============================================================================

(defn sample-results
  "Take a sample of results for inspection."
  [results n]
  (take n results))

(defn result-stats
  "Compute statistics about results."
  [results]
  {:count (count results)
   :unique-ids (count (set (map :xt/id results)))
   :fields (set (mapcat keys results))
   :sample (sample-results results 3)})

;;; ============================================================================
;;; Test Helpers
;;; ============================================================================

(defn with-timeout
  "Execute function with timeout (in milliseconds)."
  [timeout-ms f]
  (let [future (future (f))
        result (deref future timeout-ms ::timeout)]
    (if (= result ::timeout)
      (do
        (future-cancel future)
        (throw (ex-info "Operation timed out"
                        {:timeout-ms timeout-ms})))
      result)))

(defn retry
  "Retry function up to n times on failure."
  [n f]
  (loop [attempts n]
    (if (zero? attempts)
      (throw (ex-info "All retry attempts failed" {:attempts n}))
      (try
        (f)
        (catch Exception e
          (if (= attempts 1)
            (throw e)
            (recur (dec attempts))))))))

;;; ============================================================================
;;; Debugging
;;; ============================================================================

(defn enable-debug-logging!
  "Enable debug logging for fuzzing tests."
  []
  (alter-var-root #'*debug-enabled* (constantly true)))

(defn disable-debug-logging!
  "Disable debug logging for fuzzing tests."
  []
  (alter-var-root #'*debug-enabled* (constantly false)))

(def ^:dynamic *debug-enabled* false)

(defmacro debug
  "Print debug message if debugging is enabled."
  [& args]
  `(when *debug-enabled*
     (println "[DEBUG]" ~@args)))

;;; ============================================================================
;;; Performance Tracking
;;; ============================================================================

(defn time-execution
  "Time execution of function and return [result duration-ms]."
  [f]
  (let [start (System/nanoTime)
        result (f)
        end (System/nanoTime)
        duration-ms (/ (- end start) 1000000.0)]
    [result duration-ms]))

(defn benchmark
  "Benchmark function execution over n iterations."
  [n f]
  (let [times (doall
               (for [_ (range n)]
                 (second (time-execution f))))
        avg (/ (reduce + times) n)
        min-time (apply min times)
        max-time (apply max times)]
    {:iterations n
     :avg-ms avg
     :min-ms min-time
     :max-ms max-time
     :total-ms (reduce + times)}))

(comment
  ;; Example usage

  ;; Save a failure
  (save-failure
   (format-failure
    {:query-spec {:table :users :operators [...]}}
    [{:xt/id "1"}]
    []
    {:missing 1}))

  ;; Format query
  (format-query-spec
   {:table :users
    :operators [{:op :from :table :users :fields [:*]}
                {:op :where :predicate [:= :tier "premium"]}]})

  ;; Benchmark
  (benchmark 10 #(Thread/sleep 100)))
