(ns xtflow.fuzzing.coverage
  "Coverage tracking for fuzzing tests.

  Tracks which operators and operator combinations have been tested
  to ensure comprehensive coverage.")

;;; ============================================================================
;;; Coverage State
;;; ============================================================================

(defonce operator-coverage
  "Tracks how many times each operator has been tested."
  (atom {}))

(defonce operator-pair-coverage
  "Tracks how many times each operator pair has been tested."
  (atom {}))

(defonce operator-triple-coverage
  "Tracks how many times each operator triple has been tested."
  (atom {}))

;;; ============================================================================
;;; Coverage Tracking
;;; ============================================================================

(defn extract-operators
  "Extract operator types from query spec."
  [query-spec]
  (mapv :op (:operators query-spec)))

(defn track-operator-coverage
  "Track coverage for individual operators."
  [query-spec]
  (let [ops (extract-operators query-spec)]
    (doseq [op ops]
      (swap! operator-coverage update op (fnil inc 0)))))

(defn track-operator-pair-coverage
  "Track coverage for operator pairs (sequential)."
  [query-spec]
  (let [ops (extract-operators query-spec)
        pairs (partition 2 1 ops)]
    (doseq [pair pairs]
      (swap! operator-pair-coverage update pair (fnil inc 0)))))

(defn track-operator-triple-coverage
  "Track coverage for operator triples (sequential)."
  [query-spec]
  (let [ops (extract-operators query-spec)
        triples (partition 3 1 ops)]
    (doseq [triple triples]
      (swap! operator-triple-coverage update triple (fnil inc 0)))))

(defn track-coverage
  "Track all coverage metrics for a query."
  [query-spec]
  (track-operator-coverage query-spec)
  (track-operator-pair-coverage query-spec)
  (track-operator-triple-coverage query-spec))

;;; ============================================================================
;;; Coverage Reporting
;;; ============================================================================

(def all-operators
  "All operators that should be covered."
  [:from :where :with :without :unnest :aggregate
   :join :left-join :unify :exists :pull :pull-star
   :limit :offset :return :rel])

(defn operator-coverage-report
  "Generate coverage report for individual operators."
  []
  (let [coverage @operator-coverage
        total-tests (reduce + (vals coverage))]
    (println "\n=== Operator Coverage ===")
    (println (format "Total tests: %d" total-tests))
    (println)
    (doseq [op all-operators]
      (let [count (get coverage op 0)
            pct (if (pos? total-tests)
                  (* 100.0 (/ count total-tests))
                  0.0)]
        (println (format "%-15s: %5d tests (%5.1f%%)"
                         (name op)
                         count
                         pct))))
    (println)))

(defn operator-pair-coverage-report
  "Generate coverage report for operator pairs."
  []
  (let [coverage @operator-pair-coverage
        sorted-pairs (sort-by second > coverage)
        top-10 (take 10 sorted-pairs)]
    (println "\n=== Top 10 Operator Pairs ===")
    (doseq [[pair count] top-10]
      (println (format "%-30s: %5d tests"
                       (str (mapv name pair))
                       count)))
    (println)))

(defn uncovered-operators
  "Find operators that have never been tested."
  []
  (let [coverage @operator-coverage
        covered (set (keys coverage))]
    (remove covered all-operators)))

(defn uncovered-operator-pairs
  "Find operator pairs that have never been tested."
  []
  (let [coverage @operator-pair-coverage
        covered-pairs (set (keys coverage))
        all-possible-pairs (for [op1 all-operators
                                 op2 all-operators]
                             [op1 op2])]
    (remove covered-pairs all-possible-pairs)))

(defn coverage-summary
  "Generate comprehensive coverage summary."
  []
  (let [op-cov @operator-coverage
        pair-cov @operator-pair-coverage
        total-ops (count all-operators)
        covered-ops (count (keys op-cov))
        coverage-pct (* 100.0 (/ covered-ops total-ops))
        uncovered (uncovered-operators)]

    (println "\n=== Coverage Summary ===")
    (println (format "Operators covered: %d/%d (%.1f%%)"
                     covered-ops
                     total-ops
                     coverage-pct))
    (println (format "Total operator tests: %d"
                     (reduce + (vals op-cov))))
    (println (format "Unique operator pairs: %d"
                     (count (keys pair-cov))))
    (println (format "Total pair tests: %d"
                     (reduce + (vals pair-cov))))

    (when (seq uncovered)
      (println)
      (println "Uncovered operators:")
      (doseq [op uncovered]
        (println (format "  - %s" (name op)))))

    (println)))

(defn full-coverage-report
  "Generate full coverage report with all metrics."
  []
  (operator-coverage-report)
  (operator-pair-coverage-report)
  (coverage-summary))

;;; ============================================================================
;;; Coverage Reset
;;; ============================================================================

(defn reset-coverage!
  "Reset all coverage tracking."
  []
  (reset! operator-coverage {})
  (reset! operator-pair-coverage {})
  (reset! operator-triple-coverage {}))

;;; ============================================================================
;;; Coverage Utilities
;;; ============================================================================

(defn save-coverage-report
  "Save coverage report to file."
  [filename]
  (spit filename
        (with-out-str (full-coverage-report))))

(defn coverage-percentage
  "Calculate overall coverage percentage."
  []
  (let [covered (count (keys @operator-coverage))
        total (count all-operators)]
    (* 100.0 (/ covered total))))

(defn is-well-covered?
  "Check if coverage meets threshold (default 80%)."
  ([] (is-well-covered? 80.0))
  ([threshold]
   (>= (coverage-percentage) threshold)))

(comment
  ;; Example usage

  ;; Track some queries
  (track-coverage {:operators [{:op :from} {:op :where} {:op :aggregate}]})
  (track-coverage {:operators [{:op :from} {:op :unnest} {:op :limit}]})

  ;; Generate report
  (full-coverage-report)

  ;; Check coverage
  (coverage-percentage)
  (is-well-covered? 75.0)

  ;; Save report
  (save-coverage-report "target/fuzzing-coverage.txt")

  ;; Reset for new run
  (reset-coverage!))
