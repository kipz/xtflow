(ns xtflow.fuzzing.invariants
  "Invariant checking for fuzzing tests.

  Verifies mathematical properties and state consistency beyond simple
  result equality. These checks help catch subtle bugs in operator
  implementations."
  (:require [clojure.set :as set]))

;;; ============================================================================
;;; General Invariants
;;; ============================================================================

(defn check-count-invariant
  "Verify count invariant: result count should match unless LIMIT is used."
  [naive-results dataflow-results query-spec]
  (let [has-limit? (some #(= :limit (:op %)) (:operators query-spec))]
    (or has-limit?
        (= (count naive-results) (count dataflow-results)))))

(defn check-field-presence
  "Verify all results have required fields."
  [results required-fields]
  (every? (fn [result]
            (every? #(contains? result %) required-fields))
          results))

(defn check-no-nil-ids
  "Verify no results have nil :xt/id."
  [results]
  (every? #(some? (:xt/id %)) results))

;;; ============================================================================
;;; Aggregate Invariants
;;; ============================================================================

(defn check-aggregate-counts-non-negative
  "Verify all row counts are non-negative."
  [results]
  (every? (fn [result]
            (let [count-val (:count result)]
              (or (nil? count-val)
                  (and (number? count-val) (>= count-val 0)))))
          results))

(defn check-aggregate-sums-numeric
  "Verify all sums are numeric."
  [results sum-fields]
  (every? (fn [result]
            (every? (fn [field]
                      (let [val (get result field)]
                        (or (nil? val) (number? val))))
                    sum-fields))
          results))

(defn check-aggregate-avg-between-min-max
  "Verify averages are between min and max (when all present)."
  [results]
  (every? (fn [result]
            (let [avg (:avg result)
                  min-val (:min result)
                  max-val (:max result)]
              (or (nil? avg)
                  (nil? min-val)
                  (nil? max-val)
                  (and (<= min-val avg) (<= avg max-val)))))
          results))

(defn check-aggregate-group-keys-unique
  "Verify each group key appears at most once."
  [results]
  (= (count results)
     (count (set (map :group-key results)))))

;;; ============================================================================
;;; Join Invariants
;;; ============================================================================

(defn check-join-result-size
  "Verify join result size doesn't exceed cartesian product."
  [left-data right-data result-data]
  (<= (count result-data)
      (* (count left-data) (count right-data))))

(defn check-join-keys-match
  "Verify all join results have matching keys."
  [result-data left-key right-key]
  (every? (fn [result]
            (= (get result left-key) (get result right-key)))
          result-data))

(defn check-no-duplicate-fields
  "Verify no fields are duplicated (except join keys)."
  [_result-data left-fields right-fields join-key]
  (let [common-fields (set/intersection
                       (disj (set left-fields) join-key)
                       (disj (set right-fields) join-key))]
    (empty? common-fields)))

;;; ============================================================================
;;; UNNEST Invariants
;;; ============================================================================

(defn check-unnest-expansion
  "Verify UNNEST expands arrays correctly.

  For each input document with array of N elements,
  should produce N output documents."
  [input-data output-data _array-field]
  ;; This is a simplified check - real implementation would track document IDs
  (>= (count output-data) (count input-data)))

(defn check-unnest-preserves-fields
  "Verify UNNEST preserves original fields (plus new binding)."
  [_input-data output-data original-fields _binding-var]
  (every? (fn [result]
            (set/subset? original-fields (set (keys result))))
          output-data))

;;; ============================================================================
;;; LIMIT/OFFSET Invariants
;;; ============================================================================

(defn check-limit-respects-bound
  "Verify LIMIT produces at most N results."
  [results limit-n]
  (<= (count results) limit-n))

(defn check-offset-skips-correctly
  "Verify OFFSET skips at least N results (when input has enough)."
  [input-data output-data offset-n]
  (or (<= (count input-data) offset-n)  ; Not enough input
      (<= (count output-data) (- (count input-data) offset-n))))

;;; ============================================================================
;;; Monotonicity Invariants (for incremental updates)
;;; ============================================================================

(defn check-delta-monotonicity
  "Verify deltas have valid multiplicities.

  Each document should have net multiplicity of +1, 0, or -1."
  [deltas]
  (let [doc-mults (frequencies (map :doc deltas))]
    (every? #(<= -1 % 1) (vals doc-mults))))

(defn check-aggregate-state-consistency
  "Verify aggregate state matches emitted results.

  This checks that internal state (groups) is consistent with
  the aggregated results that were emitted."
  [aggregate-state emitted-results]
  (let [state-groups (set (keys (:groups aggregate-state)))
        result-groups (set (map :group-key emitted-results))]
    (= state-groups result-groups)))

;;; ============================================================================
;;; Composite Invariant Checks
;;; ============================================================================

(defn check-all-invariants
  "Run all applicable invariants for a query result.

  Returns {:valid? boolean, :violations [...]}"
  [naive-results dataflow-results query-spec]
  (let [violations (atom [])
        check (fn [name pred]
                (when-not pred
                  (swap! violations conj name)))]

    ;; General invariants
    (check :count-invariant
           (check-count-invariant naive-results dataflow-results query-spec))

    (check :no-nil-ids
           (check-no-nil-ids naive-results))

    ;; Aggregate-specific
    (when (some #(= :aggregate (:op %)) (:operators query-spec))
      (check :aggregate-counts-non-negative
             (check-aggregate-counts-non-negative naive-results))

      (check :aggregate-group-keys-unique
             (check-aggregate-group-keys-unique naive-results)))

    ;; LIMIT-specific
    (when-let [limit-op (first (filter #(= :limit (:op %)) (:operators query-spec)))]
      (check :limit-respects-bound
             (check-limit-respects-bound naive-results (:n limit-op))))

    {:valid? (empty? @violations)
     :violations @violations}))

(comment
  ;; Example usage

  ;; Check aggregate invariants
  (check-all-invariants
   [{:group-key "premium" :count 10}
    {:group-key "free" :count 5}]
   [{:group-key "premium" :count 10}
    {:group-key "free" :count 5}]
   {:operators [{:op :aggregate}]})

  ;; Check with violation
  (check-all-invariants
   [{:xt/id "1" :name "Alice"}
    {:xt/id "2" :name "Bob"}]
   [{:xt/id "1" :name "Alice"}]  ; Missing one result
   {:operators [{:op :from}]}))
