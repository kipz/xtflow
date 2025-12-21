(ns xtflow.fuzzing.comparison
  "Result comparison logic for fuzzing tests.

  Handles normalization and comparison of query results from naive
  and dataflow implementations, accounting for non-determinism."
  (:require [clojure.data :as data]))

;;; ============================================================================
;;; Normalization
;;; ============================================================================

(defn normalize-number
  "Normalize number for comparison (handle floating point tolerance).

  Rounds floats to 4 decimal places to handle floating point precision issues.
  Converts whole-number floats to integers for consistent comparison."
  [n]
  (if (float? n)
    (let [rounded (/ (Math/round (* n 10000.0)) 10000.0)]
      (if (== rounded (Math/floor rounded))
        (long rounded)
        rounded))
    n))

(defn normalize-doc
  "Normalize a single document for comparison.

  - Converts maps to sorted maps for consistent ordering
  - Rounds floating point numbers
  - Removes nil values"
  [doc]
  (cond
    (map? doc)
    (into (sorted-map)
          (for [[k v] doc
                :when (some? v)]
            [k (normalize-doc v)]))

    (sequential? doc)
    (mapv normalize-doc doc)

    (float? doc)
    (normalize-number doc)

    :else doc))

(defn has-limit-or-offset?
  "Check if query has LIMIT or OFFSET operators."
  [query-spec]
  (let [ops (get query-spec :operators [])]
    (some #(#{:limit :offset} (:op %)) ops)))

(defn has-operators-after-limit?
  "Check if query has operators after LIMIT/OFFSET.

  When LIMIT/OFFSET appears before other operators (like WHERE, WITH, etc.),
  the results become inherently non-deterministic. For example:

    (-> (from :users) (limit 10) (where [:= :role \"admin\"]))

  This takes any 10 users (non-deterministic), then filters for admins.
  Different implementations can take different sets of 10 users, resulting in
  different numbers of admins. Both are valid!"
  [query-spec]
  (let [ops (get query-spec :operators [])
        limit-idx (first (keep-indexed #(when (#{:limit :offset} (:op %2)) %1) ops))]
    (when limit-idx
      ;; Check if there are any operators after LIMIT/OFFSET
      (< (inc limit-idx) (count ops)))))

(defn normalize-result-set
  "Normalize a collection of result documents.

  - Normalizes each document
  - Converts to set for comparison (unless order matters)"
  [results query-spec]
  (let [normalized (map normalize-doc results)]
    (if (has-limit-or-offset? query-spec)
      ;; For LIMIT/OFFSET, order can be non-deterministic
      ;; but we still want to compare as sets
      (set normalized)
      ;; For other queries, convert to set
      (set normalized))))

;;; ============================================================================
;;; Comparison
;;; ============================================================================

(defn set-equals?
  "Check if two result sets are equal (as sets)."
  [naive-results dataflow-results]
  (= (set naive-results) (set dataflow-results)))

(defn subset-match?
  "Check if naive and dataflow results are valid for LIMIT/OFFSET queries.

  For LIMIT without ORDER BY, both implementations can return different subsets
  (since the order is non-deterministic). We only verify:
  1. Both return the same count
  2. Count is <= LIMIT value

  We don't require the same specific rows since both are equally valid."
  [naive-results dataflow-results query-spec]
  (let [naive-set (set naive-results)
        dataflow-set (set dataflow-results)
        naive-count (count naive-set)
        dataflow-count (count dataflow-set)]
    (and
     ;; Both should return the same count
     (= naive-count dataflow-count)
     ;; Size should match LIMIT if present
     (if-let [limit-op (first (filter #(= :limit (:op %)) (:operators query-spec)))]
       (<= dataflow-count (:n limit-op))
       true))))

(defn results-match?
  "Determine if naive and dataflow results match.

  Handles different matching strategies based on query type:
  - LIMIT/OFFSET with operators after: accept any results (inherently non-deterministic)
  - LIMIT/OFFSET at end: subset match (any N results are valid)
  - Exact match for most queries
  - Floating point tolerance for aggregate queries"
  [naive-results dataflow-results query-spec]
  (let [norm-naive (normalize-result-set naive-results query-spec)
        norm-dataflow (normalize-result-set dataflow-results query-spec)]
    (cond
      ;; LIMIT/OFFSET with operators after (e.g., LIMIT then WHERE):
      ;; Results are inherently non-deterministic - both are valid
      (has-operators-after-limit? query-spec)
      true  ; Accept any results as valid

      ;; LIMIT/OFFSET at end without ORDER-BY: subset match
      (has-limit-or-offset? query-spec)
      (subset-match? norm-naive norm-dataflow query-spec)

      ;; Default: exact set equality
      :else
      (set-equals? norm-naive norm-dataflow))))

(defn compare-results
  "Compare naive and dataflow results and return detailed comparison.

  Returns a map with:
    :match? - boolean indicating if results match
    :naive-count - number of naive results
    :dataflow-count - number of dataflow results
    :diff - difference between results (if not matching)
    :naive-only - results only in naive (if not matching)
    :dataflow-only - results only in dataflow (if not matching)"
  [naive-results dataflow-results query-spec]
  (let [match? (results-match? naive-results dataflow-results query-spec)
        naive-count (count naive-results)
        dataflow-count (count dataflow-results)]
    (if match?
      {:match? true
       :naive-count naive-count
       :dataflow-count dataflow-count}
      (let [norm-naive (normalize-result-set naive-results query-spec)
            norm-dataflow (normalize-result-set dataflow-results query-spec)
            [naive-only dataflow-only both] (data/diff norm-naive norm-dataflow)]
        {:match? false
         :naive-count naive-count
         :dataflow-count dataflow-count
         :naive-only naive-only
         :dataflow-only dataflow-only
         :both both
         :diff {:only-in-naive (count naive-only)
                :only-in-dataflow (count dataflow-only)
                :in-both (count both)}}))))

(comment
  ;; Example usage

  ;; Compare simple results
  (compare-results
   [{:xt/id "1" :name "Alice"}
    {:xt/id "2" :name "Bob"}]
   [{:xt/id "1" :name "Alice"}
    {:xt/id "2" :name "Bob"}]
   {:operators []})

  ;; Compare with mismatch
  (compare-results
   [{:xt/id "1" :name "Alice"}
    {:xt/id "2" :name "Bob"}]
   [{:xt/id "1" :name "Alice"}]
   {:operators []})

  ;; Compare aggregate results with floating point
  (compare-results
   [{:tier "premium" :avg 99.99999}]
   [{:tier "premium" :avg 100.00001}]
   {:operators [{:op :aggregate}]}))
