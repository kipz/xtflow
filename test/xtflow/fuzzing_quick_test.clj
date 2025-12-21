(ns xtflow.fuzzing-quick-test
  "Quick fuzzing tests for CI (target: <2 minutes).

  These tests run on every commit to catch regressions quickly.
  For more comprehensive testing, see fuzzing_test.clj."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.generators :as gen]
            [xtdb.api :as xt]
            [xtflow.core :as xtflow]
            [xtflow.fuzzing.generators :as fgen]
            [xtflow.fuzzing.naive :as naive]
            [xtflow.fuzzing.comparison :as cmp]
            [xtflow.test-fixtures :refer [xtdb-container-fixture
                                          clear-tables-fixture
                                          *xtdb-client*]]))

;;; Test fixtures - use existing XTDB container setup
(use-fixtures :once xtdb-container-fixture)
(use-fixtures :each clear-tables-fixture)

;;; ============================================================================
;;; Helper Functions
;;; ============================================================================

(defn setup-initial-data-with-xtflow!
  "Insert initial dataset through XTFlow to trigger delta propagation."
  [xtdb-client dataset]
  (doseq [[table docs] dataset
          :when (seq docs)]
    (xtflow/execute-tx! xtdb-client
                        [(into [:put-docs table] docs)])))

(defn test-query-equivalence
  "Test that naive and dataflow query results match.

  This implements true differential testing:
  1. Clear tables (ensure clean state for each iteration)
  2. Register query FIRST (before any data)
  3. Insert data through XTFlow (triggers delta propagation)
  4. Compare naive results with dataflow results
  5. Assert they match

  Returns true if results match, throws exception otherwise."
  [xtdb-client query-spec dataset]
  (let [query-id (str "fuzz-" (java.util.UUID/randomUUID))]
    (try
      ;; STEP 0: Clear all tables to ensure clean state for each test iteration
      (xtflow/reset-all-queries!)
      (doseq [table [:prod_attestations :users :orders :permissions]]
        (try
          (let [ids (map :xt/id (xt/q xtdb-client (list 'from table ['xt/id])))
                tx-result (when (seq ids)
                            (xt/execute-tx xtdb-client
                                           [(into [:delete-docs table] ids)]))]
            ;; Wait for delete to complete AND verify table is empty
            (when tx-result
              @tx-result
              ;; Query again to ensure delete is processed
              (while (seq (xt/q xtdb-client (list 'from table ['xt/id])))
                (Thread/sleep 10))))
          (catch Exception _ nil)))

      ;; Build XTQL string from query spec
      (let [xtql-string (fgen/build-xtql-string query-spec)

            ;; STEP 1: Register query BEFORE inserting any data
            _ (xtflow/register-query!
               {:query-id query-id
                :xtql xtql-string
                :callback (fn [_changes] nil)})

            ;; STEP 2: Insert data through XTFlow (triggers delta propagation)
            _ (setup-initial-data-with-xtflow! xtdb-client dataset)

            ;; STEP 3: Get both results
            ;; Naive: full table scan + in-memory processing (ground truth)
            naive-results (naive/naive-execute-query xtdb-client query-spec)

            ;; Dataflow: materialized incremental results from XTFlow
            dataflow-results (vec (xtflow/query-results query-id))

            ;; STEP 4: Compare results
            comparison (cmp/compare-results naive-results dataflow-results query-spec)]

        ;; Log detailed mismatch information for debugging
        (when-not (:match? comparison)
          (println "\n=== DIFFERENTIAL TESTING FAILURE ===")
          (println "Query ID:" query-id)
          (println "XTQL:" xtql-string)
          (println "Naive results:" (count naive-results))
          (println "Dataflow results:" (count dataflow-results))
          (println "Comparison:" comparison)
          (println "First 3 naive results:" (take 3 naive-results))
          (println "First 3 dataflow results:" (take 3 dataflow-results))
          (println "=====================================\n"))

        ;; Cleanup
        (xtflow/unregister-query! query-id)

        ;; ASSERT: Results must match
        ;; If this fails, we've found a bug in the dataflow implementation!
        (:match? comparison))

      (catch Exception e
        (println "\n=== ERROR ===")
        (println "Error executing query:" (.getMessage e))
        (println "Query ID:" query-id)
        (println "Query spec:" query-spec)
        (println "Stack trace:")
        (.printStackTrace e)
        (println "=============\n")

        ;; Cleanup on error
        (try (xtflow/unregister-query! query-id) (catch Exception _))
        false))))

;;; ============================================================================
;;; Property-Based Tests (Quick Suite)
;;; ============================================================================

(defspec ^:fuzzing-quick prop-simple-queries-quick
  50  ; num-tests
  (prop/for-all
   [dataset (fgen/gen-initial-dataset {:n-users 10 :n-orders 20})
    query-spec (fgen/gen-simple-query)]

   (test-query-equivalence *xtdb-client* query-spec dataset)))

(defspec ^:fuzzing-quick prop-medium-queries-quick
  30  ; num-tests
  (prop/for-all
   [dataset (fgen/gen-initial-dataset {:n-users 15 :n-orders 30})
    query-spec (fgen/gen-medium-query)]

   (test-query-equivalence *xtdb-client* query-spec dataset)))

(defspec ^:fuzzing-quick prop-aggregate-queries-quick
  20  ; num-tests
  (prop/for-all
   [dataset (fgen/gen-initial-dataset {:n-users 20})
    table (gen/elements [:users :orders])
    group-by-field (gen/elements [:tier :status :region])]

   (let [query-spec {:table table
                     :operators [{:op :from :table table :fields :*}
                                 {:op :aggregate
                                  :group-by group-by-field
                                  :aggregates {:count [:row-count]}}]}]
     (test-query-equivalence *xtdb-client* query-spec dataset))))

;;; ============================================================================
;;; Smoke Tests (Manual Validation)
;;; ============================================================================

(deftest ^:fuzzing-quick smoke-test-naive-executor
  (testing "Naive executor can execute simple queries"
    (let [dataset {:users [(fgen/make-user 1)
                           (fgen/make-user 2)
                           (fgen/make-user 3)]}
          query-spec {:table :users
                      :operators [{:op :from :table :users :fields :*}]}]

      ;; Insert data through XTFlow
      (setup-initial-data-with-xtflow! *xtdb-client* dataset)

      (let [results (naive/naive-execute-query *xtdb-client* query-spec)]
        (is (= 3 (count results)))
        (is (every? :xt/id results))))))

(deftest ^:fuzzing-quick smoke-test-where-operator
  (testing "WHERE operator filters correctly"
    (let [dataset {:users [{:xt/id "u1" :user-id 1 :name "Alice" :tier "premium"}
                           {:xt/id "u2" :user-id 2 :name "Bob" :tier "free"}
                           {:xt/id "u3" :user-id 3 :name "Charlie" :tier "premium"}]}
          query-spec {:table :users
                      :operators [{:op :from :table :users :fields :*}
                                  {:op :where :predicate [:= :tier "premium"]}]}]

      (setup-initial-data-with-xtflow! *xtdb-client* dataset)

      (let [results (naive/naive-execute-query *xtdb-client* query-spec)]
        (is (= 2 (count results)))
        (is (every? #(= "premium" (:tier %)) results))))))

(deftest ^:fuzzing-quick smoke-test-aggregate-operator
  (testing "AGGREGATE operator groups correctly"
    (let [dataset {:users [{:xt/id "u1" :user-id 1 :name "Alice" :tier "premium"}
                           {:xt/id "u2" :user-id 2 :name "Bob" :tier "free"}
                           {:xt/id "u3" :user-id 3 :name "Charlie" :tier "premium"}]}
          query-spec {:table :users
                      :operators [{:op :from :table :users :fields :*}
                                  {:op :aggregate
                                   :group-by :tier
                                   :aggregates {:count [:row-count]}}]}]

      (setup-initial-data-with-xtflow! *xtdb-client* dataset)

      (let [results (naive/naive-execute-query *xtdb-client* query-spec)]
        (is (= 2 (count results)))  ; Two groups: premium and free
        (is (= #{2 1} (set (map :count results))))))))

(deftest ^:fuzzing-quick smoke-test-limit-operator
  (testing "LIMIT operator restricts results"
    (let [dataset {:users [(fgen/make-user 1)
                           (fgen/make-user 2)
                           (fgen/make-user 3)
                           (fgen/make-user 4)
                           (fgen/make-user 5)]}
          query-spec {:table :users
                      :operators [{:op :from :table :users :fields :*}
                                  {:op :limit :n 3}]}]

      (setup-initial-data-with-xtflow! *xtdb-client* dataset)

      (let [results (naive/naive-execute-query *xtdb-client* query-spec)]
        (is (<= (count results) 3))))))

(comment
  ;; Run quick tests manually
  (clojure.test/run-tests 'xtflow.fuzzing-quick-test)

  ;; Check a specific test
  (smoke-test-naive-executor))
