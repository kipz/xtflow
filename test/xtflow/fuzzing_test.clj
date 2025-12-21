(ns xtflow.fuzzing-test
  "Extended fuzzing tests for nightly runs (target: ~30 minutes).

  These tests run comprehensive property-based testing to find edge cases
  and rare bugs. For quick CI feedback, see fuzzing_quick_test.clj."
  (:require [clojure.test :refer [use-fixtures]]
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

;;; Test fixtures
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
            (when tx-result @tx-result))
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
;;; Extended Property-Based Tests
;;; ============================================================================

(defspec ^:fuzzing-extended prop-simple-queries-extended
  200

  (prop/for-all
   [dataset (fgen/gen-initial-dataset {:n-users 50 :n-orders 100})
    query-spec (fgen/gen-simple-query)]

   (test-query-equivalence *xtdb-client* query-spec dataset)))

(defspec ^:fuzzing-extended prop-medium-queries-extended
  150

  (prop/for-all
   [dataset (fgen/gen-initial-dataset {:n-users 40 :n-orders 80})
    query-spec (fgen/gen-medium-query)]

   (test-query-equivalence *xtdb-client* query-spec dataset)))

(defspec ^:fuzzing-extended prop-complex-queries-extended
  100

  (prop/for-all
   [dataset (fgen/gen-initial-dataset {:n-sboms 20 :n-users 30})
    query-spec (fgen/gen-complex-query)]

   (test-query-equivalence *xtdb-client* query-spec dataset)))

;;; Operator-Specific Extended Tests

(defspec ^:fuzzing-extended prop-aggregate-row-count-extended
  100

  (prop/for-all
   [dataset (fgen/gen-initial-dataset {:n-users 30})
    table (gen/elements [:users :orders])
    group-by-field (gen/elements [:tier :status :region :org])]

   (let [query-spec {:table table
                     :operators [{:op :from :table table :fields :*}
                                 {:op :aggregate
                                  :group-by group-by-field
                                  :aggregates {:count [:row-count]}}]}]
     (test-query-equivalence *xtdb-client* query-spec dataset))))

(defspec ^:fuzzing-extended prop-aggregate-sum-extended
  80

  (prop/for-all
   [dataset (fgen/gen-initial-dataset {:n-users 25 :n-orders 50})]

   (let [query-spec {:table :orders
                     :operators [{:op :from :table :orders :fields :*}
                                 {:op :aggregate
                                  :group-by :status
                                  :aggregates {:total [:sum :amount]}}]}]
     (test-query-equivalence *xtdb-client* query-spec dataset))))

(defspec ^:fuzzing-extended prop-unnest-extended
  80

  (prop/for-all
   [dataset (fgen/gen-initial-dataset {:n-sboms 15})]

   (let [query-spec {:table :prod_attestations
                     :operators [{:op :from :table :prod_attestations :fields :*}
                                 {:op :where :predicate [:= :predicate_type "cyclonedx-sbom-v1.4"]}
                                 {:op :unnest :binding {:comp [:.. :predicate :components]}}]}]
     (test-query-equivalence *xtdb-client* query-spec dataset))))

(defspec ^:fuzzing-extended prop-limit-offset-extended
  100

  (prop/for-all
   [dataset (fgen/gen-initial-dataset {:n-users 50})
    limit-n (gen/choose 5 20)
    offset-n (gen/choose 0 10)]

   (let [query-spec {:table :users
                     :operators [{:op :from :table :users :fields :*}
                                 {:op :offset :n offset-n}
                                 {:op :limit :n limit-n}]}]
     (test-query-equivalence *xtdb-client* query-spec dataset))))

(defspec ^:fuzzing-extended prop-with-without-extended
  80

  (prop/for-all
   [dataset (fgen/gen-initial-dataset {:n-users 30})]

   (let [query-spec {:table :users
                     :operators [{:op :from :table :users :fields :*}
                                 {:op :with :fields {:name-copy :name}}
                                 {:op :without :fields [:user-id]}]}]
     (test-query-equivalence *xtdb-client* query-spec dataset))))

;;; Edge Case Tests

(defspec ^:fuzzing-extended prop-empty-results-extended
  50

  (prop/for-all
   [dataset (fgen/gen-initial-dataset {:n-users 20})]

   (let [query-spec {:table :users
                     :operators [{:op :from :table :users :fields :*}
                                 {:op :where :predicate [:= :tier "nonexistent"]}]}]
     (test-query-equivalence *xtdb-client* query-spec dataset))))

(defspec ^:fuzzing-extended prop-single-result-extended
  50

  (prop/for-all
   [dataset (fgen/gen-initial-dataset {:n-users 20})]

   (let [query-spec {:table :users
                     :operators [{:op :from :table :users :fields :*}
                                 {:op :limit :n 1}]}]
     (test-query-equivalence *xtdb-client* query-spec dataset))))

;;; Stress Tests

(defspec ^:fuzzing-extended prop-large-dataset-stress
  20

  (prop/for-all
   [dataset (fgen/gen-initial-dataset {:n-users 100 :n-orders 500})]

   (let [query-spec {:table :orders
                     :operators [{:op :from :table :orders :fields :*}
                                 {:op :aggregate
                                  :group-by :status
                                  :aggregates {:count [:row-count]
                                               :total [:sum :amount]}}]}]
     (test-query-equivalence *xtdb-client* query-spec dataset))))

(defspec ^:fuzzing-extended prop-complex-pipeline-stress
  50

  (prop/for-all
   [dataset (fgen/gen-initial-dataset {:n-users 40 :n-orders 80})]

   (let [query-spec {:table :orders
                     :operators [{:op :from :table :orders :fields :*}
                                 {:op :where :predicate [:= :status "completed"]}
                                 {:op :with :fields {:rounded-amount :amount}}
                                 {:op :aggregate
                                  :group-by :user-id
                                  :aggregates {:count [:row-count]
                                               :total [:sum :rounded-amount]}}
                                 {:op :limit :n 20}]}]
     (test-query-equivalence *xtdb-client* query-spec dataset))))

(comment
  ;; Run extended tests manually
  (clojure.test/run-tests 'xtflow.fuzzing-test))
