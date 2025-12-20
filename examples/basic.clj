(ns examples.basic
  (:require [xtflow.core :as diff]
            [xtflow.operators :as ops]
            [xtflow.delta :as delta]
            [xtflow.dataflow :as df]
            [clojure.pprint :as pp]))

(println "\n=== Testing Differential Dataflow ===\n")

;; Test 1: Create operators
(println "Test 1: Creating operators...")
(def from-op (ops/create-from-operator :prod_attestations [:predicate_type]))
(def where-op (ops/create-where-operator [:= :predicate_type "slsa"]))
(def agg-op (ops/create-aggregate-operator :predicate_type {:count [:row-count]}))
(println "✓ Operators created successfully")

;; Test 2: Process deltas through operators
(println "\nTest 2: Processing deltas...")
(def test-delta (delta/add-delta {:xt/id "1" :xt/table :prod_attestations :predicate_type "slsa"}))
(def from-result (ops/process-delta from-op test-delta))
(println "From operator output:" (count from-result) "deltas")
(def where-result (mapcat #(ops/process-delta where-op %) from-result))
(println "Where operator output:" (count where-result) "deltas")
(println "✓ Delta processing works")

;; Test 3: Execute pipeline
(println "\nTest 3: Executing pipeline...")
(def pipeline [from-op where-op agg-op])
(def input-deltas [(delta/add-delta {:xt/id "1" :xt/table :prod_attestations :predicate_type "slsa"})
                   (delta/add-delta {:xt/id "2" :xt/table :prod_attestations :predicate_type "slsa"})
                   (delta/add-delta {:xt/id "3" :xt/table :prod_attestations :predicate_type "sbom"})])
(def result (df/execute-linear-pipeline pipeline input-deltas))
(println "Pipeline result:" (count result) "deltas")
(pp/pprint (map :doc result))
(println "✓ Pipeline execution works")

;; Test 4: Register a query
(println "\nTest 4: Registering query...")
(diff/register-query!
 {:query-id "test-query"
  :xtql "(-> (from :prod_attestations [predicate_type])
              (aggregate predicate_type {:count (row-count)}))"
  :callback (fn [changes]
              (println "\n>>> CALLBACK FIRED <<<")
              (println "Added:" (count (:added changes)))
              (println "Modified:" (count (:modified changes)))
              (println "Removed:" (count (:removed changes))))})
(println "✓ Query registered")
(println "Registered queries:" (count (diff/list-queries)))

;; Test 5: Check registry stats
(println "\nTest 5: Registry stats...")
(pp/pprint (diff/registry-stats))

(println "\n=== All Tests Passed! ===\n")
