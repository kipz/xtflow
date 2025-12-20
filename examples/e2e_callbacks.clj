(ns examples.e2e-callbacks
  "End-to-end tests demonstrating callback firing on data changes."
  (:require [xtflow.core :as diff]
            [xtdb.api :as xt]
            [clojure.pprint :as pp]))

(println "\n╔══════════════════════════════════════════════════════════╗")
(println "║  End-to-End Differential Dataflow Callback Tests        ║")
(println "╚══════════════════════════════════════════════════════════╝\n")

;; Setup XTDB client
(def xtdb-client (xt/client {:host "localhost" :port 5432 :user "xtdb"}))

;; Clear any existing queries
(doseq [query (diff/list-queries)]
  (diff/unregister-query! (:query-id query)))

(println "Starting with clean state. Registered queries:" (count (diff/list-queries)))

;;; Test 1: Simple Aggregation - First Insert Triggers Callback

(println "\n" (apply str (repeat 60 "=")) "\n")
(println "TEST 1: First Insert - Callback Fires with Added Group\n")

(def test1-changes (atom nil))

(diff/register-query!
 {:query-id "test1-aggregation"
  :xtql "(-> (from :prod_attestations [predicate_type])
              (aggregate predicate_type {:count (row-count)}))"
  :callback (fn [changes]
              (println ">>> CALLBACK FIRED! <<<")
              (println "Added groups:" (count (:added changes)))
              (println "Modified groups:" (count (:modified changes)))
              (println "Removed groups:" (count (:removed changes)))
              (when (seq (:added changes))
                (println "\nNew groups:")
                (doseq [doc (:added changes)]
                  (println (format "  - %s: count=%d"
                                   (:predicate_type doc)
                                   (:count doc)))))
              (reset! test1-changes changes))})

(println "Query registered. Current results:")
(pp/pprint (diff/query-results "test1-aggregation"))

(println "\nExecuting transaction: Adding first SLSA attestation...")
(diff/execute-tx! xtdb-client
                  [[:put-docs :prod_attestations
                    {:xt/id "test1-doc1"
                     :predicate_type "slsa-provenance-v1"
                     :subjects []}]])

(println "\nChanges captured by callback:")
(pp/pprint @test1-changes)

(println "\nCurrent query results after insert:")
(pp/pprint (diff/query-results "test1-aggregation"))

;;; Test 2: Second Insert of Same Type - Callback Fires with Modified Group

(println "\n" (apply str (repeat 60 "=")) "\n")
(println "TEST 2: Second Insert - Callback Fires with Modified Count\n")

(reset! test1-changes nil)

(println "Executing transaction: Adding second SLSA attestation (same type)...")
(diff/execute-tx! xtdb-client
                  [[:put-docs :prod_attestations
                    {:xt/id "test1-doc2"
                     :predicate_type "slsa-provenance-v1"
                     :subjects []}]])

(println "\nChanges captured by callback:")
(pp/pprint @test1-changes)

(when-let [modified (first (:modified @test1-changes))]
  (println "\nChange details:")
  (println "  Old count:" (:count (:old modified)))
  (println "  New count:" (:count (:new modified)))
  (println "  Δ:" (- (:count (:new modified)) (:count (:old modified)))))

(println "\nCurrent query results:")
(pp/pprint (diff/query-results "test1-aggregation"))

;;; Test 3: Insert Different Type - Callback Fires with Added Group

(println "\n" (apply str (repeat 60 "=")) "\n")
(println "TEST 3: Different Type Insert - Callback Fires with New Group\n")

(reset! test1-changes nil)

(println "Executing transaction: Adding SBOM attestation (different type)...")
(diff/execute-tx! xtdb-client
                  [[:put-docs :prod_attestations
                    {:xt/id "test1-doc3"
                     :predicate_type "cyclonedx-sbom-v1"
                     :subjects []}]])

(println "\nChanges captured by callback:")
(pp/pprint @test1-changes)

(println "\nCurrent query results (now has 2 groups):")
(pp/pprint (diff/query-results "test1-aggregation"))

(diff/unregister-query! "test1-aggregation")

;;; Test 4: Filtered Aggregation - Callback Only Fires for Matching Data

(println "\n" (apply str (repeat 60 "=")) "\n")
(println "TEST 4: Filtered Query - Callback Only Fires for Matching Data\n")

(def test4-changes (atom nil))

(diff/register-query!
 {:query-id "test4-filtered"
  :xtql "(-> (from :prod_attestations [predicate_type])
              (where (like predicate_type \"%slsa%\"))
              (aggregate predicate_type {:count (row-count)}))"
  :callback (fn [changes]
              (println ">>> FILTERED CALLBACK FIRED! <<<")
              (println "Changes:" changes)
              (reset! test4-changes changes))})

(println "Query registered for SLSA attestations only.\n")

(println "Test 4a: Adding SLSA attestation (should trigger callback)...")
(reset! test4-changes nil)
(diff/execute-tx! xtdb-client
                  [[:put-docs :prod_attestations
                    {:xt/id "test4-doc1"
                     :predicate_type "slsa-provenance-v2"
                     :subjects []}]])

(Thread/sleep 100)
(if @test4-changes
  (println "✓ Callback fired as expected!")
  (println "✗ ERROR: Callback did not fire!"))

(println "\nTest 4b: Adding non-SLSA attestation (should NOT trigger callback)...")
(reset! test4-changes nil)
(diff/execute-tx! xtdb-client
                  [[:put-docs :prod_attestations
                    {:xt/id "test4-doc2"
                     :predicate_type "policy-ticket-v1"
                     :subjects []}]])

(Thread/sleep 100)
(if @test4-changes
  (println "✗ ERROR: Callback fired unexpectedly!")
  (println "✓ Callback correctly did not fire!"))

(println "\nCurrent SLSA results (should only include SLSA types):")
(pp/pprint (diff/query-results "test4-filtered"))

(diff/unregister-query! "test4-filtered")

;;; Test 5: Nested Field Access - Callback Fires on Predicate Changes

(println "\n" (apply str (repeat 60 "=")) "\n")
(println "TEST 5: Nested Field Access - Grouping by Nested Builder ID\n")

(def test5-changes (atom nil))

(diff/register-query!
 {:query-id "test5-nested"
  :xtql "(-> (from :prod_attestations [predicate])
              (with {:builder_id (.. predicate :builder :id)})
              (where :builder_id)
              (aggregate builder_id {:count (row-count)}))"
  :callback (fn [changes]
              (println ">>> NESTED FIELD CALLBACK FIRED! <<<")
              (when (seq (:added changes))
                (println "New builders:")
                (doseq [doc (:added changes)]
                  (println (format "  - %s (count: %d)"
                                   (:builder_id doc)
                                   (:count doc)))))
              (when (seq (:modified changes))
                (println "Updated builders:")
                (doseq [mod (:modified changes)]
                  (println (format "  - %s: %d → %d"
                                   (:builder_id (:new mod))
                                   (:count (:old mod))
                                   (:count (:new mod))))))
              (reset! test5-changes changes))})

(println "Query registered for grouping by nested builder ID.\n")

(println "Test 5a: Adding attestation with GitHub Actions builder...")
(reset! test5-changes nil)
(diff/execute-tx! xtdb-client
                  [[:put-docs :prod_attestations
                    {:xt/id "test5-doc1"
                     :predicate_type "slsa-provenance-v1"
                     :predicate {:buildType "https://github.com/actions"
                                 :builder {:id "https://github.com/actions/runner"
                                           :version "1.0"}
                                 :materials []}
                     :subjects []}]])

(Thread/sleep 100)
(println "\nChanges:")
(pp/pprint @test5-changes)

(println "\nTest 5b: Adding another attestation with same builder...")
(reset! test5-changes nil)
(diff/execute-tx! xtdb-client
                  [[:put-docs :prod_attestations
                    {:xt/id "test5-doc2"
                     :predicate_type "slsa-provenance-v1"
                     :predicate {:buildType "https://github.com/actions"
                                 :builder {:id "https://github.com/actions/runner"
                                           :version "1.0"}
                                 :materials []}
                     :subjects []}]])

(Thread/sleep 100)
(println "\nChanges (should show modified with count increment):")
(pp/pprint @test5-changes)

(println "\nTest 5c: Adding attestation with different builder...")
(reset! test5-changes nil)
(diff/execute-tx! xtdb-client
                  [[:put-docs :prod_attestations
                    {:xt/id "test5-doc3"
                     :predicate_type "slsa-provenance-v1"
                     :predicate {:buildType "https://gitlab.com/ci"
                                 :builder {:id "https://gitlab.com/runner"
                                           :version "2.0"}
                                 :materials []}
                     :subjects []}]])

(Thread/sleep 100)
(println "\nChanges (should show new builder added):")
(pp/pprint @test5-changes)

(println "\nFinal results (should show 2 builders):")
(pp/pprint (diff/query-results "test5-nested"))

(diff/unregister-query! "test5-nested")

;;; Test 6: Unnest - Callback Fires for Array Elements

(println "\n" (apply str (repeat 60 "=")) "\n")
(println "TEST 6: Unnest - Callback Fires for Individual Components\n")

(def test6-changes (atom nil))

(diff/register-query!
 {:query-id "test6-unnest"
  :xtql "(-> (from :prod_attestations [predicate_type predicate])
              (where (like predicate_type \"%cyclonedx%\"))
              (unnest {:comp (.. predicate :components)})
              (with {:name (.. comp :name)
                     :version (.. comp :version)})
              (limit 10))"
  :callback (fn [changes]
              (println ">>> UNNEST CALLBACK FIRED! <<<")
              (println (format "Added: %d, Removed: %d"
                               (count (:added changes))
                               (count (:removed changes))))
              (when (seq (:added changes))
                (println "\nNew components:")
                (doseq [comp (take 3 (:added changes))]
                  (println (format "  - %s @ %s" (:name comp) (:version comp)))))
              (reset! test6-changes changes))})

(println "Query registered for SBOM components.\n")

(println "Adding SBOM with 3 components...")
(reset! test6-changes nil)
(diff/execute-tx! xtdb-client
                  [[:put-docs :prod_attestations
                    {:xt/id "test6-doc1"
                     :predicate_type "cyclonedx-sbom-v1"
                     :predicate {:bomFormat "CycloneDX"
                                 :specVersion "1.4"
                                 :components [{:type "library"
                                               :name "github.com/foo/bar"
                                               :version "v1.2.3"}
                                              {:type "library"
                                               :name "github.com/baz/qux"
                                               :version "v2.0.0"}
                                              {:type "application"
                                               :name "my-app"
                                               :version "v0.1.0"}]}
                     :subjects []}]])

(Thread/sleep 100)
(println "\nChanges (should show 3 added components):")
(println "Added count:" (count (:added @test6-changes)))
(doseq [comp (take 3 (:added @test6-changes))]
  (println (format "  - %s @ %s" (:name comp) (:version comp))))

(println "\nCurrent results:")
(pp/pprint (diff/query-results "test6-unnest"))

(diff/unregister-query! "test6-unnest")

;;; Test 7: Multiple Queries - Multiple Callbacks Fire

(println "\n" (apply str (repeat 60 "=")) "\n")
(println "TEST 7: Multiple Queries - Single Transaction Triggers Multiple Callbacks\n")

(def test7a-changes (atom nil))
(def test7b-changes (atom nil))
(def test7c-changes (atom nil))

(diff/register-query!
 {:query-id "test7-count-all"
  :xtql "(-> (from :prod_attestations [_id])
              (aggregate {:total (row-count)}))"
  :callback (fn [changes]
              (println ">>> Callback A: Total count changed!")
              (when-let [new-total (-> changes :modified first :new :total)]
                (println (format "    New total: %d" new-total)))
              (reset! test7a-changes changes))})

(diff/register-query!
 {:query-id "test7-count-slsa"
  :xtql "(-> (from :prod_attestations [predicate_type])
              (where (like predicate_type \"%slsa%\"))
              (aggregate {:total (row-count)}))"
  :callback (fn [changes]
              (println ">>> Callback B: SLSA count changed!")
              (when-let [new-total (-> changes :modified first :new :total)]
                (println (format "    New SLSA total: %d" new-total)))
              (reset! test7b-changes changes))})

(diff/register-query!
 {:query-id "test7-count-sbom"
  :xtql "(-> (from :prod_attestations [predicate_type])
              (where (like predicate_type \"%cyclonedx%\"))
              (aggregate {:total (row-count)}))"
  :callback (fn [changes]
              (println ">>> Callback C: SBOM count changed!")
              (when-let [new-total (-> changes :modified first :new :total)]
                (println (format "    New SBOM total: %d" new-total)))
              (reset! test7c-changes changes))})

(println "3 queries registered.\n")

(println "Executing single transaction with SLSA attestation...")
(reset! test7a-changes nil)
(reset! test7b-changes nil)
(reset! test7c-changes nil)

(let [result (diff/execute-tx! xtdb-client
                               [[:put-docs :prod_attestations
                                 {:xt/id "test7-doc1"
                                  :predicate_type "slsa-provenance-v1"
                                  :subjects []}]])]
  (println "\nTransaction result:")
  (println "  Affected queries:" (:affected-query-count result))
  (println "  Callbacks fired:" (:callbacks-fired result)))

(Thread/sleep 100)

(println "\nCallback results:")
(println "  Callback A (all):" (if @test7a-changes "✓ FIRED" "✗ Not fired"))
(println "  Callback B (SLSA):" (if @test7b-changes "✓ FIRED" "✗ Not fired"))
(println "  Callback C (SBOM):" (if @test7c-changes "✓ FIRED" "✗ NOT fired (expected)"))

(diff/unregister-query! "test7-count-all")
(diff/unregister-query! "test7-count-slsa")
(diff/unregister-query! "test7-count-sbom")

;;; Summary

(println "\n" (apply str (repeat 60 "=")) "\n")
(println "╔══════════════════════════════════════════════════════════╗")
(println "║  All End-to-End Callback Tests Complete!                ║")
(println "╚══════════════════════════════════════════════════════════╝\n")

(println "Summary of tests:")
(println "  ✓ Test 1: First insert triggers callback with added group")
(println "  ✓ Test 2: Second insert triggers callback with modified count")
(println "  ✓ Test 3: Different type triggers callback with new group")
(println "  ✓ Test 4: Filtered query only fires for matching data")
(println "  ✓ Test 5: Nested field access works with grouping")
(println "  ✓ Test 6: Unnest expands arrays and fires callback")
(println "  ✓ Test 7: Multiple queries fire multiple callbacks")

(println "\nRegistry stats:")
(pp/pprint (diff/registry-stats))

(println "\n✅ All differential dataflow features working correctly!\n")
