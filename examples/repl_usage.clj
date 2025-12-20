(ns examples.repl-usage
  "REPL examples for differential dataflow experimentation.

  This file demonstrates how to use the differential dataflow system
  to register queries and receive incremental updates."
  (:require [xtflow.core :as diff]
            [xtdb.api :as xt]
            [clojure.pprint :as pp]))

;;; Setup

(comment
  ;; Start XTDB if not already running
  ;; Run: ./scripts/run-xtdb-docker.sh

  ;; Create XTDB client
  (def xtdb-client (xt/client {:host "localhost" :port 5432 :user "xtdb"})))

;;; Example 1: Simple Aggregation - Count Attestations by Type

(comment
  ;; Register a query that counts attestations by predicate_type
  (diff/register-query!
   {:query-id "attestation-counts"
    :xtql "(-> (from :prod_attestations [predicate_type])
                (aggregate predicate_type {:count (row-count)}))"
    :callback (fn [changes]
                (println "\n=== Attestation Counts Changed ===")
                (when (seq (:added changes))
                  (println "NEW TYPES:")
                  (doseq [doc (:added changes)]
                    (println (format "  %s: %d" (:predicate_type doc) (:count doc)))))
                (when (seq (:modified changes))
                  (println "UPDATED COUNTS:")
                  (doseq [mod (:modified changes)]
                    (println (format "  %s: %d → %d"
                                     (:predicate_type (:new mod))
                                     (:count (:old mod))
                                     (:count (:new mod))))))
                (when (seq (:removed changes))
                  (println "REMOVED TYPES:")
                  (doseq [doc (:removed changes)]
                    (println (format "  %s" (:predicate_type doc))))))})

  ;; Simulate adding attestations
  (diff/execute-tx! xtdb-client
                    [[:put-docs :prod_attestations
                      {:xt/id "test-1"
                       :predicate_type "slsa-provenance-v1"
                       :subjects [{:name "gcr.io/test-image"}]}]])

  ;; Add more of the same type
  (diff/execute-tx! xtdb-client
                    [[:put-docs :prod_attestations
                      {:xt/id "test-2"
                       :predicate_type "slsa-provenance-v1"
                       :subjects [{:name "gcr.io/another-image"}]}]])

  ;; View current results
  (pp/pprint (diff/query-results "attestation-counts"))

  ;; Unregister
  (diff/unregister-query! "attestation-counts"))

;;; Example 2: Filtered Aggregation - SLSA Attestations Only

(comment
  (diff/register-query!
   {:query-id "slsa-counts"
    :xtql "(-> (from :prod_attestations [predicate_type])
                (where (like predicate_type \"%slsa%\"))
                (aggregate predicate_type {:count (row-count)}))"
    :callback (fn [changes]
                (println "\n=== SLSA Counts Changed ===")
                (pp/pprint changes))})

  ;; Add SLSA attestation - will trigger callback
  (diff/execute-tx! xtdb-client
                    [[:put-docs :prod_attestations
                      {:xt/id "slsa-1"
                       :predicate_type "slsa-provenance-v1"
                       :subjects []}]])

  ;; Add non-SLSA attestation - will NOT trigger callback
  (diff/execute-tx! xtdb-client
                    [[:put-docs :prod_attestations
                      {:xt/id "sbom-1"
                       :predicate_type "cyclonedx-sbom"
                       :subjects []}]])

  (diff/unregister-query! "slsa-counts"))

;;; Example 3: Nested Field Access - GitHub Actions Builds

(comment
  ;; Monitor SLSA attestations from GitHub Actions by accessing nested builder ID
  (diff/register-query!
   {:query-id "github-builds"
    :xtql "(-> (from :prod_attestations [predicate])
                (where (like predicate_type \"%slsa%\"))
                (with {:builder_id (.. predicate :builder :id)})
                (where (like builder_id \"%github%\"))
                (aggregate builder_id {:count (row-count)}))"
    :callback (fn [changes]
                (println "\n=== GitHub Actions Builds Changed ===")
                (doseq [mod (:modified changes)]
                  (println (format "Builder %s: %d → %d builds"
                                   (:builder_id (:new mod))
                                   (:count (:old mod))
                                   (:count (:new mod))))))})

  ;; Add attestation with nested builder field
  (diff/execute-tx! xtdb-client
                    [[:put-docs :prod_attestations
                      {:xt/id "github-1"
                       :predicate_type "slsa-provenance-v1"
                       :predicate {:buildType "https://github.com/actions"
                                   :builder {:id "https://github.com/actions/runner"}
                                   :materials []}
                       :subjects []}]])

  ;; Add another from same builder
  (diff/execute-tx! xtdb-client
                    [[:put-docs :prod_attestations
                      {:xt/id "github-2"
                       :predicate_type "slsa-provenance-v1"
                       :predicate {:buildType "https://github.com/actions"
                                   :builder {:id "https://github.com/actions/runner"}
                                   :materials []}
                       :subjects []}]])

  (pp/pprint (diff/query-results "github-builds"))

  (diff/unregister-query! "github-builds"))

;;; Example 4: Unnest - Track SBOM Components

(comment
  ;; Monitor individual SBOM components (not aggregated)
  (diff/register-query!
   {:query-id "sbom-components"
    :xtql "(-> (from :prod_attestations [predicate_type predicate])
                (where (like predicate_type \"%cyclonedx%\"))
                (unnest {:comp (.. predicate :components)})
                (with {:name (.. comp :name)
                       :version (.. comp :version)})
                (limit 100))"
    :callback (fn [changes]
                (println "\n=== SBOM Components Changed ===")
                (println "Added:" (count (:added changes)))
                (println "Removed:" (count (:removed changes)))
                (when (seq (:added changes))
                  (println "Sample new components:")
                  (doseq [comp (take 5 (:added changes))]
                    (println (format "  - %s @ %s" (:name comp) (:version comp))))))})

  ;; Add SBOM with components
  (diff/execute-tx! xtdb-client
                    [[:put-docs :prod_attestations
                      {:xt/id "sbom-1"
                       :predicate_type "cyclonedx-sbom-v1"
                       :predicate {:bomFormat "CycloneDX"
                                   :components [{:type "library"
                                                 :name "github.com/foo/bar"
                                                 :version "v1.2.3"}
                                                {:type "library"
                                                 :name "github.com/baz/qux"
                                                 :version "v2.0.0"}]}
                       :subjects []}]])

  (pp/pprint (diff/query-results "sbom-components"))

  (diff/unregister-query! "sbom-components"))

;;; Example 5: Monitor Specific Image Digest

(comment
  (def target-digest "abc123def456")

  (diff/register-query!
   {:query-id "my-image-attestations"
    :xtql (format "(-> (from :prod_attestations [subjects predicate_type])
                        (unnest {:s subjects})
                        (with {:digest (.. s :digest :sha256)})
                        (where (= digest \"%s\")))"
                  target-digest)
    :callback (fn [changes]
                (when (seq (:added changes))
                  (println "\n=== NEW ATTESTATION FOR MY IMAGE ===")
                  (doseq [att (:added changes)]
                    (println (format "Type: %s" (:predicate_type att))))))})

  ;; Add attestation for this image
  (diff/execute-tx! xtdb-client
                    [[:put-docs :prod_attestations
                      {:xt/id "my-att-1"
                       :predicate_type "slsa-provenance-v1"
                       :subjects [{:name "gcr.io/my-image:latest"
                                   :digest {:sha256 target-digest}}]}]])

  (diff/unregister-query! "my-image-attestations"))

;;; Example 6: Multiple Queries on Same Table

(comment
  ;; Register multiple queries that all watch prod_attestations
  (diff/register-query!
   {:query-id "count-all"
    :xtql "(-> (from :prod_attestations [_id])
                (aggregate {:total (row-count)}))"
    :callback (fn [changes]
                (println "Total attestations changed:"
                         (-> changes :modified first :new :total)))})

  (diff/register-query!
   {:query-id "count-slsa"
    :xtql "(-> (from :prod_attestations [predicate_type])
                (where (like predicate_type \"%slsa%\"))
                (aggregate {:total (row-count)}))"
    :callback (fn [changes]
                (println "SLSA attestations changed:"
                         (-> changes :modified first :new :total)))})

  (diff/register-query!
   {:query-id "count-sbom"
    :xtql "(-> (from :prod_attestations [predicate_type])
                (where (like predicate_type \"%cyclonedx%\"))
                (aggregate {:total (row-count)}))"
    :callback (fn [changes]
                (println "SBOM attestations changed:"
                         (-> changes :modified first :new :total)))})

  ;; Single transaction triggers all affected queries
  (diff/execute-tx! xtdb-client
                    [[:put-docs :prod_attestations
                      {:xt/id "multi-1"
                       :predicate_type "slsa-provenance-v1"
                       :subjects []}]])
  ;; Output: "Total attestations changed: 1" and "SLSA attestations changed: 1"
  ;; (SBOM query not affected)

  (diff/list-queries)

  (diff/unregister-query! "count-all")
  (diff/unregister-query! "count-slsa")
  (diff/unregister-query! "count-sbom"))

;;; Utility Functions

(comment
  ;; List all registered queries
  (diff/list-queries)

  ;; Get query info
  (diff/query-info "attestation-counts")

  ;; Get current results for a query
  (diff/query-results "attestation-counts")

  ;; Registry statistics
  (diff/registry-stats)

  ;; Reset all queries (for testing)
  (diff/reset-all-queries!))

;;; Testing Nested Field Access with Real Predicate Structures

(comment
  ;; Test with SLSA provenance structure
  (diff/register-query!
   {:query-id "test-nested"
    :xtql "(-> (from :prod_attestations [predicate])
                (with {:build_type (.. predicate :buildType)
                       :builder_id (.. predicate :builder :id)
                       :material_count (.. predicate :materials)}))"
    :callback (fn [changes]
                (println "Nested field extraction:")
                (pp/pprint changes))})

  (diff/execute-tx! xtdb-client
                    [[:put-docs :prod_attestations
                      {:xt/id "nested-test"
                       :predicate {:buildType "https://example.com/build"
                                   :builder {:id "https://builder.example.com"
                                             :version "1.0"}
                                   :materials [{:uri "git+https://github.com/repo"}
                                               {:uri "pkg:npm/package"}]}
                       :predicate_type "test"}]])

  (diff/query-results "test-nested")

  (diff/unregister-query! "test-nested"))
