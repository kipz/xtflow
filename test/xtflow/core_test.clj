(ns xtflow.core-test
  "Integration tests for differential dataflow callback functionality."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [xtflow.core :as diff]
            [xtflow.operators :as ops]
            [xtflow.dataflow :as df]
            [xtflow.test-fixtures :refer [*xtdb-client*
                                          xtdb-container-fixture
                                          clear-tables-fixture]]))

;; Container started once for entire namespace
(use-fixtures :once xtdb-container-fixture)

;; Tables cleared between each test
(use-fixtures :each clear-tables-fixture)

(deftest test-basic-aggregation-callback
  (testing "First insert triggers callback with added group"
    (let [changes (atom nil)]
      (diff/register-query!
       {:query-id "test-basic-agg"
        :xtql "(-> (from :prod_attestations [predicate_type])
                   (aggregate predicate_type {:count (row-count)}))"
        :callback (fn [c] (reset! changes c))})

      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "test-basic-1"
                           :predicate_type "slsa-provenance-v1"
                           :subjects []}]])

      (is (not (nil? @changes)) "Callback should fire")
      (is (= 1 (count (:added @changes))) "Should have one added group")
      (is (= "slsa-provenance-v1" (-> @changes :added first :group_key)))
      (is (= 1 (-> @changes :added first :count)))

      (diff/unregister-query! "test-basic-agg"))))

(deftest test-incremental-count-update
  (testing "Second insert increments count in modified group"
    (let [changes (atom nil)]
      (diff/register-query!
       {:query-id "test-increment"
        :xtql "(-> (from :prod_attestations [predicate_type])
                   (aggregate predicate_type {:count (row-count)}))"
        :callback (fn [c] (reset! changes c))})

      ;; First insert
      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "test-inc-1"
                           :predicate_type "slsa-provenance-v1"
                           :subjects []}]])

      (reset! changes nil)

      ;; Second insert of same type
      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "test-inc-2"
                           :predicate_type "slsa-provenance-v1"
                           :subjects []}]])

      (is (not (nil? @changes)) "Callback should fire on second insert")
      (is (= 1 (count (:added @changes))) "Should have new count added")
      (is (= 1 (count (:removed @changes))) "Should have old count removed")
      (is (= 2 (-> @changes :added first :count)) "New count should be 2")

      (diff/unregister-query! "test-increment"))))

(deftest test-filtered-query-callback
  (testing "WHERE clause filters correctly and callbacks fire only for matching data"
    (let [changes (atom nil)]
      (diff/register-query!
       {:query-id "test-filtered"
        :xtql "(-> (from :prod_attestations [predicate_type])
                   (where (like predicate_type \"%slsa%\"))
                   (aggregate predicate_type {:count (row-count)}))"
        :callback (fn [c] (reset! changes c))})

      ;; Add SLSA attestation - should trigger
      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "test-filter-1"
                           :predicate_type "slsa-provenance-v2"
                           :subjects []}]])

      (is (not (nil? @changes)) "Callback should fire for SLSA attestation")
      (is (= 1 (count (:added @changes))))

      (reset! changes nil)

      ;; Add non-SLSA attestation - should NOT trigger
      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "test-filter-2"
                           :predicate_type "policy-ticket-v1"
                           :subjects []}]])

      (is (nil? @changes) "Callback should NOT fire for non-SLSA attestation")

      (diff/unregister-query! "test-filtered"))))

(deftest test-nested-field-access
  (testing "WITH operator extracts nested fields and aggregates correctly"
    (let [changes (atom nil)]
      (diff/register-query!
       {:query-id "test-nested"
        :xtql "(-> (from :prod_attestations [predicate])
                   (with {:builder_id (.. predicate :builder :id)})
                   (where :builder_id)
                   (aggregate builder_id {:count (row-count)}))"
        :callback (fn [c] (reset! changes c))})

      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "test-nested-1"
                           :predicate_type "slsa-provenance-v1"
                           :predicate {:buildType "https://github.com/actions"
                                       :builder {:id "https://github.com/actions/runner"
                                                 :version "1.0"}
                                       :materials []}
                           :subjects []}]])

      (is (not (nil? @changes)) "Callback should fire")
      (is (= 1 (count (:added @changes))))
      (is (= "https://github.com/actions/runner" (-> @changes :added first :group_key)))
      (is (= 1 (-> @changes :added first :count)))

      (diff/unregister-query! "test-nested"))))

(deftest test-unnest-operator
  (testing "UNNEST expands arrays and fires callbacks for individual elements"
    (let [changes (atom nil)]
      (diff/register-query!
       {:query-id "test-unnest"
        :xtql "(-> (from :prod_attestations [predicate_type predicate])
                   (where (like predicate_type \"%cyclonedx%\"))
                   (unnest {:comp (.. predicate :components)})
                   (with {:name (.. comp :name)
                          :version (.. comp :version)})
                   (limit 10))"
        :callback (fn [c] (reset! changes c))})

      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "test-unnest-1"
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

      (is (not (nil? @changes)) "Callback should fire")
      (is (= 3 (count (:added @changes))) "Should have 3 components added")
      (is (some #(= "github.com/foo/bar" (:name %)) (:added @changes)))
      (is (some #(= "github.com/baz/qux" (:name %)) (:added @changes)))
      (is (some #(= "my-app" (:name %)) (:added @changes)))

      (diff/unregister-query! "test-unnest"))))

(deftest test-multiple-queries-single-transaction
  (testing "Single transaction triggers multiple callbacks for affected queries"
    (let [changes-a (atom nil)
          changes-b (atom nil)
          changes-c (atom nil)]

      (diff/register-query!
       {:query-id "test-multi-all"
        :xtql "(-> (from :prod_attestations [_id])
                   (aggregate {:total (row-count)}))"
        :callback (fn [c] (reset! changes-a c))})

      (diff/register-query!
       {:query-id "test-multi-slsa"
        :xtql "(-> (from :prod_attestations [predicate_type])
                   (where (like predicate_type \"%slsa%\"))
                   (aggregate {:total (row-count)}))"
        :callback (fn [c] (reset! changes-b c))})

      (diff/register-query!
       {:query-id "test-multi-sbom"
        :xtql "(-> (from :prod_attestations [predicate_type])
                   (where (like predicate_type \"%cyclonedx%\"))
                   (aggregate {:total (row-count)}))"
        :callback (fn [c] (reset! changes-c c))})

      (let [result (diff/execute-tx! *xtdb-client*
                                     [[:put-docs :prod_attestations
                                       {:xt/id "test-multi-1"
                                        :predicate_type "slsa-provenance-v1"
                                        :subjects []}]])]

        (is (= 3 (:affected-query-count result)) "Should affect 3 queries")
        (is (= 2 (:callbacks-fired result)) "Should fire 2 callbacks")
        (is (not (nil? @changes-a)) "Callback A should fire (all attestations)")
        (is (not (nil? @changes-b)) "Callback B should fire (SLSA)")
        (is (nil? @changes-c) "Callback C should NOT fire (SBOM)"))

      (diff/unregister-query! "test-multi-all")
      (diff/unregister-query! "test-multi-slsa")
      (diff/unregister-query! "test-multi-sbom"))))

(deftest test-query-registration-and-management
  (testing "Query registration and unregistration work correctly"
    (is (= 0 (count (diff/list-queries))) "Should start with no queries")

    (diff/register-query!
     {:query-id "test-mgmt"
      :xtql "(-> (from :prod_attestations [predicate_type]))"
      :callback (fn [_] nil)})

    (is (= 1 (count (diff/list-queries))) "Should have 1 query after registration")
    (is (not (nil? (diff/query-info "test-mgmt"))) "Query info should be available")

    (diff/unregister-query! "test-mgmt")

    (is (= 0 (count (diff/list-queries))) "Should have no queries after unregistration")
    (is (nil? (diff/query-info "test-mgmt")) "Query info should be nil after unregistration")))

(deftest test-registry-stats
  (testing "Registry stats return correct information"
    (diff/register-query!
     {:query-id "test-stats-1"
      :xtql "(-> (from :prod_attestations [predicate_type]))"
      :callback (fn [_] nil)})

    (diff/register-query!
     {:query-id "test-stats-2"
      :xtql "(-> (from :prod_attestations [predicate_type]))"
      :callback (fn [_] nil)})

    (let [stats (diff/registry-stats)]
      (is (= 2 (:query-count stats)) "Should have 2 queries")
      (is (contains? (:queries-by-table stats) :prod_attestations) "Should track prod_attestations table"))

    (diff/unregister-query! "test-stats-1")
    (diff/unregister-query! "test-stats-2")))

(deftest test-library-version-image-count
  (testing "Count of images containing specific library version triggers callbacks"
    (let [changes (atom nil)
          target-lib "github.com/example/vulnerable-lib"
          target-version "v1.2.3"]

      ;; Register query to count occurrences of the target library version across images
      ;; Each SBOM that contains this library will contribute to the count
      (diff/register-query!
       {:query-id "test-lib-count"
        :xtql (format "(-> (from :prod_attestations [predicate_type predicate])
                           (where (like predicate_type \"%%cyclonedx%%\"))
                           (unnest {:comp (.. predicate :components)})
                           (with {:lib_name (.. comp :name)
                                  :lib_version (.. comp :version)})
                           (where (= lib_name \"%s\"))
                           (where (= lib_version \"%s\"))
                           (aggregate {:count (row-count)}))"
                      target-lib
                      target-version)
        :callback (fn [c] (reset! changes c))})

      ;; First image with the vulnerable library
      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "sbom-image-1"
                           :predicate_type "cyclonedx-sbom-v1"
                           :predicate {:bomFormat "CycloneDX"
                                       :specVersion "1.4"
                                       :components [{:type "library"
                                                     :name target-lib
                                                     :version target-version}
                                                    {:type "library"
                                                     :name "github.com/other/lib"
                                                     :version "v2.0.0"}]}
                           :subjects [{:name "gcr.io/project/app-a:latest"
                                       :digest {:sha256 "abc123"}}]}]])

      (is (not (nil? @changes)) "Callback should fire for first image")
      (is (= 1 (count (:added @changes))) "Should have one added result")
      (is (= 1 (-> @changes :added first :count)) "Count should be 1 for first image")

      (reset! changes nil)

      ;; Second image with the same vulnerable library
      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "sbom-image-2"
                           :predicate_type "cyclonedx-sbom-v1"
                           :predicate {:bomFormat "CycloneDX"
                                       :specVersion "1.4"
                                       :components [{:type "library"
                                                     :name target-lib
                                                     :version target-version}
                                                    {:type "library"
                                                     :name "github.com/another/dep"
                                                     :version "v1.0.0"}]}
                           :subjects [{:name "gcr.io/project/app-b:v2"
                                       :digest {:sha256 "def456"}}]}]])

      (is (not (nil? @changes)) "Callback should fire for second image")
      (is (= 1 (count (:added @changes))) "Should have new count added")
      (is (= 1 (count (:removed @changes))) "Should have old count removed")
      (is (= 2 (-> @changes :added first :count)) "Count should be 2 after second image")
      (is (= 1 (-> @changes :removed first :count)) "Old count should be 1")

      (reset! changes nil)

      ;; Third image with different library version - should NOT affect count
      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "sbom-image-3"
                           :predicate_type "cyclonedx-sbom-v1"
                           :predicate {:bomFormat "CycloneDX"
                                       :specVersion "1.4"
                                       :components [{:type "library"
                                                     :name target-lib
                                                     :version "v2.0.0"}]}  ; Different version!
                           :subjects [{:name "gcr.io/project/app-c:latest"
                                       :digest {:sha256 "ghi789"}}]}]])

      (is (nil? @changes) "Callback should NOT fire for different library version")

      ;; Verify final count is still 2
      (let [results (diff/query-results "test-lib-count")]
        (is (= 1 (count results)) "Should have one aggregate result")
        (is (= 2 (-> results first :count)) "Final count should still be 2"))

      (diff/unregister-query! "test-lib-count"))))

(deftest test-all-operators-complex-sbom
  (testing "All 6 operators with deep SBOM query - nested license extraction and aggregation"
    (let [changes (atom nil)]

      ;; Register query using ALL operators: FROM, WHERE, UNNEST, WITH, WHERE, AGGREGATE, LIMIT
      ;; Query deep into SBOM structure: components -> licenses array -> license object -> id field
      (diff/register-query!
       {:query-id "test-all-ops"
        :xtql "(-> (from :prod_attestations [predicate_type predicate])
                   (where (like predicate_type \"%cyclonedx%\"))
                   (unnest {:comp (.. predicate :components)})
                   (with {:comp_name (.. comp :name)
                          :comp_type (.. comp :type)
                          :licenses (.. comp :licenses)})
                   (where :licenses)
                   (unnest {:lic licenses})
                   (with {:license_id (.. lic :license :id)
                          :license_name (.. lic :license :name)})
                   (where (like license_id \"%MIT%\"))
                   (aggregate license_id {:count (row-count)})
                   (limit 5))"
        :callback (fn [c] (reset! changes c))})

      ;; First SBOM: Complex structure with multiple components and nested licenses
      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "complex-sbom-1"
                           :predicate_type "cyclonedx-sbom-v1"
                           :predicate {:bomFormat "CycloneDX"
                                       :specVersion "1.4"
                                       :components [{:type "library"
                                                     :name "github.com/user/lib-a"
                                                     :version "v1.0.0"
                                                     :licenses [{:license {:id "MIT"
                                                                           :name "MIT License"
                                                                           :url "https://opensource.org/licenses/MIT"}}]}
                                                    {:type "library"
                                                     :name "github.com/user/lib-b"
                                                     :version "v2.1.0"
                                                     :licenses [{:license {:id "Apache-2.0"
                                                                           :name "Apache License 2.0"}}]}
                                                    {:type "application"
                                                     :name "my-service"
                                                     :version "v1.0.0"
                                                     :licenses [{:license {:id "MIT"
                                                                           :name "MIT License"}}
                                                                {:license {:id "BSD-3-Clause"
                                                                           :name "BSD 3-Clause"}}]}]}
                           :subjects [{:name "gcr.io/project/image-1:v1"}]}]])

      (is (not (nil? @changes)) "Callback should fire for first SBOM")
      (is (= 1 (count (:added @changes))) "Should have one added aggregate result")
      ;; MIT appears in 2 components (lib-a and my-service), so count should be 2
      (is (= "MIT" (-> @changes :added first :group_key)) "Should group by MIT license")
      (is (= 2 (-> @changes :added first :count)) "Should count 2 MIT occurrences")

      (reset! changes nil)

      ;; Second SBOM: More complex with different licenses including MIT variants
      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "complex-sbom-2"
                           :predicate_type "cyclonedx-sbom-v1.5"
                           :predicate {:bomFormat "CycloneDX"
                                       :specVersion "1.5"
                                       :components [{:type "library"
                                                     :name "npm/package-x"
                                                     :version "3.2.1"
                                                     :licenses [{:license {:id "MIT"
                                                                           :name "MIT License"}}]}
                                                    {:type "library"
                                                     :name "pypi/package-y"
                                                     :version "1.0.0"
                                                     :licenses [{:license {:id "MIT-0"
                                                                           :name "MIT No Attribution"}}]}
                                                    {:type "library"
                                                     :name "maven/lib-z"
                                                     :version "2.3.4"
                                                     :licenses [{:license {:id "GPL-3.0"
                                                                           :name "GNU General Public License v3.0"}}]}]}
                           :subjects [{:name "gcr.io/project/image-2:v2"}]}]])

      (is (not (nil? @changes)) "Callback should fire for second SBOM")
      ;; Now we have:
      ;; - MIT: was 2, now +1 = 3 total
      ;; - MIT-0: new, 1 total
      (is (= 2 (count (:added @changes))) "Should have 2 new aggregate results")
      (is (= 1 (count (:removed @changes))) "Should remove old MIT count")

      ;; Find MIT in the results
      (let [mit-result (first (filter #(= "MIT" (:group_key %)) (:added @changes)))]
        (is (not (nil? mit-result)) "Should have MIT in added results")
        (is (= 3 (:count mit-result)) "MIT count should now be 3"))

      ;; Find MIT-0 in the results
      (let [mit0-result (first (filter #(= "MIT-0" (:group_key %)) (:added @changes)))]
        (is (not (nil? mit0-result)) "Should have MIT-0 in added results")
        (is (= 1 (:count mit0-result)) "MIT-0 count should be 1"))

      (reset! changes nil)

      ;; Third SBOM: No MIT licenses - should NOT trigger callback
      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "complex-sbom-3"
                           :predicate_type "cyclonedx-sbom-v1"
                           :predicate {:components [{:type "library"
                                                     :name "some-lib"
                                                     :licenses [{:license {:id "Apache-2.0"}}]}]}
                           :subjects [{:name "gcr.io/project/image-3:v3"}]}]])

      (is (nil? @changes) "Callback should NOT fire when no MIT licenses present")

      ;; Verify LIMIT works - should have at most 5 license types in results
      (let [results (diff/query-results "test-all-ops")]
        (is (<= (count results) 5) "LIMIT should restrict results to at most 5")
        (is (= 2 (count results)) "Should have 2 license types (MIT and MIT-0)"))

      (diff/unregister-query! "test-all-ops"))))

(deftest test-return-operator-callback
  (testing "RETURN operator projects only specified fields and triggers callbacks correctly"
    (let [changes (atom nil)]
      (diff/register-query!
       {:query-id "test-return"
        :xtql "(-> (from :prod_attestations [predicate_type predicate subjects])
                   (where (like predicate_type \"%slsa%\"))
                   (return predicate_type subjects))"
        :callback (fn [c] (reset! changes c))})

      ;; Insert attestation with multiple fields
      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "test-return-1"
                           :predicate_type "slsa-provenance-v1"
                           :predicate {:buildType "https://github.com/actions"
                                       :builder {:id "github-actions"}}
                           :subjects [{:name "gcr.io/project/image:v1"
                                       :digest {:sha256 "abc123"}}]}]])

      (is (not (nil? @changes)) "Callback should fire")
      (is (= 1 (count (:added @changes))) "Should have one added result")

      (let [result (first (:added @changes))]
        ;; Should only have predicate_type and subjects, not predicate
        (is (= "slsa-provenance-v1" (:predicate_type result)) "Should have predicate_type")
        (is (vector? (:subjects result)) "Should have subjects")
        (is (nil? (:predicate result)) "Should NOT have predicate field")
        (is (= 2 (count (keys result))) "Should only have 2 fields"))

      (diff/unregister-query! "test-return"))))

(deftest test-return-with-aggregate-callback
  (testing "RETURN operator combined with aggregate for clean results"
    (let [changes (atom nil)]
      (diff/register-query!
       {:query-id "test-return-agg"
        :xtql "(-> (from :prod_attestations [predicate_type subjects])
                   (aggregate predicate_type {:count (row-count)})
                   (return group_key count))"
        :callback (fn [c] (reset! changes c))})

      ;; First insert
      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "test-return-agg-1"
                           :predicate_type "cyclonedx-sbom-v1"
                           :subjects []}]])

      (is (not (nil? @changes)) "Callback should fire")
      (is (= 1 (count (:added @changes))))

      (let [result (first (:added @changes))]
        ;; AGGREGATE creates group_key field when group-by is a symbol
        ;; RETURN filters to only group_key and count
        (is (= "cyclonedx-sbom-v1" (:group_key result)))
        (is (= 1 (:count result)))
        (is (= 2 (count (keys result))) "Should only have 2 fields: group_key and count"))

      (reset! changes nil)

      ;; Second insert of same type
      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "test-return-agg-2"
                           :predicate_type "cyclonedx-sbom-v1"
                           :subjects []}]])

      (is (not (nil? @changes)) "Callback should fire on second insert")
      (let [added (first (:added @changes))]
        (is (= 2 (:count added)) "Count should be incremented to 2")
        (is (= 2 (count (keys added))) "Should still only have 2 fields"))

      (diff/unregister-query! "test-return-agg"))))

(deftest test-without-operator-callback
  (testing "WITHOUT operator removes sensitive fields and triggers callbacks correctly"
    (let [changes (atom nil)]
      (diff/register-query!
       {:query-id "test-without"
        :xtql "(-> (from :prod_attestations [predicate_type predicate subjects])
                   (where (like predicate_type \"%slsa%\"))
                   (without predicate))"
        :callback (fn [c] (reset! changes c))})

      ;; Insert attestation with sensitive predicate field
      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "test-without-1"
                           :predicate_type "slsa-provenance-v1"
                           :predicate {:buildType "https://github.com/actions"
                                       :builder {:id "github-actions"}
                                       :secretToken "sensitive-data"}
                           :subjects [{:name "gcr.io/project/image:v1"
                                       :digest {:sha256 "abc123"}}]}]])

      (is (not (nil? @changes)) "Callback should fire")
      (is (= 1 (count (:added @changes))) "Should have one added result")

      (let [result (first (:added @changes))]
        ;; Should have xt/id, predicate_type, and subjects, but NOT predicate
        (is (= "slsa-provenance-v1" (:predicate_type result)) "Should have predicate_type")
        (is (vector? (:subjects result)) "Should have subjects")
        (is (nil? (:predicate result)) "Should NOT have predicate field (removed)")
        (is (= 3 (count (keys result))) "Should have 3 fields: xt/id, predicate_type, subjects"))

      (diff/unregister-query! "test-without"))))

(deftest test-without-with-with-callback
  (testing "WITHOUT operator combined with WITH for field transformation"
    (let [changes (atom nil)]
      (diff/register-query!
       {:query-id "test-without-with"
        :xtql "(-> (from :prod_attestations [predicate_type predicate subjects])
                   (with {:builder_id (.. predicate :builder :id)})
                   (without predicate subjects))"
        :callback (fn [c] (reset! changes c))})

      ;; Insert attestation
      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "test-without-with-1"
                           :predicate_type "slsa-provenance-v1"
                           :predicate {:buildType "https://github.com/actions"
                                       :builder {:id "https://github.com/actions/runner"}}
                           :subjects [{:name "gcr.io/project/image:v1"}]}]])

      (is (not (nil? @changes)) "Callback should fire")
      (is (= 1 (count (:added @changes))))

      (let [result (first (:added @changes))]
        ;; WITH extracts builder_id, WITHOUT removes original predicate and subjects
        ;; FROM includes xt/id by default
        (is (= "slsa-provenance-v1" (:predicate_type result)))
        (is (= "https://github.com/actions/runner" (:builder_id result)))
        (is (nil? (:predicate result)) "predicate should be removed")
        (is (nil? (:subjects result)) "subjects should be removed")
        (is (= 3 (count (keys result))) "Should have 3 fields: xt/id, predicate_type, and builder_id"))

      (diff/unregister-query! "test-without-with"))))

(deftest test-offset-operator-callback
  (testing "OFFSET operator skips first N results (non-deterministic without ORDER BY)"
    (let [changes (atom [])]
      (diff/register-query!
       {:query-id "test-offset"
        :xtql "(-> (from :prod_attestations [predicate_type])
                   (aggregate predicate_type {:count (row-count)})
                   (offset 1)
                   (limit 2))"
        :callback (fn [c] (swap! changes conj c))})

      ;; Insert first doc
      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "test-offset-1"
                           :predicate_type "slsa-v1"}]])

      ;; No callback yet (first result is skipped by offset)
      (is (or (empty? @changes)
              (empty? (:added (first @changes))))
          "First aggregate result should be skipped by offset")

      ;; Insert second doc of different type
      (diff/execute-tx! *xtdb-client*
                        [[:put-docs :prod_attestations
                          {:xt/id "test-offset-2"
                           :predicate_type "cyclonedx-v1"}]])

      ;; Should see callback for second group (after offset)
      (is (pos? (count @changes)) "Should have callbacks after offset")

      (diff/unregister-query! "test-offset"))))

(deftest test-rel-operator-inline-data
  (testing "REL operator works with inline data as source"
    ;; Note: This test verifies REL at the operator level since our query
    ;; system currently expects queries to start with FROM for table tracking.
    ;; REL would be used in contexts where inline data is needed.
    (let [rel-op (ops/create-rel-operator
                  [{:name "Alice" :age 30}
                   {:name "Bob" :age 25}
                   {:name "Charlie" :age 35}])
          where-op (ops/create-where-operator [:>= :age 30])
          aggregate-op (ops/create-aggregate-operator {:count [:row-count]})
          ;; Get initial deltas from REL
          initial-deltas (ops/rel-initial-deltas rel-op)
          ;; Process through WHERE
          where-results (mapcat #(ops/process-delta where-op %) initial-deltas)
          ;; Process through AGGREGATE
          agg-results (mapcat #(ops/process-delta aggregate-op %) where-results)]
      (is (= 3 (count initial-deltas)) "REL should emit 3 tuples")
      (is (= 2 (count where-results)) "WHERE should filter to 2 results (Alice, Charlie)")
      (is (pos? (count agg-results)) "Should have aggregate results"))))

(deftest test-join-operator-multi-input
  (testing "JOIN operator works with multi-input graph"
    ;; Create two REL sources and a JOIN operator
    (let [users-rel (ops/create-rel-operator
                     [{:user-id 1 :name "Alice"}
                      {:user-id 2 :name "Bob"}
                      {:user-id 3 :name "Charlie"}])
          emails-rel (ops/create-rel-operator
                      [{:user-id 1 :email "alice@example.com"}
                       {:user-id 2 :email "bob@example.com"}
                       {:user-id 99 :email "orphan@example.com"}])
          join-op (ops/create-join-operator :user-id :user-id)

          ;; Create graph with multi-input edges
          ;; 0: users-rel -> JOIN (left input)
          ;; 1: emails-rel -> JOIN (right input)
          ;; 2: join-op (output)
          operators {0 users-rel
                     1 emails-rel
                     2 join-op}
          edges {0 [{:to 2 :input-port :left}]   ; users to JOIN left
                 1 [{:to 2 :input-port :right}]}  ; emails to JOIN right
          graph (df/create-operator-graph operators edges)
          ;; Execute graph with empty input (REL operators generate their own data)
          results (df/propagate-deltas graph [])
          ;; Get output from JOIN operator (id 2)
          join-results (get results 2)
          ;; Verify joined documents contain fields from both sides
          result-docs (set (map :doc join-results))]
      (is (= 2 (count join-results)) "Should have 2 join results (Alice, Bob)")
      (is (contains? result-docs {:user-id 1 :name "Alice" :email "alice@example.com"}))
      (is (contains? result-docs {:user-id 2 :name "Bob" :email "bob@example.com"}))
      ;; Charlie (user-id 3) has no email, should not appear (inner join)
      ;; Orphan email (user-id 99) has no user, should not appear
      (is (not-any? #(= 3 (:user-id %)) result-docs) "Charlie should not appear")
      (is (not-any? #(= 99 (:user-id %)) result-docs) "Orphan should not appear"))))

(deftest test-left-join-operator-multi-input
  (testing "LEFT JOIN operator works with multi-input graph"
    ;; Create two REL sources and a LEFT JOIN operator
    (let [users-rel (ops/create-rel-operator
                     [{:user-id 1 :name "Alice"}
                      {:user-id 2 :name "Bob"}
                      {:user-id 3 :name "Charlie"}])
          emails-rel (ops/create-rel-operator
                      [{:user-id 1 :email "alice@example.com"}
                       {:user-id 2 :email "bob@example.com"}])
          left-join-op (ops/create-left-join-operator :user-id :user-id)
          operators {0 users-rel
                     1 emails-rel
                     2 left-join-op}
          edges {0 [{:to 2 :input-port :left}]
                 1 [{:to 2 :input-port :right}]}
          graph (df/create-operator-graph operators edges)
          ;; Execute graph with empty input (REL operators generate their own data)
          results (df/propagate-deltas graph [])
          join-results (get results 2)
          ;; Accumulate final state by applying deltas (mult +1 adds, -1 removes)
          final-docs (reduce (fn [acc delta]
                               (let [doc (:doc delta)
                                     mult (:mult delta)]
                                 (if (pos? mult)
                                   (conj acc doc)
                                   (disj acc doc))))
                             #{}
                             join-results)]
      (is (= 3 (count final-docs)) "Should have 3 final results (all left rows)")
      (is (contains? final-docs {:user-id 1 :name "Alice" :email "alice@example.com"}))
      (is (contains? final-docs {:user-id 2 :name "Bob" :email "bob@example.com"}))
      ;; Charlie has no email, should appear as left-only
      (is (contains? final-docs {:user-id 3 :name "Charlie"})))))

(deftest test-comprehensive-multi-operator-workflow
  (testing "Comprehensive workflow covering most operators with realistic data"
    ;; Scenario: Join user data with order data, aggregate, filter, and project
    (let [;; Source 1: Users
          users-rel (ops/create-rel-operator
                     [{:user-id 1 :name "Alice" :tier "premium" :score 95}
                      {:user-id 2 :name "Bob" :tier "free" :score 75}
                      {:user-id 3 :name "Charlie" :tier "premium" :score 88}
                      {:user-id 4 :name "David" :tier "free" :score 62}
                      {:user-id 5 :name "Eve" :tier "basic" :score 80}])

          ;; Source 2: Orders
          orders-rel (ops/create-rel-operator
                      [{:user-id 1 :order-id "ord-1" :amount 100.0}
                       {:user-id 1 :order-id "ord-2" :amount 150.0}
                       {:user-id 2 :order-id "ord-3" :amount 50.0}
                       {:user-id 3 :order-id "ord-4" :amount 200.0}
                       {:user-id 5 :order-id "ord-5" :amount 75.0}
                       {:user-id 5 :order-id "ord-6" :amount 125.0}])

          ;; Build pipeline:
          ;; 1. INNER JOIN users with orders
          ;; 2. Filter to users with score >= 80
          ;; 3. WITHOUT removes order-id to simplify
          ;; 4. Aggregate by tier
          ;; 5. Project specific fields with RETURN

          join-op (ops/create-join-operator :user-id :user-id)
          where-op (ops/create-where-operator [:>= :score 80])
          without-op (ops/create-without-operator [:order-id :name])
          aggregate-op (ops/create-aggregate-operator :tier
                                                      {:order-count [:row-count]
                                                       :total-amount [:sum :amount]
                                                       :avg-score [:avg :score]})
          return-op (ops/create-return-operator [:tier :order-count :total-amount :avg-score])

          operators {0 users-rel
                     1 orders-rel
                     2 join-op
                     3 where-op
                     4 without-op
                     5 aggregate-op
                     6 return-op}

          edges {0 [{:to 2 :input-port :left}]
                 1 [{:to 2 :input-port :right}]
                 2 [3]
                 3 [4]
                 4 [5]
                 5 [6]}

          graph (df/create-operator-graph operators edges)
          results (df/propagate-deltas graph [])
          final-results (get results 6)
          final-state (reduce (fn [acc delta]
                                (let [doc (:doc delta)
                                      mult (:mult delta)]
                                  (if (pos? mult)
                                    (conj acc doc)
                                    (disj acc doc))))
                              #{}
                              final-results)]

      ;; Verify results
      (is (= 2 (count final-state)) "Should have 2 tiers (premium and basic)")

      ;; Check premium tier (Alice: 2 orders, Charlie: 1 order = 3 total)
      (let [premium (first (filter #(= "premium" (:tier %)) final-state))]
        (is premium "Should have premium tier")
        (is (= 3 (:order-count premium)) "Should have 3 premium orders")
        (is (= 450.0 (:total-amount premium)) "Total: 100 + 150 + 200 = 450"))

      ;; Check basic tier (Eve: 2 orders)
      (let [basic (first (filter #(= "basic" (:tier %)) final-state))]
        (is basic "Should have basic tier")
        (is (= 2 (:order-count basic)) "Should have 2 basic orders")
        (is (= 200.0 (:total-amount basic)) "Total: 75 + 125 = 200"))

      ;; Verify RETURN projection worked
      (doseq [result final-state]
        (is (= 4 (count (keys result))) "Should have exactly 4 fields")
        (is (contains? result :tier))
        (is (contains? result :order-count))
        (is (contains? result :total-amount))
        (is (contains? result :avg-score))))))
(deftest test-unify-operator-multi-input
  (testing "UNIFY operator works with multi-input graph"
    ;; Scenario: Multi-field unification of users and permissions
    (let [users-rel (ops/create-rel-operator
                     [{:user-id 1 :org "acme" :name "Alice"}
                      {:user-id 2 :org "acme" :name "Bob"}
                      {:user-id 3 :org "other" :name "Charlie"}])
          perms-rel (ops/create-rel-operator
                     [{:user-id 1 :org "acme" :role "admin"}
                      {:user-id 2 :org "acme" :role "user"}
                      {:user-id 3 :org "acme" :role "guest"}    ; wrong org
                      {:user-id 99 :org "acme" :role "orphan"}]) ; no user
          ;; Unify on both user-id AND org (both must match)
          unify-op (ops/create-unify-operator [[:user-id :user-id] [:org :org]])
          operators {0 users-rel
                     1 perms-rel
                     2 unify-op}
          edges {0 [{:to 2 :input-port :left}]
                 1 [{:to 2 :input-port :right}]}
          graph (df/create-operator-graph operators edges)
          results (df/propagate-deltas graph [])
          unify-results (get results 2)
          final-docs (set (map :doc unify-results))]
      ;; Only Alice and Bob should match (both user-id and org match)
      (is (= 2 (count final-docs)) "Should have 2 unified results")
      (is (contains? final-docs {:user-id 1 :org "acme" :name "Alice" :role "admin"}))
      (is (contains? final-docs {:user-id 2 :org "acme" :name "Bob" :role "user"}))
      ;; Charlie shouldn't match (org differs: other vs acme)
      (is (not-any? #(= "Charlie" (:name %)) final-docs) "Charlie should not appear")
      ;; Orphan shouldn't match (no matching user-id)
      (is (not-any? #(= 99 (:user-id %)) final-docs) "Orphan should not appear"))))
