(ns xtflow.performance-test
  "Performance benchmarks comparing differential dataflow vs naive re-querying.

  This test suite demonstrates the performance advantages of XTFlow's differential
  dataflow approach over naive full re-computation after each transaction."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [clj-test-containers.core :as tc]
            [xtflow.core :as diff]
            [xtdb.api :as xt]
            [clojure.string :as str]))

;;;; Test Data Generators

(defn gen-uuid
  "Generate random UUID string."
  []
  (str (java.util.UUID/randomUUID)))

(defn weighted-pick
  "Pick item from collection based on weights."
  [items weights]
  (let [total (reduce + weights)
        r (rand total)]
    (loop [items items
           ws weights
           sum 0]
      (if (< r (+ sum (first ws)))
        (first items)
        (recur (rest items) (rest ws) (+ sum (first ws)))))))

(defn gen-sbom-component
  "Generate realistic SBOM component with licenses.

  License distribution matches real-world usage:
  - MIT: 30%
  - Apache-2.0: 25%
  - GPL-3.0: 15%
  - Others: 30% (distributed across 5 licenses)"
  [idx]
  (let [licenses ["MIT" "Apache-2.0" "GPL-3.0" "BSD-3-Clause"
                  "ISC" "MPL-2.0" "LGPL-3.0" "EPL-2.0"]
        weights [30 25 15 10 8 5 4 3]
        license-id (weighted-pick licenses weights)]
    {:type (rand-nth ["library" "application" "framework"])
     :name (str "github.com/org" (mod idx 1000) "/pkg" idx)
     :version (str "v" (rand-int 5) "." (rand-int 20) "." (rand-int 100))
     :licenses [{:license {:id license-id
                           :name license-id
                           :url (str "https://spdx.org/licenses/" license-id)}}]}))

(defn gen-sbom
  "Generate complete CycloneDX SBOM with N components."
  [id n-components]
  {:xt/id id
   :predicate_type "cyclonedx-sbom-v1.4"
   :predicate {:bomFormat "CycloneDX"
               :specVersion "1.4"
               :serialNumber (gen-uuid)
               :components (vec (repeatedly n-components
                                            #(gen-sbom-component (rand-int 100000))))}
   :subjects [{:name (str "gcr.io/project/image-" id)
               :digest {:sha256 (gen-uuid)}}]})

(defn gen-user
  "Generate user record with tier and region."
  [id]
  {:xt/id (str "user-" id)
   :user-id id
   :name (str "User-" id)
   :tier (rand-nth ["free" "basic" "premium"])
   :region (rand-nth ["us-east" "us-west" "eu-west" "ap-south"])
   :score (+ 50 (rand-int 51))})

(defn gen-order
  "Generate order record for a user."
  [id user-id]
  {:xt/id (str "order-" id)
   :order-id id
   :user-id user-id
   :amount (* 10.0 (inc (rand-int 100)))
   :status (rand-nth ["pending" "completed" "cancelled"])
   :timestamp (System/currentTimeMillis)})

(defn gen-permission
  "Generate permission record for UNIFY tests."
  [user-id org]
  {:xt/id (str "perm-" user-id "-" org)
   :user-id user-id
   :org org
   :role (rand-nth ["admin" "user" "viewer" "editor"])
   :scope (rand-nth ["read" "write" "delete" "all"])})

(defn gen-bulk-sboms
  "Generate N SBOMs for bulk insert."
  [n start-idx]
  (vec (for [i (range n)]
         (gen-sbom (str "sbom-" (+ start-idx i))
                   (+ 20 (rand-int 80))))))  ; 20-100 components each

(defn gen-bulk-users
  "Generate N users."
  [n]
  (vec (for [i (range n)]
         (gen-user i))))

(defn gen-bulk-orders
  "Generate N orders with power-law user distribution (20% users get 80% orders)."
  [n start-idx n-users]
  (let [;; 20% of users are "popular" and get 80% of orders
        popular-users (take (quot n-users 5) (range n-users))
        all-users (range n-users)
        pick-user (fn []
                    (if (< (rand) 0.8)
                      (rand-nth popular-users)
                      (rand-nth all-users)))]
    (vec (for [i (range n)]
           (gen-order (+ start-idx i) (pick-user))))))

(defn gen-bulk-permissions
  "Generate N permissions for UNIFY tests."
  [n-users n-orgs]
  (let [orgs (vec (for [i (range n-orgs)] (str "org-" i)))]
    (vec (for [user-id (range n-users)
               :let [org (rand-nth orgs)]]
           (gen-permission user-id org)))))

(defn gen-simple-attestation
  "Generate simple attestation for projection tests."
  [id]
  {:xt/id (str "att-" id)
   :predicate_type "in-toto-v0.1"
   :predicate {:builder {:id (str "https://github.com/org" (mod id 100))}
               :buildType "https://tekton.dev/v1"
               :invocation {:configSource {:uri "https://example.com"}}}
   :subjects [{:name (str "image-" id)
               :digest {:sha256 (gen-uuid)}}]
   :timestamp (System/currentTimeMillis)})

(defn gen-bulk-attestations
  "Generate N simple attestations."
  [n start-idx]
  (vec (for [i (range n)]
         (gen-simple-attestation (+ start-idx i)))))

;;;; Timing Utilities

(defn time-block
  "Time execution of a function, return [result elapsed-ms]."
  [f]
  (let [start (System/nanoTime)
        result (f)
        end (System/nanoTime)
        elapsed-ms (/ (- end start) 1000000.0)]
    [result elapsed-ms]))

(defn timing-stats
  "Compute statistics from timing vector."
  [timings]
  (when (seq timings)
    (let [sorted (sort timings)
          n (count sorted)
          p50-idx (quot n 2)
          p95-idx (quot (* n 95) 100)
          p99-idx (quot (* n 99) 100)]
      {:count n
       :min (first sorted)
       :max (last sorted)
       :mean (/ (reduce + sorted) (double n))
       :median (nth sorted p50-idx)
       :p95 (nth sorted (min p95-idx (dec n)))
       :p99 (nth sorted (min p99-idx (dec n)))
       :total (reduce + sorted)})))

(defmacro with-timing
  "Execute body and return [result elapsed-ms]."
  [& body]
  `(time-block (fn [] ~@body)))

;;;; Naive Implementation Framework

(defn naive-fetch-all
  "Fetch all documents from a table in XTDB using SQL."
  [xtdb-client table]
  (let [sql (str "SELECT * FROM " (name table))
        results (xt/q xtdb-client sql)]
    results))

(defn naive-aggregate-license-counts
  "Naive implementation: Query all SBOMs, extract all components, count by license.

  This simulates re-running the full query on every transaction."
  [xtdb-client]
  (let [;; Fetch ALL SBOMs
        all-sboms (naive-fetch-all xtdb-client :prod_attestations)

        ;; Filter to CycloneDX SBOMs
        cyclonedx-sboms (filter #(and (:predicate_type %)
                                      (str/includes? (:predicate_type %) "cyclonedx"))
                                all-sboms)

        ;; Unnest all components
        all-components (mapcat #(get-in % [:predicate :components]) cyclonedx-sboms)

        ;; Extract license IDs
        license-ids (keep #(get-in % [:licenses 0 :license :id]) all-components)

        ;; Count by license
        license-counts (frequencies license-ids)

        ;; Format as results
        results (vec (for [[lic-id count] license-counts]
                       {:license_id lic-id :count count}))]
    results))

(defn naive-join-users-orders
  "Naive implementation: Fetch all users and orders, perform nested loop join, aggregate by tier."
  [xtdb-client]
  (let [;; Fetch ALL users and orders
        all-users (naive-fetch-all xtdb-client :users)
        all-orders (naive-fetch-all xtdb-client :orders)

        ;; Perform nested loop join (O(n*m))
        joined (for [user all-users
                     order all-orders
                     :when (= (:user-id user) (:user-id order))]
                 (merge user order))

        ;; Filter to orders > 100
        filtered (filter #(> (:amount %) 100) joined)

        ;; Group by tier and aggregate
        by-tier (group-by :tier filtered)
        results (vec (for [[tier orders] by-tier]
                       {:tier tier
                        :count (count orders)
                        :total (reduce + (map :amount orders))}))]
    results))

(defn naive-filter-components
  "Naive implementation: Filter library components with github.com prefix."
  [xtdb-client]
  (let [;; Fetch ALL SBOMs
        all-sboms (naive-fetch-all xtdb-client :prod_attestations)

        ;; Unnest components
        components (mapcat #(get-in % [:predicate :components]) all-sboms)

        ;; Filter: library type AND github.com name prefix
        filtered (filter (fn [comp]
                           (and (= (:type comp) "library")
                                (str/starts-with? (:name comp) "github.com")))
                         components)

        ;; Project fields
        results (vec (for [comp filtered]
                       {:name (:name comp)
                        :type (:type comp)}))]
    results))

(defn naive-projection
  "Naive implementation: Fetch all, project fields."
  [xtdb-client table fields]
  (let [all-docs (naive-fetch-all xtdb-client table)
        results (vec (map #(select-keys % fields) all-docs))]
    results))

(defn naive-unify-multifield
  "Naive implementation: UNIFY on user-id AND org with aggregation.
  Performs cartesian product, filters by matching fields."
  [xtdb-client]
  (let [;; Fetch ALL users and permissions
        all-users (naive-fetch-all xtdb-client :users)
        all-perms (naive-fetch-all xtdb-client :permissions)

        ;; Unify: cartesian product filtered by user-id AND org match
        unified (for [user all-users
                      perm all-perms
                      :when (and (= (:user-id user) (:user-id perm))
                                 (= (:org user) (:org perm)))]
                  (merge user perm))

        ;; Aggregate by org
        by-org (group-by :org unified)
        results (vec (for [[org records] by-org]
                       {:org org
                        :user_count (count (distinct (map :user-id records)))
                        :total_perms (count records)}))]
    results))

;;;; Test Fixture

(def ^:dynamic *xtdb-client* nil)

(defn clear-table!
  "Clear all documents from a table."
  [xtdb-client table]
  (try
    (xt/execute-tx xtdb-client
                   [[:sql (str "DELETE FROM " (name table))]])
    (catch Exception _
      ;; Table might not exist yet, that's okay
      nil)))

(def ^:dynamic *xtdb-container* nil)

(defn xtdb-container-fixture
  "Start XTDB container with increased heap for performance testing.
   Uses test containers on a non-standard port to avoid conflicts.
   Container runs once for entire test namespace with 16GB heap."
  [f]
  (let [container (-> (tc/create
                       {:image-name "ghcr.io/xtdb/xtdb:2.1.0"
                        :exposed-ports [5432 8080]
                        :env-vars {"JAVA_OPTS" "-Xmx16g -Xms16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"}
                        :wait-for {:strategy :log
                                   :message "Node started"}})
                      (tc/start!))
        host (:host container)
        port (get (:mapped-ports container) 5432)
        client (xt/client {:host host :port port :user "xtdb"})]

    (println (format "\n🐳 XTDB test container started on %s:%d with 16GB heap" host port))

    (binding [*xtdb-container* container
              *xtdb-client* client]
      (try
        (f)
        (finally
          (tc/stop! container)
          (println "\n🐳 XTDB test container stopped"))))))

(defn clear-tables-fixture
  "Clear all tables and queries between individual tests."
  [f]
  ;; Clear all queries
  (diff/reset-all-queries!)

  ;; Clear all tables
  (clear-table! *xtdb-client* :prod_attestations)
  (clear-table! *xtdb-client* :users)
  (clear-table! *xtdb-client* :orders)
  (clear-table! *xtdb-client* :permissions)

  ;; Run test
  (f))

(use-fixtures :once xtdb-container-fixture)
(use-fixtures :each clear-tables-fixture)

;;;; Output Formatting

(defn format-time
  "Format milliseconds for display."
  [ms]
  (cond
    (< ms 1) (format "%.2f µs" (* ms 1000))
    (< ms 1000) (format "%.2f ms" ms)
    :else (format "%.2f s" (/ ms 1000))))

(defn format-speedup
  "Format speedup factor."
  [speedup]
  (format "%.1fx" speedup))

(defn benefit-level
  "Determine benefit level from speedup."
  [speedup]
  (cond
    (>= speedup 100) "EXTREME"
    (>= speedup 30) "HIGH"
    (>= speedup 15) "MEDIUM"
    :else "LOW"))

(defn print-separator
  "Print table separator line."
  [widths]
  (println (str "+" (str/join "+" (map #(apply str (repeat (+ % 2) "-")) widths)) "+")))

(defn print-row
  "Print table row."
  [cols widths]
  (println (str "| "
                (str/join " | "
                          (map #(format (str "%-" %2 "s") (str %1)) cols widths))
                " |")))

(defn print-benchmark-results
  "Print comprehensive benchmark results table."
  [results]
  (println)
  (println (str/join "" (repeat 80 "=")))
  (println " XTFLOW PERFORMANCE BENCHMARK RESULTS")
  (println (str/join "" (repeat 80 "=")))
  (println)

  ;; Table
  (let [widths [35 12 12 10 10]
        headers ["Benchmark" "Diff (ms)" "Naive (ms)" "Speedup" "Benefit"]]

    (print-separator widths)
    (print-row headers widths)
    (print-separator widths)

    (doseq [{:keys [name diff-mean naive-mean speedup]} results]
      (print-row [name
                  (format-time diff-mean)
                  (format-time naive-mean)
                  (format-speedup speedup)
                  (benefit-level speedup)]
                 widths))

    (print-separator widths))

  ;; Summary
  (let [total-diff (reduce + (map :diff-mean results))
        total-naive (reduce + (map :naive-mean results))
        overall-speedup (/ total-naive total-diff)]
    (println)
    (println "OVERALL SUMMARY:")
    (println (format "  Total Differential Time: %s" (format-time total-diff)))
    (println (format "  Total Naive Time: %s" (format-time total-naive)))
    (println (format "  Overall Speedup: %s" (format-speedup overall-speedup)))
    (println)))

;;;; Benchmark Scenarios

(deftest ^:benchmark benchmark-aggregate-license-counting
  (testing "AGGREGATE: License counting with 50K SBOMs"
    (let [xtdb-client *xtdb-client*
          diff-timings (atom [])
          naive-timings (atom [])

          ;; Register differential query
          _ (diff/register-query!
             {:query-id "bench-agg-licenses"
              :xtql "(-> (from :prod_attestations [predicate_type predicate])
                         (where (like predicate_type \"%cyclonedx%\"))
                         (unnest {:comp (.. predicate :components)})
                         (with {:license_id (.. comp :licenses 0 :license :id)})
                         (where :license_id)
                         (aggregate license_id {:count (row-count)}))"
              :callback (fn [_changes])})

          ;; Initial load (reduced from 10K to 1K to avoid OOM)
          initial-sboms (gen-bulk-sboms 1000 0)
          _ (xt/execute-tx xtdb-client [(into [:put-docs :prod_attestations] initial-sboms)])
          _ (diff/execute-tx! xtdb-client [(into [:put-docs :prod_attestations] initial-sboms)])

          ;; Warmup: 5 transactions (reduced batch size)
          _ (dotimes [i 5]
              (let [batch (gen-bulk-sboms 20 (* 1000000 (inc i)))]
                (xt/execute-tx xtdb-client [(into [:put-docs :prod_attestations] batch)])
                (diff/execute-tx! xtdb-client [(into [:put-docs :prod_attestations] batch)])
                (naive-aggregate-license-counts xtdb-client)))

          ;; Timed incremental transactions (reduced from 100 to 20)
          n-transactions 20
          _ (dotimes [tx-idx n-transactions]
              (let [batch-size (+ 10 (rand-int 40))  ; 10-50 SBOMs per batch
                    new-sboms (gen-bulk-sboms batch-size (* 10000 (+ 10 tx-idx)))

                    ;; Execute in XTDB first
                    _ (xt/execute-tx xtdb-client [(into [:put-docs :prod_attestations] new-sboms)])

                    ;; Time differential
                    [_ diff-ms] (with-timing
                                  (diff/execute-tx! xtdb-client [(into [:put-docs :prod_attestations] new-sboms)]))

                    ;; Time naive (re-query everything)
                    [_ naive-ms] (with-timing
                                   (naive-aggregate-license-counts xtdb-client))]

                (swap! diff-timings conj diff-ms)
                (swap! naive-timings conj naive-ms)))

          ;; Compute statistics
          diff-stats (timing-stats @diff-timings)
          naive-stats (timing-stats @naive-timings)
          speedup (/ (:mean naive-stats) (:mean diff-stats))]

      ;; Assertions
      (is (>= speedup 20) (str "Expected >20x speedup, got " (format "%.1fx" speedup)))
      (is (> (count @diff-timings) 0) "Should have timing data"))))

;;; Scenario 2: JOIN - Users × Orders

(deftest ^:benchmark benchmark-join-users-orders
  (testing "JOIN: Users × Orders with aggregation by tier"
    (let [diff-timings (atom [])
          naive-timings (atom [])

          ;; Register differential query: JOIN users with orders, aggregate by tier
          _ (diff/register-query!
             {:query-id "bench-join-users-orders"
              :xtql "(-> (from :users [xt/id user-id tier region score])
                         (join (from :orders [order-id user-id amount status timestamp])
                               {:user-id user-id})
                         (aggregate tier {:order_count (row-count)
                                          :total_amount (sum amount)}))"
              :callback (fn [_changes])})

          ;; Initial load: 10K users, 10K orders
          initial-users (gen-bulk-users 10000)
          initial-orders (gen-bulk-orders 10000 0 10000)
          _ (xt/execute-tx *xtdb-client* [(into [:put-docs :users] initial-users)
                                          (into [:put-docs :orders] initial-orders)])
          _ (diff/execute-tx! *xtdb-client* [(into [:put-docs :users] initial-users)
                                             (into [:put-docs :orders] initial-orders)])

          ;; Warmup: 5 transactions
          _ (dotimes [i 5]
              (let [warmup-orders (gen-bulk-orders 100 (+ 20000 (* i 100)) 10000)]
                (xt/execute-tx *xtdb-client* [(into [:put-docs :orders] warmup-orders)])
                (diff/execute-tx! *xtdb-client* [(into [:put-docs :orders] warmup-orders)])
                (naive-join-users-orders *xtdb-client*)))

          ;; Timed incremental transactions: 100 batches of 500 orders each
          _ (dotimes [tx-idx 100]
              (let [batch-size 500
                    start-id (+ 25000 (* tx-idx batch-size))
                    new-orders (gen-bulk-orders batch-size start-id 10000)

                    ;; Commit to XTDB
                    _ (xt/execute-tx *xtdb-client* [(into [:put-docs :orders] new-orders)])

                    ;; Time differential approach
                    [_ diff-ms] (with-timing
                                  (diff/execute-tx! *xtdb-client* [(into [:put-docs :orders] new-orders)]))

                    ;; Time naive approach
                    [_ naive-ms] (with-timing
                                   (naive-join-users-orders *xtdb-client*))]

                (swap! diff-timings conj diff-ms)
                (swap! naive-timings conj naive-ms)))

          ;; Compute statistics
          diff-stats (timing-stats @diff-timings)
          naive-stats (timing-stats @naive-timings)
          speedup (/ (:mean naive-stats) (:mean diff-stats))]

      ;; Assertions: expect >20x speedup for JOIN
      (is (>= speedup 20)
          (str "Expected >20x speedup for JOIN benchmark, got " speedup "x"))

      ;; Print detailed results
      (println "\n" (str/join "" (repeat 80 "=")))
      (println " BENCHMARK RESULT: JOIN - Users × Orders")
      (println (str/join "" (repeat 80 "=")))
      (println (format "Data: 10K users, 75K orders (after all transactions)"))
      (println (format "Transactions: 100 incremental (500 orders each)"))
      (println)
      (println "Differential Approach:")
      (println (format "  Mean: %s | Median: %s | P95: %s"
                       (format-time (:mean diff-stats))
                       (format-time (:median diff-stats))
                       (format-time (:p95 diff-stats))))
      (println)
      (println "Naive Approach (full re-query):")
      (println (format "  Mean: %s | Median: %s | P95: %s"
                       (format-time (:mean naive-stats))
                       (format-time (:median naive-stats))
                       (format-time (:p95 naive-stats))))
      (println)
      (println (format "Speedup: %s (%s benefit)"
                       (format-speedup speedup)
                       (benefit-level speedup))))))

;;; Scenario 3: UNIFY - Multi-field Matching

(deftest ^:benchmark benchmark-unify-multifield
  (testing "UNIFY: Multi-field matching on user-id AND org"
    (let [diff-timings (atom [])
          naive-timings (atom [])

          ;; Register differential query: UNIFY on user-id AND org
          _ (diff/register-query!
             {:query-id "bench-unify-multifield"
              :xtql "(-> (unify (from :users [user-id org tier])
                                (from :permissions [user-id org role])
                                {:user-id user-id :org org})
                         (aggregate org {:user_count (count-distinct user-id)
                                         :total_perms (row-count)}))"
              :callback (fn [_changes])})

          ;; Initial load: 5K users, 5K permissions, 100 orgs
          n-orgs 100
          initial-users (mapv #(assoc (gen-user %) :org (str "org" (mod % n-orgs))) (range 5000))
          initial-perms (mapv #(gen-permission (mod % 5000) (str "org" (mod % n-orgs))) (range 5000))
          _ (xt/execute-tx *xtdb-client* [(into [:put-docs :users] initial-users)
                                          (into [:put-docs :permissions] initial-perms)])
          _ (diff/execute-tx! *xtdb-client* [(into [:put-docs :users] initial-users)
                                             (into [:put-docs :permissions] initial-perms)])

          ;; Warmup: 5 transactions
          _ (dotimes [i 5]
              (let [warmup-users (mapv #(assoc (gen-user (+ 10000 (* i 50) %))
                                               :org (str "org" (mod % n-orgs)))
                                       (range 50))
                    warmup-perms (mapv #(gen-permission (+ 10000 (* i 50) %)
                                                        (str "org" (mod % n-orgs)))
                                       (range 50))]
                (xt/execute-tx *xtdb-client* [(into [:put-docs :users] warmup-users)
                                              (into [:put-docs :permissions] warmup-perms)])
                (diff/execute-tx! *xtdb-client* [(into [:put-docs :users] warmup-users)
                                                 (into [:put-docs :permissions] warmup-perms)])
                (naive-unify-multifield *xtdb-client*)))

          ;; Timed incremental transactions: 100 batches of 150 users + 150 perms each
          _ (dotimes [tx-idx 100]
              (let [batch-size 150
                    start-id (+ 11000 (* tx-idx batch-size))
                    new-users (mapv #(assoc (gen-user (+ start-id %))
                                            :org (str "org" (mod % n-orgs)))
                                    (range batch-size))
                    new-perms (mapv #(gen-permission (+ start-id %)
                                                     (str "org" (mod % n-orgs)))
                                    (range batch-size))

                    ;; Commit to XTDB
                    _ (xt/execute-tx *xtdb-client* [(into [:put-docs :users] new-users)
                                                    (into [:put-docs :permissions] new-perms)])

                    ;; Time differential approach
                    [_ diff-ms] (with-timing
                                  (diff/execute-tx! *xtdb-client* [(into [:put-docs :users] new-users)
                                                                   (into [:put-docs :permissions] new-perms)]))

                    ;; Time naive approach
                    [_ naive-ms] (with-timing
                                   (naive-unify-multifield *xtdb-client*))]

                (swap! diff-timings conj diff-ms)
                (swap! naive-timings conj naive-ms)))

          ;; Compute statistics
          diff-stats (timing-stats @diff-timings)
          naive-stats (timing-stats @naive-timings)
          speedup (/ (:mean naive-stats) (:mean diff-stats))]

      ;; Assertions: expect >30x speedup for UNIFY
      (is (>= speedup 30)
          (str "Expected >30x speedup for UNIFY benchmark, got " speedup "x"))

      ;; Print detailed results
      (println "\n" (str/join "" (repeat 80 "=")))
      (println " BENCHMARK RESULT: UNIFY - Multi-field Matching")
      (println (str/join "" (repeat 80 "=")))
      (println (format "Data: 20K users, 20K permissions, 100 orgs (after all transactions)"))
      (println (format "Transactions: 100 incremental (150 users + 150 perms each)"))
      (println)
      (println "Differential Approach:")
      (println (format "  Mean: %s | Median: %s | P95: %s"
                       (format-time (:mean diff-stats))
                       (format-time (:median diff-stats))
                       (format-time (:p95 diff-stats))))
      (println)
      (println "Naive Approach (full re-query):")
      (println (format "  Mean: %s | Median: %s | P95: %s"
                       (format-time (:mean naive-stats))
                       (format-time (:median naive-stats))
                       (format-time (:p95 naive-stats))))
      (println)
      (println (format "Speedup: %s (%s benefit)"
                       (format-speedup speedup)
                       (benefit-level speedup))))))

;;; Scenario 4: WHERE + UNNEST - Deep Filtering

(deftest ^:benchmark benchmark-where-unnest-filtering
  (testing "WHERE + UNNEST: Deep filtering of library components"
    (let [diff-timings (atom [])
          naive-timings (atom [])

          ;; Register differential query: Filter library components with github.com prefix
          _ (diff/register-query!
             {:query-id "bench-where-unnest"
              :xtql "(-> (from :prod_attestations [xt/id predicate_type predicate])
                         (where (like predicate_type \"%cyclonedx%\"))
                         (unnest {:comp (.. predicate :components)})
                         (where (= (.. comp :type) \"library\"))
                         (where (like (.. comp :name) \"github.com%\"))
                         (with {:name (.. comp :name)
                                :type (.. comp :type)}))"
              :callback (fn [_changes])})

          ;; Initial load: 10K SBOMs
          initial-sboms (gen-bulk-sboms 10000 0)
          _ (xt/execute-tx *xtdb-client* [(into [:put-docs :prod_attestations] initial-sboms)])
          _ (diff/execute-tx! *xtdb-client* [(into [:put-docs :prod_attestations] initial-sboms)])

          ;; Warmup: 5 transactions
          _ (dotimes [i 5]
              (let [warmup-sboms (gen-bulk-sboms 100 (+ 20000 (* i 100)))]
                (xt/execute-tx *xtdb-client* [(into [:put-docs :prod_attestations] warmup-sboms)])
                (diff/execute-tx! *xtdb-client* [(into [:put-docs :prod_attestations] warmup-sboms)])
                (naive-filter-components *xtdb-client*)))

          ;; Timed incremental transactions: 100 batches of 200 SBOMs each
          _ (dotimes [tx-idx 100]
              (let [batch-size 200
                    start-id (+ 21000 (* tx-idx batch-size))
                    new-sboms (gen-bulk-sboms batch-size start-id)

                    ;; Commit to XTDB
                    _ (xt/execute-tx *xtdb-client* [(into [:put-docs :prod_attestations] new-sboms)])

                    ;; Time differential approach
                    [_ diff-ms] (with-timing
                                  (diff/execute-tx! *xtdb-client* [(into [:put-docs :prod_attestations] new-sboms)]))

                    ;; Time naive approach
                    [_ naive-ms] (with-timing
                                   (naive-filter-components *xtdb-client*))]

                (swap! diff-timings conj diff-ms)
                (swap! naive-timings conj naive-ms)))

          ;; Compute statistics
          diff-stats (timing-stats @diff-timings)
          naive-stats (timing-stats @naive-timings)
          speedup (/ (:mean naive-stats) (:mean diff-stats))]

      ;; Assertions: expect >15x speedup for WHERE+UNNEST
      (is (>= speedup 15)
          (str "Expected >15x speedup for WHERE+UNNEST benchmark, got " speedup "x"))

      ;; Print detailed results
      (println "\n" (str/join "" (repeat 80 "=")))
      (println " BENCHMARK RESULT: WHERE + UNNEST - Deep Filtering")
      (println (str/join "" (repeat 80 "=")))
      (println (format "Data: 31K SBOMs, ~1.5M components (after all transactions)"))
      (println (format "Transactions: 100 incremental (200 SBOMs each)"))
      (println)
      (println "Differential Approach:")
      (println (format "  Mean: %s | Median: %s | P95: %s"
                       (format-time (:mean diff-stats))
                       (format-time (:median diff-stats))
                       (format-time (:p95 diff-stats))))
      (println)
      (println "Naive Approach (full re-query):")
      (println (format "  Mean: %s | Median: %s | P95: %s"
                       (format-time (:mean naive-stats))
                       (format-time (:median naive-stats))
                       (format-time (:p95 naive-stats))))
      (println)
      (println (format "Speedup: %s (%s benefit)"
                       (format-speedup speedup)
                       (benefit-level speedup))))))

;;; Scenario 5: LIMIT + OFFSET

(deftest ^:benchmark benchmark-limit-offset
  (testing "LIMIT + OFFSET: Top-N query with pagination"
    (let [diff-timings (atom [])
          naive-timings (atom [])

          ;; Register differential query: Skip 10, take 100
          _ (diff/register-query!
             {:query-id "bench-limit-offset"
              :xtql "(-> (from :prod_attestations [xt/id predicate_type timestamp])
                         (where (like predicate_type \"%slsa%\"))
                         (offset 10)
                         (limit 100))"
              :callback (fn [_changes])})

          ;; Initial load: 10K attestations
          initial-atts (gen-bulk-attestations 10000 0)
          _ (xt/execute-tx *xtdb-client* [(into [:put-docs :prod_attestations] initial-atts)])
          _ (diff/execute-tx! *xtdb-client* [(into [:put-docs :prod_attestations] initial-atts)])

          ;; Warmup: 5 transactions
          _ (dotimes [i 5]
              (let [warmup-atts (gen-bulk-attestations 500 (+ 20000 (* i 500)))]
                (xt/execute-tx *xtdb-client* [(into [:put-docs :prod_attestations] warmup-atts)])
                (diff/execute-tx! *xtdb-client* [(into [:put-docs :prod_attestations] warmup-atts)])
                (naive-projection *xtdb-client* :prod_attestations [:xt/id :predicate_type :timestamp])))

          ;; Timed incremental transactions: 50 batches of 1000 attestations each
          _ (dotimes [tx-idx 50]
              (let [batch-size 1000
                    start-id (+ 23000 (* tx-idx batch-size))
                    new-atts (gen-bulk-attestations batch-size start-id)

                    ;; Commit to XTDB
                    _ (xt/execute-tx *xtdb-client* [(into [:put-docs :prod_attestations] new-atts)])

                    ;; Time differential approach
                    [_ diff-ms] (with-timing
                                  (diff/execute-tx! *xtdb-client* [(into [:put-docs :prod_attestations] new-atts)]))

                    ;; Time naive approach (fetch all, apply limit/offset in memory)
                    [_ naive-ms] (with-timing
                                   (let [all-docs (naive-fetch-all *xtdb-client* :prod_attestations)
                                         filtered (filter #(str/includes? (:predicate_type %) "slsa") all-docs)
                                         paginated (take 100 (drop 10 filtered))]
                                     paginated))]

                (swap! diff-timings conj diff-ms)
                (swap! naive-timings conj naive-ms)))

          ;; Compute statistics
          diff-stats (timing-stats @diff-timings)
          naive-stats (timing-stats @naive-timings)
          speedup (/ (:mean naive-stats) (:mean diff-stats))]

      ;; Assertions: expect >3x speedup for LIMIT+OFFSET (lower benefit)
      (is (>= speedup 3)
          (str "Expected >3x speedup for LIMIT+OFFSET benchmark, got " speedup "x"))

      ;; Print detailed results
      (println "\n" (str/join "" (repeat 80 "=")))
      (println " BENCHMARK RESULT: LIMIT + OFFSET - Top-N Query")
      (println (str/join "" (repeat 80 "=")))
      (println (format "Data: 63K attestations (after all transactions)"))
      (println (format "Transactions: 50 incremental (1000 attestations each)"))
      (println (format "Note: Non-deterministic without ORDER BY"))
      (println)
      (println "Differential Approach:")
      (println (format "  Mean: %s | Median: %s | P95: %s"
                       (format-time (:mean diff-stats))
                       (format-time (:median diff-stats))
                       (format-time (:p95 diff-stats))))
      (println)
      (println "Naive Approach (full re-query):")
      (println (format "  Mean: %s | Median: %s | P95: %s"
                       (format-time (:mean naive-stats))
                       (format-time (:median naive-stats))
                       (format-time (:p95 naive-stats))))
      (println)
      (println (format "Speedup: %s (%s benefit)"
                       (format-speedup speedup)
                       (benefit-level speedup))))))

;;; Scenario 6: Complex Pipeline - Multi-Stage

(deftest ^:benchmark benchmark-complex-pipeline
  (testing "Complex Pipeline: FROM → WHERE → UNNEST → WITH → WHERE → AGGREGATE → LIMIT"
    (let [diff-timings (atom [])
          naive-timings (atom [])

          ;; Register differential query: Complex multi-stage pipeline
          _ (diff/register-query!
             {:query-id "bench-complex-pipeline"
              :xtql "(-> (from :prod_attestations [xt/id predicate_type predicate])
                         (where (like predicate_type \"%cyclonedx%\"))
                         (unnest {:comp (.. predicate :components)})
                         (with {:license_id (.. comp :licenses 0 :license :id)
                                :comp_type (.. comp :type)
                                :comp_name (.. comp :name)})
                         (where :license_id)
                         (where (= comp_type \"library\"))
                         (aggregate license_id {:count (row-count)})
                         (limit 50))"
              :callback (fn [_changes])})

          ;; Initial load: 10K SBOMs
          initial-sboms (gen-bulk-sboms 10000 0)
          _ (xt/execute-tx *xtdb-client* [(into [:put-docs :prod_attestations] initial-sboms)])
          _ (diff/execute-tx! *xtdb-client* [(into [:put-docs :prod_attestations] initial-sboms)])

          ;; Warmup: 5 transactions
          _ (dotimes [i 5]
              (let [warmup-sboms (gen-bulk-sboms 200 (+ 20000 (* i 200)))]
                (xt/execute-tx *xtdb-client* [(into [:put-docs :prod_attestations] warmup-sboms)])
                (diff/execute-tx! *xtdb-client* [(into [:put-docs :prod_attestations] warmup-sboms)])
                ;; Naive: apply all stages
                (let [sboms (naive-fetch-all *xtdb-client* :prod_attestations)
                      filtered (filter #(and (:predicate_type %)
                                             (str/includes? (:predicate_type %) "cyclonedx"))
                                       sboms)
                      components (mapcat #(get-in % [:predicate :components]) filtered)
                      with-license (map #(assoc % :license_id (get-in % [:licenses 0 :license :id])) components)
                      filtered2 (filter #(and (:license_id %) (= (:type %) "library")) with-license)
                      by-license (group-by :license_id filtered2)
                      aggregated (map (fn [[lic comps]] {:license_id lic :count (count comps)}) by-license)
                      limited (take 50 aggregated)]
                  limited)))

          ;; Timed incremental transactions: 100 batches of 200 SBOMs each
          _ (dotimes [tx-idx 100]
              (let [batch-size 200
                    start-id (+ 21000 (* tx-idx batch-size))
                    new-sboms (gen-bulk-sboms batch-size start-id)

                    ;; Commit to XTDB
                    _ (xt/execute-tx *xtdb-client* [(into [:put-docs :prod_attestations] new-sboms)])

                    ;; Time differential approach
                    [_ diff-ms] (with-timing
                                  (diff/execute-tx! *xtdb-client* [(into [:put-docs :prod_attestations] new-sboms)]))

                    ;; Time naive approach - full pipeline
                    [_ naive-ms] (with-timing
                                   (let [sboms (naive-fetch-all *xtdb-client* :prod_attestations)
                                         filtered (filter #(and (:predicate_type %)
                                                                (str/includes? (:predicate_type %) "cyclonedx"))
                                                          sboms)
                                         components (mapcat #(get-in % [:predicate :components]) filtered)
                                         with-license (map #(assoc % :license_id (get-in % [:licenses 0 :license :id])) components)
                                         filtered2 (filter #(and (:license_id %) (= (:type %) "library")) with-license)
                                         by-license (group-by :license_id filtered2)
                                         aggregated (map (fn [[lic comps]] {:license_id lic :count (count comps)}) by-license)
                                         limited (take 50 aggregated)]
                                     limited))]

                (swap! diff-timings conj diff-ms)
                (swap! naive-timings conj naive-ms)))

          ;; Compute statistics
          diff-stats (timing-stats @diff-timings)
          naive-stats (timing-stats @naive-timings)
          speedup (/ (:mean naive-stats) (:mean diff-stats))]

      ;; Assertions: expect >50x speedup for complex pipeline (compounding effects)
      (is (>= speedup 50)
          (str "Expected >50x speedup for complex pipeline benchmark, got " speedup "x"))

      ;; Print detailed results
      (println "\n" (str/join "" (repeat 80 "=")))
      (println " BENCHMARK RESULT: Complex Pipeline - Multi-Stage")
      (println (str/join "" (repeat 80 "=")))
      (println (format "Data: 31K SBOMs, ~1.5M components (after all transactions)"))
      (println (format "Pipeline: FROM → WHERE → UNNEST → WITH → WHERE → AGGREGATE → LIMIT"))
      (println (format "Transactions: 100 incremental (200 SBOMs each)"))
      (println)
      (println "Differential Approach:")
      (println (format "  Mean: %s | Median: %s | P95: %s"
                       (format-time (:mean diff-stats))
                       (format-time (:median diff-stats))
                       (format-time (:p95 diff-stats))))
      (println)
      (println "Naive Approach (full re-query):")
      (println (format "  Mean: %s | Median: %s | P95: %s"
                       (format-time (:mean naive-stats))
                       (format-time (:median naive-stats))
                       (format-time (:p95 naive-stats))))
      (println)
      (println (format "Speedup: %s (%s benefit)"
                       (format-speedup speedup)
                       (benefit-level speedup))))))

;;; Scenario 7: LEFT JOIN - Outer Join

(deftest ^:benchmark benchmark-left-join
  (testing "LEFT JOIN: Outer join preserving unmatched users"
    (let [diff-timings (atom [])
          naive-timings (atom [])

          ;; Register differential query: LEFT JOIN users with orders
          _ (diff/register-query!
             {:query-id "bench-left-join"
              :xtql "(-> (from :users [xt/id user-id tier])
                         (left-join (from :orders [order-id user-id amount])
                                    {:user-id user-id})
                         (aggregate tier {:user_count (count-distinct user-id)
                                          :total_orders (row-count)}))"
              :callback (fn [_changes])})

          ;; Initial load: 10K users, 5K orders (50% users have no orders)
          initial-users (gen-bulk-users 10000)
          initial-orders (gen-bulk-orders 5000 0 10000)
          _ (xt/execute-tx *xtdb-client* [(into [:put-docs :users] initial-users)
                                          (into [:put-docs :orders] initial-orders)])
          _ (diff/execute-tx! *xtdb-client* [(into [:put-docs :users] initial-users)
                                             (into [:put-docs :orders] initial-orders)])

          ;; Warmup: 5 transactions
          _ (dotimes [i 5]
              (let [warmup-orders (gen-bulk-orders 200 (+ 20000 (* i 200)) 10000)]
                (xt/execute-tx *xtdb-client* [(into [:put-docs :orders] warmup-orders)])
                (diff/execute-tx! *xtdb-client* [(into [:put-docs :orders] warmup-orders)])
                ;; Naive LEFT JOIN
                (let [users (naive-fetch-all *xtdb-client* :users)
                      orders (naive-fetch-all *xtdb-client* :orders)
                      joined (for [user users]
                               (let [user-orders (filter #(= (:user-id user) (:user-id %)) orders)]
                                 (if (seq user-orders)
                                   (map #(merge user %) user-orders)
                                   [(merge user {:order-id nil :amount nil})])))
                      flattened (apply concat joined)
                      by-tier (group-by :tier flattened)]
                  (map (fn [[tier records]]
                         {:tier tier
                          :user_count (count (distinct (map :user-id records)))
                          :total_orders (count (filter :order-id records))})
                       by-tier))))

          ;; Timed incremental transactions: 100 batches of 500 orders each
          _ (dotimes [tx-idx 100]
              (let [batch-size 500
                    start-id (+ 21000 (* tx-idx batch-size))
                    new-orders (gen-bulk-orders batch-size start-id 10000)

                    ;; Commit to XTDB
                    _ (xt/execute-tx *xtdb-client* [(into [:put-docs :orders] new-orders)])

                    ;; Time differential approach
                    [_ diff-ms] (with-timing
                                  (diff/execute-tx! *xtdb-client* [(into [:put-docs :orders] new-orders)]))

                    ;; Time naive approach
                    [_ naive-ms] (with-timing
                                   (let [users (naive-fetch-all *xtdb-client* :users)
                                         orders (naive-fetch-all *xtdb-client* :orders)
                                         joined (for [user users]
                                                  (let [user-orders (filter #(= (:user-id user) (:user-id %)) orders)]
                                                    (if (seq user-orders)
                                                      (map #(merge user %) user-orders)
                                                      [(merge user {:order-id nil :amount nil})])))
                                         flattened (apply concat joined)
                                         by-tier (group-by :tier flattened)]
                                     (map (fn [[tier records]]
                                            {:tier tier
                                             :user_count (count (distinct (map :user-id records)))
                                             :total_orders (count (filter :order-id records))})
                                          by-tier)))]

                (swap! diff-timings conj diff-ms)
                (swap! naive-timings conj naive-ms)))

          ;; Compute statistics
          diff-stats (timing-stats @diff-timings)
          naive-stats (timing-stats @naive-timings)
          speedup (/ (:mean naive-stats) (:mean diff-stats))]

      ;; Assertions: expect >20x speedup for LEFT JOIN
      (is (>= speedup 20)
          (str "Expected >20x speedup for LEFT JOIN benchmark, got " speedup "x"))

      ;; Print detailed results
      (println "\n" (str/join "" (repeat 80 "=")))
      (println " BENCHMARK RESULT: LEFT JOIN - Outer Join")
      (println (str/join "" (repeat 80 "=")))
      (println (format "Data: 10K users, 56K orders (after all transactions)"))
      (println (format "Transactions: 100 incremental (500 orders each)"))
      (println)
      (println "Differential Approach:")
      (println (format "  Mean: %s | Median: %s | P95: %s"
                       (format-time (:mean diff-stats))
                       (format-time (:median diff-stats))
                       (format-time (:p95 diff-stats))))
      (println)
      (println "Naive Approach (full re-query):")
      (println (format "  Mean: %s | Median: %s | P95: %s"
                       (format-time (:mean naive-stats))
                       (format-time (:median naive-stats))
                       (format-time (:p95 naive-stats))))
      (println)
      (println (format "Speedup: %s (%s benefit)"
                       (format-speedup speedup)
                       (benefit-level speedup))))))

;;; Scenario 8: WITH + WITHOUT - Field Projection

(deftest ^:benchmark benchmark-with-without-projection
  (testing "WITH + WITHOUT: Field projection and transformation"
    (let [diff-timings (atom [])
          naive-timings (atom [])

          ;; Register differential query: Extract nested fields, remove others
          _ (diff/register-query!
             {:query-id "bench-with-without"
              :xtql "(-> (from :prod_attestations [xt/id predicate_type predicate subjects])
                         (with {:builder_id (.. predicate :builder :id)
                                :build_type (.. predicate :buildType)
                                :subject_name (.. subjects 0 :name)})
                         (without :predicate :subjects))"
              :callback (fn [_changes])})

          ;; Initial load: 20K attestations
          initial-atts (gen-bulk-attestations 20000 0)
          _ (xt/execute-tx *xtdb-client* [(into [:put-docs :prod_attestations] initial-atts)])
          _ (diff/execute-tx! *xtdb-client* [(into [:put-docs :prod_attestations] initial-atts)])

          ;; Warmup: 5 transactions
          _ (dotimes [i 5]
              (let [warmup-atts (gen-bulk-attestations 1000 (+ 50000 (* i 1000)))]
                (xt/execute-tx *xtdb-client* [(into [:put-docs :prod_attestations] warmup-atts)])
                (diff/execute-tx! *xtdb-client* [(into [:put-docs :prod_attestations] warmup-atts)])
                (naive-projection *xtdb-client* :prod_attestations [:xt/id :predicate_type])))

          ;; Timed incremental transactions: 50 batches of 1000 attestations each
          _ (dotimes [tx-idx 50]
              (let [batch-size 1000
                    start-id (+ 55000 (* tx-idx batch-size))
                    new-atts (gen-bulk-attestations batch-size start-id)

                    ;; Commit to XTDB
                    _ (xt/execute-tx *xtdb-client* [(into [:put-docs :prod_attestations] new-atts)])

                    ;; Time differential approach
                    [_ diff-ms] (with-timing
                                  (diff/execute-tx! *xtdb-client* [(into [:put-docs :prod_attestations] new-atts)]))

                    ;; Time naive approach
                    [_ naive-ms] (with-timing
                                   (let [docs (naive-fetch-all *xtdb-client* :prod_attestations)
                                         transformed (map (fn [doc]
                                                            {:xt/id (:xt/id doc)
                                                             :predicate_type (:predicate_type doc)
                                                             :builder_id (get-in doc [:predicate :builder :id])
                                                             :build_type (get-in doc [:predicate :buildType])
                                                             :subject_name (get-in doc [:subjects 0 :name])})
                                                          docs)]
                                     transformed))]

                (swap! diff-timings conj diff-ms)
                (swap! naive-timings conj naive-ms)))

          ;; Compute statistics
          diff-stats (timing-stats @diff-timings)
          naive-stats (timing-stats @naive-timings)
          speedup (/ (:mean naive-stats) (:mean diff-stats))]

      ;; Assertions: expect >2x speedup for WITH+WITHOUT (lower benefit - stateless)
      (is (>= speedup 2)
          (str "Expected >2x speedup for WITH+WITHOUT benchmark, got " speedup "x"))

      ;; Print detailed results
      (println "\n" (str/join "" (repeat 80 "=")))
      (println " BENCHMARK RESULT: WITH + WITHOUT - Field Projection")
      (println (str/join "" (repeat 80 "=")))
      (println (format "Data: 75K attestations (after all transactions)"))
      (println (format "Transactions: 50 incremental (1000 attestations each)"))
      (println)
      (println "Differential Approach:")
      (println (format "  Mean: %s | Median: %s | P95: %s"
                       (format-time (:mean diff-stats))
                       (format-time (:median diff-stats))
                       (format-time (:p95 diff-stats))))
      (println)
      (println "Naive Approach (full re-query):")
      (println (format "  Mean: %s | Median: %s | P95: %s"
                       (format-time (:mean naive-stats))
                       (format-time (:median naive-stats))
                       (format-time (:p95 naive-stats))))
      (println)
      (println (format "Speedup: %s (%s benefit)"
                       (format-speedup speedup)
                       (benefit-level speedup))))))

;;; Scenario 9: RETURN - Pure Projection

(deftest ^:benchmark benchmark-return-projection
  (testing "RETURN: Pure column projection"
    (let [diff-timings (atom [])
          naive-timings (atom [])

          ;; Register differential query: Project specific fields only
          _ (diff/register-query!
             {:query-id "bench-return"
              :xtql "(-> (from :prod_attestations [xt/id predicate_type timestamp])
                         (return xt/id predicate_type))"
              :callback (fn [_changes])})

          ;; Initial load: 30K attestations
          initial-atts (gen-bulk-attestations 30000 0)
          _ (xt/execute-tx *xtdb-client* [(into [:put-docs :prod_attestations] initial-atts)])
          _ (diff/execute-tx! *xtdb-client* [(into [:put-docs :prod_attestations] initial-atts)])

          ;; Warmup: 5 transactions
          _ (dotimes [i 5]
              (let [warmup-atts (gen-bulk-attestations 1000 (+ 50000 (* i 1000)))]
                (xt/execute-tx *xtdb-client* [(into [:put-docs :prod_attestations] warmup-atts)])
                (diff/execute-tx! *xtdb-client* [(into [:put-docs :prod_attestations] warmup-atts)])
                (naive-projection *xtdb-client* :prod_attestations [:xt/id :predicate_type])))

          ;; Timed incremental transactions: 50 batches of 1000 attestations each
          _ (dotimes [tx-idx 50]
              (let [batch-size 1000
                    start-id (+ 55000 (* tx-idx batch-size))
                    new-atts (gen-bulk-attestations batch-size start-id)

                    ;; Commit to XTDB
                    _ (xt/execute-tx *xtdb-client* [(into [:put-docs :prod_attestations] new-atts)])

                    ;; Time differential approach
                    [_ diff-ms] (with-timing
                                  (diff/execute-tx! *xtdb-client* [(into [:put-docs :prod_attestations] new-atts)]))

                    ;; Time naive approach
                    [_ naive-ms] (with-timing
                                   (naive-projection *xtdb-client* :prod_attestations [:xt/id :predicate_type]))]

                (swap! diff-timings conj diff-ms)
                (swap! naive-timings conj naive-ms)))

          ;; Compute statistics
          diff-stats (timing-stats @diff-timings)
          naive-stats (timing-stats @naive-timings)
          speedup (/ (:mean naive-stats) (:mean diff-stats))]

      ;; Assertions: expect >2x speedup for RETURN (minimal benefit - stateless)
      (is (>= speedup 2)
          (str "Expected >2x speedup for RETURN benchmark, got " speedup "x"))

      ;; Print detailed results
      (println "\n" (str/join "" (repeat 80 "=")))
      (println " BENCHMARK RESULT: RETURN - Pure Projection")
      (println (str/join "" (repeat 80 "=")))
      (println (format "Data: 85K attestations (after all transactions)"))
      (println (format "Transactions: 50 incremental (1000 attestations each)"))
      (println)
      (println "Differential Approach:")
      (println (format "  Mean: %s | Median: %s | P95: %s"
                       (format-time (:mean diff-stats))
                       (format-time (:median diff-stats))
                       (format-time (:p95 diff-stats))))
      (println)
      (println "Naive Approach (full re-query):")
      (println (format "  Mean: %s | Median: %s | P95: %s"
                       (format-time (:mean naive-stats))
                       (format-time (:median naive-stats))
                       (format-time (:p95 naive-stats))))
      (println)
      (println (format "Speedup: %s (%s benefit)"
                       (format-speedup speedup)
                       (benefit-level speedup))))))

;;; Scenario 10: Stress Test - 100K Documents

(deftest ^:benchmark benchmark-stress-test-100k
  (testing "Stress Test: 100K SBOMs with complex aggregation"
    (let [diff-timings (atom [])
          naive-timings (atom [])

          ;; Register differential query: Complex pipeline at scale
          _ (diff/register-query!
             {:query-id "bench-stress-100k"
              :xtql "(-> (from :prod_attestations [xt/id predicate_type predicate])
                         (where (like predicate_type \"%cyclonedx%\"))
                         (unnest {:comp (.. predicate :components)})
                         (with {:license_id (.. comp :licenses 0 :license :id)
                                :comp_type (.. comp :type)})
                         (where :license_id)
                         (where (= comp_type \"library\"))
                         (aggregate license_id {:count (row-count)}))"
              :callback (fn [_changes])})

          ;; Initial load: 50K SBOMs
          _ (println "\nLoading 50K initial SBOMs...")
          initial-sboms (gen-bulk-sboms 50000 0)
          _ (xt/execute-tx *xtdb-client* [(into [:put-docs :prod_attestations] initial-sboms)])
          _ (diff/execute-tx! *xtdb-client* [(into [:put-docs :prod_attestations] initial-sboms)])
          _ (println "Initial load complete.")

          ;; Warmup: 2 transactions (fewer due to scale)
          _ (dotimes [i 2]
              (let [warmup-sboms (gen-bulk-sboms 1000 (+ 100000 (* i 1000)))]
                (xt/execute-tx *xtdb-client* [(into [:put-docs :prod_attestations] warmup-sboms)])
                (diff/execute-tx! *xtdb-client* [(into [:put-docs :prod_attestations] warmup-sboms)])
                (naive-aggregate-license-counts *xtdb-client*)))

          ;; Timed incremental transactions: 10 large batches of 5K SBOMs each
          _ (println "Starting timed stress test...")
          _ (dotimes [tx-idx 10]
              (let [batch-size 5000
                    start-id (+ 102000 (* tx-idx batch-size))
                    _ (println (format "  Transaction %d/%d: Loading %d SBOMs..." (inc tx-idx) 10 batch-size))
                    new-sboms (gen-bulk-sboms batch-size start-id)

                    ;; Commit to XTDB
                    _ (xt/execute-tx *xtdb-client* [(into [:put-docs :prod_attestations] new-sboms)])

                    ;; Time differential approach
                    [_ diff-ms] (with-timing
                                  (diff/execute-tx! *xtdb-client* [(into [:put-docs :prod_attestations] new-sboms)]))

                    ;; Time naive approach
                    [_ naive-ms] (with-timing
                                   (naive-aggregate-license-counts *xtdb-client*))]

                (println (format "    Differential: %s | Naive: %s"
                                 (format-time diff-ms) (format-time naive-ms)))
                (swap! diff-timings conj diff-ms)
                (swap! naive-timings conj naive-ms)))

          ;; Compute statistics
          diff-stats (timing-stats @diff-timings)
          naive-stats (timing-stats @naive-timings)
          speedup (/ (:mean naive-stats) (:mean diff-stats))]

      ;; Assertions: expect >15x speedup for stress test (with large 5K batches, benefit is moderate)
      (is (>= speedup 15)
          (str "Expected >15x speedup for stress test benchmark, got " speedup "x"))

      ;; Print detailed results
      (println "\n" (str/join "" (repeat 80 "=")))
      (println " BENCHMARK RESULT: Stress Test - 100K Documents")
      (println (str/join "" (repeat 80 "=")))
      (println (format "Data: 102K SBOMs, ~5M components (after all transactions)"))
      (println (format "Transactions: 10 large batches (5K SBOMs each)"))
      (println)
      (println "Differential Approach:")
      (println (format "  Mean: %s | Median: %s | P95: %s"
                       (format-time (:mean diff-stats))
                       (format-time (:median diff-stats))
                       (format-time (:p95 diff-stats))))
      (println)
      (println "Naive Approach (full re-query):")
      (println (format "  Mean: %s | Median: %s | P95: %s"
                       (format-time (:mean naive-stats))
                       (format-time (:median naive-stats))
                       (format-time (:p95 naive-stats))))
      (println)
      (println (format "Speedup: %s (%s benefit)"
                       (format-speedup speedup)
                       (benefit-level speedup))))))
