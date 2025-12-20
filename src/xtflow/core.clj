(ns xtflow.core
  "Main API for differential dataflow query registration and execution.

  This is the primary interface for users to:
  1. Register XTQL queries with callbacks
  2. Execute transactions that trigger incremental updates
  3. Query current results
  4. Manage the query registry"
  (:require [xtflow.delta :as delta]
            [xtflow.dataflow :as df]
            [xtflow.parser :as parser]
            [xtflow.index :as idx]
            [xtdb.api :as xt]))

;;; Global State

;; Registry of active queries.
;; Map of query-id -> query-metadata
(defonce ^:private query-registry
  (atom {}))

;; Index mapping tables to queries
(defonce ^:private table-index
  (idx/create-index-atom))

;;; Query Registration

(defn register-query!
  "Register a query for differential updates.

  Args:
    spec - Query specification map:
           :query-id - Unique identifier for this query
           :xtql - XTQL query string
           :callback - Function called with changes {:added [...] :removed [...] :modified [...]}

  Returns: Query metadata map

  Example:
    (register-query!
      {:query-id \"my-query\"
       :xtql \"(-> (from :prod_attestations [predicate_type])
                   (aggregate predicate_type {:count (row-count)}))\"
       :callback (fn [changes] (println \"Changes:\" changes))})"
  [{:keys [query-id xtql callback] :as spec}]
  (when-not query-id
    (throw (ex-info "query-id required" {:spec spec})))
  (when-not xtql
    (throw (ex-info "xtql required" {:spec spec})))
  (when-not callback
    (throw (ex-info "callback required" {:spec spec})))

  ;; Parse query
  (let [{:keys [graph parsed tables]} (parser/parse-xtql-to-graph xtql)
        metadata {:query-id query-id
                  :xtql xtql
                  :callback callback
                  :graph graph
                  :parsed parsed
                  :tables tables
                  :current-results #{}
                  :created-at (java.time.Instant/now)}]

    ;; Add to registry
    (swap! query-registry assoc query-id metadata)

    ;; Add to index
    (idx/add-query! table-index query-id tables)

    metadata))

(defn unregister-query!
  "Unregister a query.

  Args:
    query-id - Query identifier

  Returns: Removed query metadata or nil if not found"
  [query-id]
  (when-let [metadata (get @query-registry query-id)]
    ;; Remove from index
    (idx/remove-query! table-index query-id (:tables metadata))

    ;; Remove from registry
    (swap! query-registry dissoc query-id)

    metadata))

(defn list-queries
  "List all registered queries.

  Returns: Sequence of query metadata maps"
  []
  (vals @query-registry))

(defn query-info
  "Get metadata for a specific query.

  Args:
    query-id - Query identifier

  Returns: Query metadata map or nil if not found"
  [query-id]
  (get @query-registry query-id))

(defn query-results
  "Get current materialized results for a query.

  Args:
    query-id - Query identifier

  Returns: Set of result documents or nil if query not found"
  [query-id]
  (:current-results (get @query-registry query-id)))

;;; Transaction Processing

(defn extract-tables-from-tx
  "Extract tables affected by a transaction.

  Args:
    tx-ops - Transaction operations

  Returns: Set of table keywords"
  [tx-ops]
  (reduce (fn [acc op]
            (case (first op)
              :put-docs (conj acc (second op))
              :delete-docs (conj acc (second op))
              acc))
          #{}
          tx-ops))

(defn tx-ops->deltas
  "Convert transaction operations to deltas.

  Args:
    tx-ops - Transaction operations

  Returns: Vector of deltas (eager evaluation)"
  [tx-ops]
  (into []
        (mapcat (fn [op]
                  (case (first op)
                    :put-docs
                    (let [[_ table & docs] op]
                      (mapv #(delta/add-delta (assoc % :xt/table table)) docs))

                    :delete-docs
                    (let [[_ table & ids] op]
                      ;; For deletions, we'd need to fetch the doc before deleting
                      ;; For now, we'll create removal deltas with just the ID
                      (mapv #(delta/remove-delta {:xt/id % :xt/table table}) ids))

                    [])))
        tx-ops))

(defn find-affected-queries
  "Find queries affected by transaction.

  Args:
    tx-ops - Transaction operations

  Returns: Sequence of query metadata maps"
  [tx-ops]
  (let [affected-tables (extract-tables-from-tx tx-ops)
        affected-query-ids (idx/find-queries-for-tables! table-index affected-tables)]
    (keep #(get @query-registry %) affected-query-ids)))

(defn process-query-with-deltas
  "Process deltas through a query and return updated results.

  Args:
    query-metadata - Query metadata map
    deltas - Input deltas

  Returns: Map with :old-results and :new-results"
  [query-metadata deltas]
  (let [graph (:graph query-metadata)
        old-results (:current-results query-metadata)

        ;; Propagate deltas through graph
        leaf-outputs (df/propagate-deltas graph deltas)

        ;; Get output deltas from leaf operators
        output-deltas (apply concat (vals leaf-outputs))

        ;; Compute new results by applying output deltas to old results with transients
        ;; We'll materialize results by tracking documents
        old-set (set old-results)
        delta-changes (persistent!
                       (reduce (fn [^clojure.lang.ITransientSet s d]
                                 (let [mult ^long (:mult d)]
                                   (if (pos? mult)
                                     (conj! s (:doc d))
                                     (disj! s (:doc d)))))
                               (transient old-set)
                               output-deltas))
        new-results (vec delta-changes)]

    {:old-results old-results
     :new-results new-results}))

(defn execute-tx!
  "Execute a transaction and trigger differential updates.

  This is the main entry point for processing changes:
  1. Executes transaction in XTDB
  2. Identifies affected queries
  3. Propagates deltas through operator graphs
  4. Computes diffs
  5. Fires callbacks

  Args:
    xtdb-client - XTDB client instance
    tx-ops - Transaction operations (vector of [:put-docs table doc ...] or [:delete-docs table id ...])

  Returns: Map with :tx-result, :affected-query-count, :callbacks-fired

  Example:
    (execute-tx! xtdb-client
      [[:put-docs :prod_attestations
        {:xt/id \"hash123\"
         :predicate_type \"slsa-v1\"
         :subjects [...]}]])"
  [xtdb-client tx-ops]
  ;; 1. Execute transaction in XTDB
  (let [tx-result (xt/execute-tx xtdb-client tx-ops)

        ;; 2. Find affected queries
        affected-queries (find-affected-queries tx-ops)

        ;; 3. Extract deltas from transaction
        deltas (tx-ops->deltas tx-ops)

        ;; 4. Process each affected query
        callbacks-fired (atom 0)]

    (doseq [query-metadata affected-queries]
      (try
        ;; Process deltas through query
        (let [{:keys [old-results new-results]} (process-query-with-deltas query-metadata deltas)

              ;; Compute diff
              diff (delta/compute-result-diff old-results new-results)]

          ;; Update current results in registry
          (swap! query-registry assoc-in [(:query-id query-metadata) :current-results]
                 new-results)

          ;; Fire callback if there are changes
          (when (delta/has-changes? diff)
            (try
              ((:callback query-metadata) diff)
              (swap! callbacks-fired inc)
              (catch Exception e
                (println "ERROR: Callback failed for query" (:query-id query-metadata))
                (println "  " (.getMessage e))))))

        (catch Exception e
          (println "ERROR: Failed to process query" (:query-id query-metadata))
          (println "  " (.getMessage e)))))

    {:tx-result tx-result
     :affected-query-count (count affected-queries)
     :callbacks-fired @callbacks-fired}))

;;; Utility Functions

(defn reset-all-queries!
  "Reset all query states (for testing).

  Returns: nil"
  []
  (doseq [[_ query-metadata] @query-registry]
    (df/reset-all-operator-states! (:graph query-metadata))
    (swap! query-registry assoc-in [(:query-id query-metadata) :current-results] #{}))
  nil)

(defn registry-stats
  "Get statistics about the query registry.

  Returns: Map with stats"
  []
  {:query-count (count @query-registry)
   :index-stats (idx/index-stats @table-index)
   :queries-by-table (reduce (fn [acc [table query-ids]]
                               (assoc acc table (count query-ids)))
                             {}
                             (:table->queries @table-index))})

(comment
  ;; Usage examples

  ;; Register a query
  (register-query!
   {:query-id "slsa-counts"
    :xtql "(-> (from :prod_attestations [predicate_type])
                (where (like predicate_type \"%slsa%\"))
                (aggregate predicate_type {:count (row-count)}))"
    :callback (fn [changes]
                (println "SLSA counts changed:")
                (println "  Added:" (:added changes))
                (println "  Modified:" (:modified changes)))})

  ;; List queries
  (list-queries)

  ;; Execute transaction
  (def xtdb-client (xt/client {:host "localhost" :port 5432 :user "xtdb"}))
  (execute-tx! xtdb-client
               [[:put-docs :prod_attestations
                 {:xt/id "test-hash-1"
                  :predicate_type "slsa-provenance-v1"
                  :subjects [{:name "gcr.io/test"}]}]])

  ;; Get current results
  (query-results "slsa-counts")

  ;; Unregister
  (unregister-query! "slsa-counts")

  ;; Registry stats
  (registry-stats))
