(ns xtflow.index
  "Index for fast query lookup by table.

  Maintains a mapping from tables to queries that reference them,
  enabling efficient identification of affected queries when transactions occur."
  (:require [clojure.set :as set]))

;;; Index Structure

(defn create-index
  "Create an empty table→queries index.

  Returns: Index map"
  []
  {:table->queries {}})

(defn add-query
  "Add a query to the index.

  Args:
    index - Index map
    query-id - Query identifier
    tables - Set of table keywords referenced by query

  Returns: Updated index"
  [index query-id tables]
  (reduce (fn [idx table]
            (update-in idx [:table->queries table] (fnil conj #{}) query-id))
          index
          tables))

(defn remove-query
  "Remove a query from the index.

  Args:
    index - Index map
    query-id - Query identifier
    tables - Set of table keywords referenced by query

  Returns: Updated index"
  [index query-id tables]
  (reduce (fn [idx table]
            (update-in idx [:table->queries table] disj query-id))
          index
          tables))

(defn find-queries-for-tables
  "Find all queries that reference any of the given tables.

  Args:
    index - Index map
    tables - Set or sequence of table keywords

  Returns: Set of query IDs"
  [index tables]
  (reduce (fn [acc table]
            (set/union acc (get-in index [:table->queries table] #{})))
          #{}
          tables))

(defn get-all-query-ids
  "Get all query IDs in the index.

  Args:
    index - Index map

  Returns: Set of query IDs"
  [index]
  (reduce set/union #{} (vals (:table->queries index))))

(defn index-stats
  "Get statistics about the index.

  Args:
    index - Index map

  Returns: Map with :table-count, :query-count, :table-stats"
  [index]
  (let [table->queries (:table->queries index)
        table-stats (reduce (fn [acc [table queries]]
                              (assoc acc table (count queries)))
                            {}
                            table->queries)]
    {:table-count (count table->queries)
     :query-count (count (get-all-query-ids index))
     :table-stats table-stats}))

;;; Atom-based Index (for mutable state)

(defn create-index-atom
  "Create an atom-based index for concurrent updates.

  Returns: Atom containing index map"
  []
  (atom (create-index)))

(defn add-query!
  "Add a query to atom-based index.

  Args:
    index-atom - Atom containing index
    query-id - Query identifier
    tables - Set of table keywords

  Returns: Updated index value"
  [index-atom query-id tables]
  (swap! index-atom add-query query-id tables))

(defn remove-query!
  "Remove a query from atom-based index.

  Args:
    index-atom - Atom containing index
    query-id - Query identifier
    tables - Set of table keywords

  Returns: Updated index value"
  [index-atom query-id tables]
  (swap! index-atom remove-query query-id tables))

(defn find-queries-for-tables!
  "Find queries in atom-based index.

  Args:
    index-atom - Atom containing index
    tables - Set or sequence of table keywords

  Returns: Set of query IDs"
  [index-atom tables]
  (find-queries-for-tables @index-atom tables))

(comment
  ;; Usage examples

  ;; Create index
  (def idx (create-index))

  ;; Add queries
  (def idx2 (-> idx
                (add-query "q1" #{:prod_attestations})
                (add-query "q2" #{:prod_attestations :unattested_images})
                (add-query "q3" #{:unattested_images})))

  ;; Find queries for tables
  (find-queries-for-tables idx2 #{:prod_attestations :unattested_images})
  ;; => #{"q1" "q2" "q3"}

  ;; Stats
  (index-stats idx2)
  ;; => {:table-count 2
  ;;     :query-count 3
  ;;     :table-stats {:prod_attestations 2 :unattested_images 2}}

  ;; Atom-based
  (def idx-atom (create-index-atom))
  (add-query! idx-atom "q1" #{:prod_attestations})
  (find-queries-for-tables! idx-atom #{:prod_attestations}))
