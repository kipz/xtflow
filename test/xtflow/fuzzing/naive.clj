(ns xtflow.fuzzing.naive
  "Naive query executor for fuzzing tests.

  Implements correct (but inefficient) query execution using standard
  Clojure collections. This serves as the ground truth for comparing
  against the incremental dataflow implementation.

  Key principle: Correctness over performance.
  All operations use simple, obviously-correct implementations."
  (:require [xtdb.api :as xt]
            [clojure.string :as str]))

;;; ============================================================================
;;; Expression Evaluation (reuse from xtflow.operators or reimplement)
;;; ============================================================================

(defn normalize-field-name
  "Convert field name variations (underscore <-> hyphen).
  Returns a seq of possible field name variations to try."
  [field]
  (when field
    (let [field-kw (cond
                     (keyword? field) field
                     (string? field) (keyword field)
                     (symbol? field) (keyword field)
                     :else field)]
      (when (keyword? field-kw)
        (let [field-str (name field-kw)
              with-hyphen (keyword (str/replace field-str #"_" "-"))
              with-underscore (keyword (str/replace field-str #"-" "_"))]
          [field-kw with-hyphen with-underscore])))))

(defn get-field
  "Get field value from document, trying multiple naming conventions.
  Handles XTDB's automatic kebab-case conversion (underscore -> hyphen)."
  [doc field]
  (when field
    (let [variants (normalize-field-name field)]
      (some #(get doc %) variants))))

(defn get-nested
  "Access nested field in a document.
  Supports paths like [:predicate :builder :id] and array indexing."
  [doc path]
  (reduce (fn [m k]
            (cond
              (nil? m) nil
              (number? k) (when (and (sequential? m) (< k (count m)))
                            (nth m k))
              (map? m) (get-field m k)
              :else nil))
          doc
          path))

(defn eval-expr
  "Evaluate an expression against a document."
  [expr doc]
  (cond
    ;; Literals
    (or (string? expr) (number? expr) (boolean? expr) (nil? expr))
    expr

    ;; Field access
    (keyword? expr)
    (get-field doc expr)

    ;; Symbol - convert to keyword
    (symbol? expr)
    (get-field doc expr)

    ;; Vector - operator or nested path
    (vector? expr)
    (let [op (first expr)]
      (case op
        ;; Nested field access
        :..
        (get-nested doc (vec (rest expr)))

        ;; Comparison operators
        :=
        (let [[_ field value] expr]
          (= (eval-expr field doc) (eval-expr value doc)))

        :>
        (let [[_ field value] expr
              field-val (eval-expr field doc)
              comp-val (eval-expr value doc)]
          (and field-val comp-val (> (double field-val) (double comp-val))))

        :<
        (let [[_ field value] expr
              field-val (eval-expr field doc)
              comp-val (eval-expr value doc)]
          (and field-val comp-val (< (double field-val) (double comp-val))))

        :>=
        (let [[_ field value] expr
              field-val (eval-expr field doc)
              comp-val (eval-expr value doc)]
          (and field-val comp-val (>= (double field-val) (double comp-val))))

        :<=
        (let [[_ field value] expr
              field-val (eval-expr field doc)
              comp-val (eval-expr value doc)]
          (and field-val comp-val (<= (double field-val) (double comp-val))))

        :!=
        (let [[_ field value] expr]
          (not= (eval-expr field doc) (eval-expr value doc)))

        :<>
        (let [[_ field value] expr]
          (not= (eval-expr field doc) (eval-expr value doc)))

        ;; Logical operators
        :and
        (every? #(eval-expr % doc) (rest expr))

        :or
        (some #(eval-expr % doc) (rest expr))

        :not
        (not (eval-expr (second expr) doc))

        ;; String operations
        :like
        (let [[_ field pattern] expr
              field-val (str (eval-expr field doc))
              pattern-str (str pattern)
              regex-pattern (-> pattern-str
                                (clojure.string/replace #"%" ".*")
                                (clojure.string/replace #"_" ".")
                                re-pattern)]
          (boolean (re-matches regex-pattern field-val)))

        ;; Default: treat as literal
        expr))

    ;; List - convert to vector and process
    (list? expr)
    (eval-expr (vec expr) doc)

    ;; Default: return as-is
    :else expr))

;;; ============================================================================
;;; Data Fetching
;;; ============================================================================

(defn naive-fetch-all
  "Fetch all documents from a table (full table scan).

  Uses XTQL to fetch all documents. Results are sorted by :xt/id to ensure
  deterministic ordering. This is critical for LIMIT/OFFSET without ORDER BY -
  naive must be deterministic ground truth."
  [xtdb-client table]
  (let [result (xt/q xtdb-client (list 'from table ['*]))]
    (vec (sort-by :xt/id result))))

;;; ============================================================================
;;; Operator Implementations
;;; ============================================================================

(defn naive-from
  "FROM operator: fetch all documents from table and project fields."
  [xtdb-client table fields]
  (let [all-docs (naive-fetch-all xtdb-client table)]
    (if (= fields :*)
      all-docs
      ;; Project fields, handling both keyword and string keys
      ;; Only include fields that exist in the document (not nil)
      (mapv (fn [doc]
              (into {}
                    (keep (fn [field]
                            (let [value (get-field doc field)]
                              (when-not (nil? value)
                                [field value])))
                          fields)))
            all-docs))))

(defn naive-where
  "WHERE operator: filter documents by predicate."
  [data predicate-expr]
  (filterv #(eval-expr predicate-expr %) data))

(defn naive-with
  "WITH operator: add computed fields."
  [data field-mappings]
  (mapv (fn [doc]
          (merge doc
                 (into {}
                       (for [[new-field source-expr] field-mappings]
                         [new-field (eval-expr source-expr doc)]))))
        data))

(defn naive-without
  "WITHOUT operator: remove fields."
  [data fields]
  ;; Deduplicate fields and ensure they're keywords
  (let [fields-to-remove (distinct (map #(if (symbol? %) (keyword %) %) fields))]
    (mapv #(apply dissoc % fields-to-remove) data)))

(defn naive-return
  "RETURN operator: project specific fields."
  [data fields]
  (mapv #(select-keys % fields) data))

(defn naive-unnest
  "UNNEST operator: expand array field into multiple rows."
  [data binding]
  (let [[var-name source-expr] (first binding)]
    (vec
     (for [doc data
           :let [array-val (eval-expr source-expr doc)]
           :when (and array-val (sequential? array-val))
           item array-val]
       (assoc doc var-name item)))))

(defn naive-limit
  "LIMIT operator: take first N documents."
  [data n]
  (vec (take n data)))

(defn naive-offset
  "OFFSET operator: skip first N documents."
  [data n]
  (vec (drop n data)))

;;; Aggregation Functions

(defn apply-agg-fn
  "Apply aggregation function to a group of documents."
  [agg-fn-spec docs]
  (let [[agg-type field] agg-fn-spec]
    (case agg-type
      :row-count
      (count docs)

      :sum
      (reduce + 0 (map #(or (get-field % field) 0) docs))

      :avg
      (let [values (keep #(get-field % field) docs)]
        (if (seq values)
          (/ (reduce + values) (count values))
          nil))

      :min
      (when-let [values (seq (keep #(get-field % field) docs))]
        (apply min values))

      :max
      (when-let [values (seq (keep #(get-field % field) docs))]
        (apply max values))

      :count-distinct
      (count (set (keep #(get-field % field) docs)))

      ;; Default
      nil)))

(defn naive-aggregate
  "AGGREGATE operator: group and aggregate."
  [data group-by-expr agg-specs]
  (let [;; Determine the field name to use for the group key in results
        ;; Match XTFlow's behavior: use the field name itself for single fields,
        ;; use :group_key for vector fields, and omit for global aggregation
        group-by-field (cond
                         (nil? group-by-expr) nil
                         (keyword? group-by-expr) group-by-expr
                         (vector? group-by-expr) :group_key
                         :else :group_key)

        ;; Group documents
        grouped (if group-by-expr
                  (group-by #(eval-expr group-by-expr %) data)
                  {::no-group data})

        ;; Compute aggregates for each group
        results (for [[group-key docs] grouped]
                  (merge
                   ;; Group key (if grouping)
                   (when (not= group-key ::no-group)
                     {group-by-field group-key})
                   ;; Aggregates
                   (into {}
                         (for [[agg-name agg-fn] agg-specs]
                           [agg-name (apply-agg-fn agg-fn docs)]))))]
    (vec results)))

;;; ============================================================================
;;; Join Operations (not yet implemented - Phase 2+)
;;; ============================================================================

(defn naive-join
  "JOIN operator: inner join two datasets."
  [left-data right-data left-key right-key]
  (vec
   (for [left left-data
         right right-data
         :when (= (get left left-key) (get right right-key))]
     (merge left right))))

(defn naive-left-join
  "LEFT-JOIN operator: left outer join two datasets."
  [left-data right-data left-key right-key]
  (vec
   (for [left left-data]
     (if-let [matching (first (filter #(= (get left left-key) (get % right-key))
                                      right-data))]
       (merge left matching)
       left))))

(defn naive-unify
  "UNIFY operator: union with deduplication."
  [left-data right-data]
  (vec (distinct (concat left-data right-data))))

;;; ============================================================================
;;; Subquery Operations (not yet implemented - Phase 3+)
;;; ============================================================================

(defn naive-exists
  "EXISTS operator: filter by subquery existence."
  [data _subquery-fn]
  ;; Placeholder - will be implemented when needed
  data)

(defn naive-pull
  "PULL operator: fetch related documents."
  [data _patterns _xtdb-client]
  ;; Placeholder - will be implemented when needed
  data)

;;; ============================================================================
;;; Main Executor
;;; ============================================================================

(defn naive-execute-query
  "Execute query naively: fetch all data, process in memory.

  Args:
    xtdb-client - XTDB client for data access
    query-spec - Query specification from generator
      {:table - table name
       :operators - vector of operator specs}

  Returns: Vector of result documents"
  [xtdb-client query-spec]
  (let [operators (:operators query-spec)
        from-op (first operators)
        rest-ops (rest operators)]

    ;; Start with FROM
    (loop [data (naive-from xtdb-client (:table from-op) (:fields from-op))
           ops rest-ops]
      (if (empty? ops)
        data
        (let [op (first ops)
              new-data (case (:op op)
                         :where (naive-where data (:predicate op))
                         :with (naive-with data (:fields op))
                         :without (naive-without data (:fields op))
                         :return (naive-return data (:fields op))
                         :unnest (naive-unnest data (:binding op))
                         :aggregate (naive-aggregate data (:group-by op) (:aggregates op))
                         :limit (naive-limit data (:n op))
                         :offset (naive-offset data (:n op))
                         ;; Unsupported ops - pass through
                         data)]
          (recur new-data (rest ops)))))))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(comment
  ;; Example usage

  ;; Simple query: fetch all users
  (naive-execute-query
   xtdb-client
   {:table :users
    :operators [{:op :from :table :users :fields [:*]}]})

  ;; Query with WHERE
  (naive-execute-query
   xtdb-client
   {:table :users
    :operators [{:op :from :table :users :fields [:*]}
                {:op :where :predicate [:= :tier "premium"]}]})

  ;; Query with AGGREGATE
  (naive-execute-query
   xtdb-client
   {:table :users
    :operators [{:op :from :table :users :fields [:*]}
                {:op :aggregate
                 :group-by :tier
                 :aggregates {:count [:row-count]}}]}))
