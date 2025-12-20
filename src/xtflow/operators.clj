(ns xtflow.operators
  "Operator implementations for differential dataflow.

  Each operator processes deltas incrementally and maintains state as needed.
  Operators include: from, where, unnest, with, aggregate, limit, return, without, offset, rel."
  (:require [clojure.string :as str]
            [clojure.set :as set]
            [xtflow.delta :as delta]))

;;; Nested Field Access
;;; Critical for accessing arbitrary attributes in predicate bodies

(defn get-nested
  "Access nested field in a document with type hints for performance.

  This is critical for predicates which are JSON objects with string keys.
  The function tries multiple approaches to find the value:
  1. Direct lookup with the key as-is
  2. Lookup with string version of key
  3. Lookup with keyword version of key

  Supports array indexing: (get-nested doc [:predicate :components 0 :name])

  Args:
    doc - Document map
    path - Vector of keys and/or indices, e.g., [:predicate :builder :id]
           or [:predicate :components 0 :name]

  Returns: Value at path, or nil if not found"
  [doc path]
  (reduce (fn [m k]
            (cond
              ;; nil propagation
              (nil? m) nil

              ;; Numeric index - array access with type hints
              (number? k)
              (if (and (sequential? m) (< ^long k (count m)))
                (nth m ^long k)
                nil)

              ;; Map access - try multiple key formats with type hints
              (map? m)
              (or (get ^clojure.lang.IPersistentMap m k)  ; try as-is
                  (when (keyword? k)
                    (get ^clojure.lang.IPersistentMap m (name k)))  ; try string version
                  (when (string? k)
                    (get ^clojure.lang.IPersistentMap m (keyword k)))  ; try keyword version
                  nil)

              ;; Can't navigate further
              :else nil))
          doc
          path))

;;; Expression Evaluation

(defn eval-expr
  "Evaluate an expression against a document.

  Supports:
  - Literals: strings, numbers, booleans
  - Field access: :field_name
  - Nested access: [:.. :predicate :builder :id]
  - Comparisons: [:= field value], [:> field value], etc.
  - Logic: [:and expr1 expr2], [:or expr1 expr2]
  - Pattern matching: [:like field pattern]

  Args:
    expr - Expression to evaluate
    doc - Document map

  Returns: Evaluated result"
  [expr doc]
  (cond
    ;; Literal values
    (or (string? expr) (number? expr) (boolean? expr) (nil? expr))
    expr

    ;; Field access (keyword)
    (keyword? expr)
    (get doc expr)

    ;; Field access (symbol - convert to keyword)
    (symbol? expr)
    (get doc (keyword expr))

    ;; List - convert to vector and process (parser produces lists)
    ;; Also convert operator symbol to keyword
    (list? expr)
    (let [op (first expr)
          op-kw (if (symbol? op) (keyword op) op)
          rest-expr (rest expr)]
      (eval-expr (vec (cons op-kw rest-expr)) doc))

    ;; Vector - either nested path or operator
    (vector? expr)
    (let [op (first expr)]
      (case op
        ;; Nested field access
        :..
        (let [path (mapv #(if (symbol? %) (keyword %) %) (rest expr))]
          (get-nested doc path))

        ;; Comparison operators
        :=
        (let [[_ field value] expr
              field-val (eval-expr field doc)
              comp-val (eval-expr value doc)]
          (= field-val comp-val))

        :>
        (let [[_ field value] expr
              field-val (eval-expr field doc)
              comp-val (eval-expr value doc)]
          (and field-val comp-val (> field-val comp-val)))

        :<
        (let [[_ field value] expr
              field-val (eval-expr field doc)
              comp-val (eval-expr value doc)]
          (and field-val comp-val (< field-val comp-val)))

        :>=
        (let [[_ field value] expr
              field-val (eval-expr field doc)
              comp-val (eval-expr value doc)]
          (and field-val comp-val (>= field-val comp-val)))

        :<=
        (let [[_ field value] expr
              field-val (eval-expr field doc)
              comp-val (eval-expr value doc)]
          (and field-val comp-val (<= field-val comp-val)))

        ;; Not equal operators
        :!=
        (let [[_ field value] expr
              field-val (eval-expr field doc)
              comp-val (eval-expr value doc)]
          (not= field-val comp-val))

        :<>
        (let [[_ field value] expr
              field-val (eval-expr field doc)
              comp-val (eval-expr value doc)]
          (not= field-val comp-val))

        ;; Pattern matching
        :like
        (let [[_ field pattern] expr
              field-val (str (eval-expr field doc))
              pattern-str (str pattern)
              ;; Convert SQL LIKE pattern to regex
              regex-str (-> pattern-str
                            (str/replace "%" ".*")
                            (str/replace "_" "."))]
          (boolean (re-matches (re-pattern regex-str) field-val)))

        :like-regex
        (let [[_ field pattern] expr
              field-val (str (eval-expr field doc))
              pattern-str (str (eval-expr pattern doc))]
          (boolean (re-find (re-pattern pattern-str) field-val)))

        ;; String functions
        :trim
        (let [[_ field] expr
              field-val (eval-expr field doc)]
          (when field-val
            (str/trim (str field-val))))

        :trim-leading
        (let [[_ field] expr
              field-val (eval-expr field doc)]
          (when field-val
            (str/triml (str field-val))))

        :trim-trailing
        (let [[_ field] expr
              field-val (eval-expr field doc)]
          (when field-val
            (str/trimr (str field-val))))

        :overlay
        (let [[_ target replacement start-pos length] expr
              target-val (str (eval-expr target doc))
              replacement-val (str (eval-expr replacement doc))
              start (eval-expr start-pos doc)
              len (if length (eval-expr length doc) (count replacement-val))]
          (str (subs target-val 0 (dec start))
               replacement-val
               (subs target-val (+ (dec start) len))))

        ;; Null/boolean predicates
        :nil?
        (let [[_ field] expr
              field-val (eval-expr field doc)]
          (nil? field-val))

        :true?
        (let [[_ field] expr
              field-val (eval-expr field doc)]
          (true? field-val))

        :false?
        (let [[_ field] expr
              field-val (eval-expr field doc)]
          (false? field-val))

        ;; Arithmetic operators
        :+
        (let [args (rest expr)
              values (map #(eval-expr % doc) args)]
          (apply + values))

        :-
        (let [args (rest expr)
              values (map #(eval-expr % doc) args)]
          (apply - values))

        :*
        (let [args (rest expr)
              values (map #(eval-expr % doc) args)]
          (apply * values))

        :/
        (let [args (rest expr)
              values (map #(eval-expr % doc) args)]
          (apply / values))

        :mod
        (let [[_ dividend divisor] expr
              dividend-val (eval-expr dividend doc)
              divisor-val (eval-expr divisor doc)]
          (mod dividend-val divisor-val))

        ;; Math functions
        :abs
        (let [[_ field] expr
              field-val (eval-expr field doc)]
          (when field-val
            (Math/abs (double field-val))))

        :floor
        (let [[_ field] expr
              field-val (eval-expr field doc)]
          (when field-val
            (Math/floor (double field-val))))

        :ceil
        (let [[_ field] expr
              field-val (eval-expr field doc)]
          (when field-val
            (Math/ceil (double field-val))))

        :round
        (let [[_ field] expr
              field-val (eval-expr field doc)]
          (when field-val
            (Math/round (double field-val))))

        :sqrt
        (let [[_ field] expr
              field-val (eval-expr field doc)]
          (when field-val
            (Math/sqrt (double field-val))))

        :pow
        (let [[_ base exponent] expr
              base-val (eval-expr base doc)
              exp-val (eval-expr exponent doc)]
          (when (and base-val exp-val)
            (Math/pow (double base-val) (double exp-val))))

        ;; Control structures
        :if
        (let [[_ condition then-expr else-expr] expr
              cond-val (eval-expr condition doc)]
          (if cond-val
            (eval-expr then-expr doc)
            (eval-expr else-expr doc)))

        :case
        (let [args (rest expr)
              pairs (partition 2 2 nil (butlast args))
              default (last args)]
          (loop [pairs pairs]
            (if (empty? pairs)
              (eval-expr default doc)
              (let [[condition result] (first pairs)]
                (if (eval-expr condition doc)
                  (eval-expr result doc)
                  (recur (rest pairs)))))))

        :cond
        (let [args (rest expr)
              pairs (partition 2 2 nil args)]
          (loop [pairs pairs]
            (if (empty? pairs)
              nil
              (let [[condition result] (first pairs)]
                (if (or (nil? condition) (eval-expr condition doc))
                  (eval-expr result doc)
                  (recur (rest pairs)))))))

        :let
        (let [[_ bindings body] expr
              binding-pairs (partition 2 bindings)
              ;; Create new doc context with bindings
              new-doc (reduce (fn [d [name value]]
                                (assoc d (if (symbol? name) (keyword name) name)
                                       (eval-expr value d)))
                              doc
                              binding-pairs)]
          (eval-expr body new-doc))

        :coalesce
        (let [args (rest expr)]
          (loop [args args]
            (if (empty? args)
              nil
              (let [val (eval-expr (first args) doc)]
                (if (nil? val)
                  (recur (rest args))
                  val)))))

        :null-if
        (let [[_ field1 field2] expr
              val1 (eval-expr field1 doc)
              val2 (eval-expr field2 doc)]
          (if (= val1 val2)
            nil
            val1))

        ;; Boolean logic
        :and
        (every? #(eval-expr % doc) (rest expr))

        :or
        (some #(eval-expr % doc) (rest expr))

        :not
        (not (eval-expr (second expr) doc))

        ;; Default: treat as nested path
        (get-nested doc expr)))

    ;; Unknown expression type
    :else
    (throw (ex-info "Unknown expression type" {:expr expr}))))

;;; Operator Protocol

(defprotocol Operator
  "Protocol for differential dataflow operators."
  (process-delta [this delta]
    "Process a single delta and return output deltas.

    Args:
      this - Operator instance
      delta - Input delta {:doc ... :mult ...}

    Returns: Sequence of output deltas")

  (get-state [this]
    "Get current operator state (for debugging)")

  (reset-state! [this]
    "Reset operator state to initial value"))

;;; From Operator

(defrecord FromOperator [table fields state]
  Operator
  (process-delta [_ delta]
    ;; From filters by table and projects fields
    (let [doc (:doc delta)
          mult (:mult delta)]
      (when (= table (:xt/table doc))
        ;; Project requested fields (include :xt/id by default)
        ;; Convert symbols to keywords since parser may use symbols
        (let [fields-to-project (if (= fields :*)
                                  (keys doc)
                                  (distinct (concat [:xt/id]
                                                    (map #(if (symbol? %) (keyword %) %) fields))))
              projected (select-keys doc fields-to-project)]
          [(delta/make-delta projected mult)]))))

  (get-state [_] @state)
  (reset-state! [_] (reset! state nil)))

(defn create-from-operator
  "Create a FROM operator.

  Args:
    table - Table keyword (e.g., :prod_attestations)
    fields - Vector of field keywords to project, or :* for all

  Returns: FromOperator instance"
  [table fields]
  (->FromOperator table fields (atom nil)))

;;; Where Operator

(defrecord WhereOperator [predicate state]
  Operator
  (process-delta [_ delta]
    ;; Where filters documents based on predicate
    (let [doc (:doc delta)]
      (when (eval-expr predicate doc)
        [delta])))

  (get-state [_] @state)
  (reset-state! [_] (reset! state nil)))

(defn create-where-operator
  "Create a WHERE operator.

  Args:
    predicate - Expression to evaluate (e.g., [:= :predicate_type \"slsa\"])

  Returns: WhereOperator instance"
  [predicate]
  (->WhereOperator predicate (atom nil)))

;;; Unnest Operator

(defrecord UnnestOperator [array-field as-field state]
  Operator
  (process-delta [_ delta]
    ;; Unnest expands arrays into multiple deltas
    (let [doc (:doc delta)
          mult (:mult delta)
          array-val (eval-expr array-field doc)]
      (if (and (sequential? array-val) (seq array-val))
        ;; Emit one delta per array element
        (for [elem array-val]
          (delta/make-delta (assoc doc as-field elem) mult))
        ;; Not an array or empty - pass through unchanged
        [delta])))

  (get-state [_] @state)
  (reset-state! [_] (reset! state nil)))

(defn create-unnest-operator
  "Create an UNNEST operator.

  Args:
    array-field - Field expression containing array (e.g., :subjects or [:.. :predicate :components])
    as-field - Keyword for unnested element (e.g., :subject)

  Returns: UnnestOperator instance"
  [array-field as-field]
  (->UnnestOperator array-field as-field (atom nil)))

;;; With Operator

(defrecord WithOperator [computed-fields state]
  Operator
  (process-delta [_ delta]
    ;; With adds computed fields to documents
    (let [doc (:doc delta)
          mult (:mult delta)
          new-doc (reduce (fn [d [field-name expr]]
                            (assoc d field-name (eval-expr expr d)))
                          doc
                          computed-fields)]
      [(delta/make-delta new-doc mult)]))

  (get-state [_] @state)
  (reset-state! [_] (reset! state nil)))

(defn create-with-operator
  "Create a WITH operator.

  Args:
    computed-fields - Map of field-name -> expression
                      e.g., {:builder_id [:.. :predicate :builder :id]}

  Returns: WithOperator instance"
  [computed-fields]
  (->WithOperator computed-fields (atom nil)))

;;; Aggregate Operator

(defn- extract-group-key
  "Extract group-by key from a document.

  Args:
    doc - Document map
    group-by - Field or vector of fields to group by

  Returns: Group key (value or vector of values)"
  [doc group-by]
  (if (vector? group-by)
    (mapv #(eval-expr % doc) group-by)
    (eval-expr group-by doc)))

(defn- compute-aggregations
  "Compute aggregations for a group with type hints and transients for performance.

  Args:
    docs - Set of documents in group
    agg-specs - Map of result-field -> aggregation-spec
                e.g., {:count [:row-count] :total [:sum :amount]}

  Returns: Map of aggregation results"
  [docs agg-specs]
  (persistent!
   (reduce (fn [^clojure.lang.ITransientMap result [result-field agg-spec]]
             (let [[agg-fn & args] (cond
                                     (vector? agg-spec) agg-spec
                                     (list? agg-spec) (vec agg-spec)
                                     :else [agg-spec])
                    ;; Convert symbol to keyword if needed
                   agg-fn-kw (if (symbol? agg-fn) (keyword agg-fn) agg-fn)
                   value (case agg-fn-kw
                           :row-count (count docs)

                            ;; Single-pass numeric aggregations with type hints
                           :sum (reduce (fn [^double acc doc]
                                          (+ acc (double (eval-expr (first args) doc))))
                                        0.0
                                        docs)

                           :min (when (seq docs)
                                  (reduce (fn [min-val doc]
                                            (let [v (eval-expr (first args) doc)]
                                              (if (nil? min-val)
                                                v
                                                (if (and v (< (compare v min-val) 0)) v min-val))))
                                          nil
                                          docs))

                           :max (when (seq docs)
                                  (reduce (fn [max-val doc]
                                            (let [v (eval-expr (first args) doc)]
                                              (if (nil? max-val)
                                                v
                                                (if (and v (> (compare v max-val) 0)) v max-val))))
                                          nil
                                          docs))

                            ;; Average - single pass with accumulator
                           :avg (when (seq docs)
                                  (let [[sum cnt] (reduce (fn [[^double s ^long c] doc]
                                                            [(+ s (double (eval-expr (first args) doc)))
                                                             (inc c)])
                                                          [0.0 0]
                                                          docs)]
                                    (/ sum cnt)))

                            ;; Count distinct - use transient set
                           :count-distinct (count (persistent!
                                                   (reduce (fn [^clojure.lang.ITransientSet s doc]
                                                             (conj! s (eval-expr (first args) doc)))
                                                           (transient #{})
                                                           docs)))

                           (throw (ex-info "Unknown aggregation function" {:fn agg-fn-kw})))]
               (assoc! result result-field value)))
           (transient {})
           agg-specs)))

(defn- group-to-result
  "Convert group state to a result document.

  Args:
    group-key - Group key value
    group-state - Group state map {:docs #{...}}
    group-by-field - Field name for group key in result
    agg-specs - Aggregation specifications

  Returns: Result document or nil if group is empty"
  [group-key group-state group-by-field agg-specs]
  (when (seq (:docs group-state))
    (let [aggs (compute-aggregations (:docs group-state) agg-specs)
          result (if group-by-field
                   (assoc aggs group-by-field group-key)
                   aggs)]
      result)))

(defrecord AggregateOperator [group-by group-by-field agg-specs state]
  Operator
  (process-delta [_ delta]
    ;; Aggregate maintains group state and emits result deltas
    (let [doc (:doc delta)
          mult (:mult delta)
          group-key (if group-by
                      (extract-group-key doc group-by)
                      ::all)

          ;; Get current group state
          current-state @state
          old-group (get-in current-state [:groups group-key])
          old-docs (or (:docs old-group) #{})

          ;; Update group state
          new-docs (if (pos? mult)
                     (conj old-docs doc)
                     (disj old-docs doc))
          new-group (assoc old-group :docs new-docs)

          ;; Compute old and new results
          old-result (group-to-result group-key old-group group-by-field agg-specs)
          new-result (group-to-result group-key new-group group-by-field agg-specs)

          ;; Update state
          _ (swap! state assoc-in [:groups group-key] new-group)

          ;; Generate output deltas
          output (cond
                   ;; Group was removed completely
                   (and old-result (nil? new-result))
                   [(delta/remove-delta old-result)]

                   ;; Group was added
                   (and (nil? old-result) new-result)
                   [(delta/add-delta new-result)]

                   ;; Group was modified
                   (and old-result new-result (not= old-result new-result))
                   [(delta/remove-delta old-result)
                    (delta/add-delta new-result)]

                   ;; No change
                   :else
                   [])]
      output))

  (get-state [_] @state)
  (reset-state! [_] (reset! state {:groups {}})))

(defn create-aggregate-operator
  "Create an AGGREGATE operator.

  Args:
    group-by - Field or vector of fields to group by, or nil for global aggregation
    group-by-field - Field name for group key in result (optional)
    agg-specs - Map of result-field -> aggregation-spec
                e.g., {:count [:row-count] :total [:sum :amount]}

  Returns: AggregateOperator instance"
  ([agg-specs]
   (create-aggregate-operator nil nil agg-specs))
  ([group-by agg-specs]
   (let [group-by-field (cond
                          (nil? group-by) nil
                          (keyword? group-by) group-by
                          (vector? group-by) :group_key
                          :else :group_key)]
     (create-aggregate-operator group-by group-by-field agg-specs)))
  ([group-by group-by-field agg-specs]
   (->AggregateOperator group-by group-by-field agg-specs (atom {:groups {}}))))

;;; Limit Operator

(defrecord LimitOperator [n state]
  Operator
  (process-delta [_ delta]
    ;; Limit maintains current result set
    (let [doc (:doc delta)
          mult (:mult delta)
          current-state @state
          current-results (or (:results current-state) #{})]
      (if (pos? mult)
        ;; Addition: add if under limit
        (if (< (count current-results) n)
          (do
            (swap! state assoc :results (conj current-results doc))
            [delta])
          [])

        ;; Deletion: remove if in results
        (if (contains? current-results doc)
          (do
            (swap! state assoc :results (disj current-results doc))
            [delta])
          []))))

  (get-state [_] @state)
  (reset-state! [_] (reset! state {:results #{}})))

(defn create-limit-operator
  "Create a LIMIT operator.

  Note: Without ORDER BY, limit is non-deterministic.

  Args:
    n - Maximum number of results

  Returns: LimitOperator instance"
  [n]
  (->LimitOperator n (atom {:results #{}})))

;;; Return Operator

(defrecord ReturnOperator [fields state]
  Operator
  (process-delta [_ delta]
    ;; Return projects only specified fields (like WITH but removes others)
    (let [doc (:doc delta)
          mult (:mult delta)
          ;; Convert symbols to keywords since parser may use symbols
          fields-kw (map #(if (symbol? %) (keyword %) %) fields)
          projected (select-keys doc fields-kw)]
      [(delta/make-delta projected mult)]))

  (get-state [_] @state)
  (reset-state! [_] (reset! state nil)))

(defn create-return-operator
  "Create a RETURN operator.

  Returns only the specified fields, removing all others. Similar to WITH
  but performs projection rather than addition.

  Args:
    fields - Vector of field keywords/symbols to return

  Returns: ReturnOperator instance"
  [fields]
  (->ReturnOperator fields (atom nil)))

;;; Without Operator

(defrecord WithoutOperator [fields state]
  Operator
  (process-delta [_ delta]
    ;; Without removes specified fields from documents
    (let [doc (:doc delta)
          mult (:mult delta)
          ;; Convert symbols to keywords since parser may use symbols
          fields-to-remove (set (map #(if (symbol? %) (keyword %) %) fields))
          result (apply dissoc doc fields-to-remove)]
      [(delta/make-delta result mult)]))

  (get-state [_] @state)
  (reset-state! [_] (reset! state nil)))

(defn create-without-operator
  "Create a WITHOUT operator.

  Removes the specified fields from documents, keeping all others.

  Args:
    fields - Vector of field keywords/symbols to remove

  Returns: WithoutOperator instance"
  [fields]
  (->WithoutOperator fields (atom nil)))

;;; Offset Operator

(defrecord OffsetOperator [n state]
  Operator
  (process-delta [_ delta]
    ;; Offset skips first N documents - BATCHED UPDATES
    (let [doc (:doc delta)
          mult (:mult delta)
          current-state @state
          seen-count (or (:seen-count current-state) 0)
          emitted (or (:emitted current-state) #{})]

      (if (pos? ^long mult)
        ;; Addition
        (if (>= ^long seen-count ^long n)
          ;; Already past offset, emit the document
          (do
            (swap! state update :emitted (fnil conj #{}) doc)
            [delta])
          ;; Still within offset, skip but increment counter
          (do
            (swap! state update :seen-count (fnil inc 0))
            []))

        ;; Removal - BATCHED
        (if (contains? emitted doc)
          ;; Was emitted, remove it and decrement seen count - SINGLE SWAP
          (do
            (swap! state (fn [s]
                           (-> s
                               (update :emitted disj doc)
                               (update :seen-count (fnil dec 1)))))
            [delta])
          ;; Was not emitted (was within offset), just decrement counter
          (do
            (swap! state update :seen-count (fnil dec 1))
            [])))))

  (get-state [_] @state)
  (reset-state! [_] (reset! state {:seen-count 0 :emitted #{}})))

(defn create-offset-operator
  "Create an OFFSET operator.

  Skips the first N documents. Note: Without ORDER BY, the offset is
  non-deterministic as document order is not guaranteed.

  Args:
    n - Number of documents to skip

  Returns: OffsetOperator instance"
  [n]
  (->OffsetOperator n (atom {:seen-count 0 :emitted #{}})))

;;; Rel Operator

(defrecord RelOperator [tuples state]
  Operator
  (process-delta [_ delta]
    ;; REL is a source operator - it passes through deltas unchanged
    ;; The actual data emission happens during initialization
    [delta])

  (get-state [_] @state)
  (reset-state! [_] (reset! state nil)))

(defn create-rel-operator
  "Create a REL operator.

  REL is a source operator that creates an inline relation from provided
  tuples/values. Unlike other operators, it emits initial data rather than
  transforming input deltas.

  Args:
    tuples - Vector of maps representing the inline relation data

  Returns: RelOperator instance"
  [tuples]
  (->RelOperator tuples (atom nil)))

(defn rel-initial-deltas
  "Generate initial deltas for a REL operator.

  This function is called during graph initialization to emit the inline
  relation data as initial deltas.

  Args:
    rel-op - RelOperator instance

  Returns: Sequence of initial add deltas"
  [rel-op]
  (mapv delta/add-delta (:tuples rel-op)))

;;; Join Operator

(defrecord JoinOperator [left-key right-key state]
  Operator
  (process-delta [_ delta]
    (let [doc (:doc delta)
          mult (:mult delta)
          source (delta/get-source delta)
          current-state @state
          left-by-key (:left-by-key current-state {})
          right-by-key (:right-by-key current-state {})]

      (cond
        ;; Delta from left input - BATCHED UPDATES
        (= source :left)
        (let [join-key-val (get doc left-key)
              matching-right (get right-by-key join-key-val #{})]
          (if (pos? ^long mult)
            ;; Addition to left - batch all state updates
            (let [new-left-by-key (assoc left-by-key
                                         join-key-val
                                         (conj (get left-by-key join-key-val #{}) doc))
                  [joined-results emitted-updates]
                  (reduce (fn [[^clojure.lang.ITransientVector results
                                ^clojure.lang.ITransientSet emit-set] right-doc]
                            (let [joined (merge right-doc doc)]
                              [(conj! results (delta/make-delta joined mult))
                               (conj! emit-set joined)]))
                          [(transient []) (transient #{})]
                          matching-right)
                  final-emitted (persistent! emitted-updates)]
              ;; SINGLE SWAP - update all state at once
              (swap! state (fn [s]
                             (-> s
                                 (assoc :left-by-key new-left-by-key)
                                 (update :emitted (fnil into #{}) final-emitted))))
              (persistent! joined-results))
            ;; Removal from left - batch all state updates
            (let [new-left-by-key (update left-by-key join-key-val disj doc)
                  [joined-results emitted-removals]
                  (reduce (fn [[^clojure.lang.ITransientVector results
                                ^clojure.lang.ITransientSet emit-set] right-doc]
                            (let [joined (merge right-doc doc)]
                              [(conj! results (delta/make-delta joined mult))
                               (conj! emit-set joined)]))
                          [(transient []) (transient #{})]
                          matching-right)
                  final-removed (persistent! emitted-removals)]
              ;; SINGLE SWAP
              (swap! state (fn [s]
                             (-> s
                                 (assoc :left-by-key new-left-by-key)
                                 (update :emitted (fnil set/difference #{}) final-removed))))
              (persistent! joined-results))))

        ;; Delta from right input - BATCHED UPDATES
        (= source :right)
        (let [join-key-val (get doc right-key)
              matching-left (get left-by-key join-key-val #{})]
          (if (pos? ^long mult)
            ;; Addition to right - batch all state updates
            (let [new-right-by-key (assoc right-by-key
                                          join-key-val
                                          (conj (get right-by-key join-key-val #{}) doc))
                  [joined-results emitted-updates]
                  (reduce (fn [[^clojure.lang.ITransientVector results
                                ^clojure.lang.ITransientSet emit-set] left-doc]
                            (let [joined (merge doc left-doc)]
                              [(conj! results (delta/make-delta joined mult))
                               (conj! emit-set joined)]))
                          [(transient []) (transient #{})]
                          matching-left)
                  final-emitted (persistent! emitted-updates)]
              ;; SINGLE SWAP
              (swap! state (fn [s]
                             (-> s
                                 (assoc :right-by-key new-right-by-key)
                                 (update :emitted (fnil into #{}) final-emitted))))
              (persistent! joined-results))
            ;; Removal from right - batch all state updates
            (let [new-right-by-key (update right-by-key join-key-val disj doc)
                  [joined-results emitted-removals]
                  (reduce (fn [[^clojure.lang.ITransientVector results
                                ^clojure.lang.ITransientSet emit-set] left-doc]
                            (let [joined (merge doc left-doc)]
                              [(conj! results (delta/make-delta joined mult))
                               (conj! emit-set joined)]))
                          [(transient []) (transient #{})]
                          matching-left)
                  final-removed (persistent! emitted-removals)]
              ;; SINGLE SWAP
              (swap! state (fn [s]
                             (-> s
                                 (assoc :right-by-key new-right-by-key)
                                 (update :emitted (fnil set/difference #{}) final-removed))))
              (persistent! joined-results))))

        ;; Untagged delta - error
        :else
        (throw (ex-info "JOIN operator requires tagged deltas with :source"
                        {:delta delta})))))

  (get-state [_] @state)
  (reset-state! [_] (reset! state {:left-by-key {} :right-by-key {} :emitted #{}})))

(defn create-join-operator
  "Create a JOIN operator for inner join.

  Performs inner join between left and right input streams on specified keys.
  Requires tagged deltas with :source :left or :source :right.

  Args:
    left-key - Key field to join on from left input
    right-key - Key field to join on from right input

  Returns: JoinOperator instance

  Example:
    (create-join-operator :user-id :user-id)  ; join on same field name
    (create-join-operator :id :user-id)       ; join on different field names"
  [left-key right-key]
  (->JoinOperator left-key right-key (atom {:left-by-key {} :right-by-key {} :emitted #{}})))

;;; Left Join Operator

(defrecord LeftJoinOperator [left-key right-key state]
  Operator
  (process-delta [_ delta]
    (let [doc (:doc delta)
          mult (:mult delta)
          source (delta/get-source delta)
          current-state @state
          left-by-key (:left-by-key current-state {})
          right-by-key (:right-by-key current-state {})
          unmatched-left (:unmatched-left current-state #{})]

      (cond
        ;; Delta from left input
        (= source :left)
        (let [join-key-val (get doc left-key)
              matching-right (get right-by-key join-key-val #{})]
          (if (pos? mult)
            ;; Addition to left
            (do
              (swap! state update-in [:left-by-key join-key-val] (fnil conj #{}) doc)
              (if (seq matching-right)
                ;; Has matching right rows - emit joined results
                (mapv (fn [right-doc]
                        (let [joined (merge right-doc doc)]
                          (swap! state update :emitted (fnil conj #{}) joined)
                          (delta/make-delta joined mult)))
                      matching-right)
                ;; No matching right rows - emit left-only result
                (do
                  (swap! state update :unmatched-left (fnil conj #{}) doc)
                  (swap! state update :emitted (fnil conj #{}) doc)
                  [(delta/make-delta doc mult)])))
            ;; Removal from left
            (do
              (swap! state update-in [:left-by-key join-key-val] disj doc)
              (if (seq matching-right)
                ;; Had matching right rows - emit removal deltas
                (mapv (fn [right-doc]
                        (let [joined (merge right-doc doc)]
                          (swap! state update :emitted disj joined)
                          (delta/make-delta joined mult)))
                      matching-right)
                ;; Was unmatched - emit removal of left-only result
                (do
                  (swap! state update :unmatched-left disj doc)
                  (swap! state update :emitted disj doc)
                  [(delta/make-delta doc mult)])))))

        ;; Delta from right input
        (= source :right)
        (let [join-key-val (get doc right-key)
              matching-left (get left-by-key join-key-val #{})]
          (if (pos? mult)
            ;; Addition to right
            (do
              (swap! state update-in [:right-by-key join-key-val] (fnil conj #{}) doc)
              ;; For each matching left doc:
              ;; 1. If it was unmatched, emit removal of left-only result
              ;; 2. Emit joined result
              (mapcat (fn [left-doc]
                        (let [joined (merge doc left-doc)
                              was-unmatched (contains? unmatched-left left-doc)]
                          (when was-unmatched
                            ;; Remove from unmatched set
                            (swap! state update :unmatched-left disj left-doc)
                            (swap! state update :emitted disj left-doc))
                          (swap! state update :emitted (fnil conj #{}) joined)
                          ;; Return deltas: remove old left-only (if applicable), add joined
                          (if was-unmatched
                            [(delta/make-delta left-doc -1)  ; remove left-only
                             (delta/make-delta joined mult)] ; add joined
                            [(delta/make-delta joined mult)]))) ; just add joined
                      matching-left))
            ;; Removal from right
            (let [;; Check other right matches BEFORE removing
                  other-right-before (disj (get right-by-key join-key-val #{}) doc)]
              (swap! state update-in [:right-by-key join-key-val] disj doc)
              ;; For each matching left doc:
              ;; 1. Emit removal of joined result
              ;; 2. Emit left-only result (becomes unmatched again)
              (mapcat (fn [left-doc]
                        (let [joined (merge doc left-doc)]
                          (swap! state update :emitted disj joined)
                          (if (empty? other-right-before)
                            ;; No other right matches - becomes unmatched
                            (do
                              (swap! state update :unmatched-left (fnil conj #{}) left-doc)
                              (swap! state update :emitted (fnil conj #{}) left-doc)
                              [(delta/make-delta joined mult)   ; remove joined
                               (delta/make-delta left-doc 1)])  ; add left-only
                            ;; Still has other right matches - just remove this join
                            [(delta/make-delta joined mult)])))
                      matching-left))))

        ;; Untagged delta - error
        :else
        (throw (ex-info "LEFT JOIN operator requires tagged deltas with :source"
                        {:delta delta})))))

  (get-state [_] @state)
  (reset-state! [_] (reset! state {:left-by-key {} :right-by-key {} :emitted #{} :unmatched-left #{}})))

(defn create-left-join-operator
  "Create a LEFT JOIN operator for left outer join.

  Performs left outer join between left and right input streams on specified keys.
  Always emits left rows, even when there's no matching right row.
  Requires tagged deltas with :source :left or :source :right.

  Args:
    left-key - Key field to join on from left input
    right-key - Key field to join on from right input

  Returns: LeftJoinOperator instance

  Example:
    (create-left-join-operator :user-id :user-id)  ; left join on same field name
    (create-left-join-operator :id :user-id)       ; left join on different field names"
  [left-key right-key]
  (->LeftJoinOperator left-key right-key (atom {:left-by-key {} :right-by-key {} :emitted #{} :unmatched-left #{}})))

;;; Unify Operator

(defrecord UnifyOperator [unify-fields state]
  Operator
  (process-delta [_ delta]
    ;; UNIFY maintains both sides and performs multi-field matching
    ;; unify-fields is a vector of [left-field right-field] pairs
    (let [doc (:doc delta)
          mult (:mult delta)
          source (delta/get-source delta)
          current-state @state
          left-docs (:left-docs current-state #{})
          right-docs (:right-docs current-state #{})]

      (letfn [(matches? [left-doc right-doc]
                ;; Check if all unify-fields match between documents
                (every? (fn [[left-field right-field]]
                          (= (get left-doc left-field)
                             (get right-doc right-field)))
                        unify-fields))

              (find-matches [doc other-docs is-left?]
                ;; Find all documents in other-docs that match doc
                (filter (fn [other-doc]
                          (if is-left?
                            (matches? doc other-doc)
                            (matches? other-doc doc)))
                        other-docs))

              (merge-unified [left-doc right-doc]
                ;; Merge documents (right fields take precedence on conflicts)
                (merge left-doc right-doc))]

        (cond
          (= source :left)
          (if (pos? mult)
            ;; Add to left side
            (do
              (swap! state update :left-docs (fnil conj #{}) doc)
              ;; Find all matching right documents
              (let [matching-right (find-matches doc right-docs true)]
                (mapv (fn [right-doc]
                        (let [unified (merge-unified doc right-doc)]
                          (swap! state update :emitted (fnil conj #{}) unified)
                          (delta/make-delta unified mult)))
                      matching-right)))
            ;; Remove from left side
            (do
              (swap! state update :left-docs disj doc)
              ;; Remove all matches that involved this document
              (let [matching-right (find-matches doc right-docs true)]
                (mapv (fn [right-doc]
                        (let [unified (merge-unified doc right-doc)]
                          (swap! state update :emitted disj unified)
                          (delta/make-delta unified mult)))
                      matching-right))))

          (= source :right)
          (if (pos? mult)
            ;; Add to right side
            (do
              (swap! state update :right-docs (fnil conj #{}) doc)
              ;; Find all matching left documents
              (let [matching-left (find-matches doc left-docs false)]
                (mapv (fn [left-doc]
                        (let [unified (merge-unified left-doc doc)]
                          (swap! state update :emitted (fnil conj #{}) unified)
                          (delta/make-delta unified mult)))
                      matching-left)))
            ;; Remove from right side
            (do
              (swap! state update :right-docs disj doc)
              ;; Remove all matches that involved this document
              (let [matching-left (find-matches doc left-docs false)]
                (mapv (fn [left-doc]
                        (let [unified (merge-unified left-doc doc)]
                          (swap! state update :emitted disj unified)
                          (delta/make-delta unified mult)))
                      matching-left))))

          :else
          (throw (ex-info "UNIFY operator requires tagged deltas with :source"
                          {:delta delta}))))))

  (get-state [_] @state)
  (reset-state! [_] (reset! state {:left-docs #{} :right-docs #{} :emitted #{}})))

(defn create-unify-operator
  "Create a UNIFY operator for Datalog-style multi-field unification.

  Args:
    unify-fields - Vector of [left-field right-field] pairs to unify on
                   e.g., [[:user-id :user-id] [:org :org]]
                   For single field: [[:user-id :user-id]]

  Returns: UnifyOperator instance

  Example:
    (create-unify-operator [[:user-id :user-id]])           ; single field unification
    (create-unify-operator [[:id :user-id] [:org :org]])   ; multi-field unification"
  [unify-fields]
  (->UnifyOperator unify-fields (atom {:left-docs #{} :right-docs #{} :emitted #{}})))

;;; EXISTS Operator
;;
;; EXISTS executes a subquery for each document and adds a boolean field
;; indicating whether the subquery returned any results.
;; Uses TTL caching to avoid re-executing subqueries unnecessarily.

(defn- cache-expired?
  "Check if a cache entry has expired.

  Args:
    cache-entry - Map with :expires-at timestamp
    current-time - Current time in milliseconds

  Returns: true if expired or missing"
  [cache-entry current-time]
  (or (nil? cache-entry)
      (>= current-time (:expires-at cache-entry))))

(defrecord ExistsOperator [field-name nested-graph ttl-ms state]
  Operator
  (process-delta [_ delta]
    ;; EXISTS executes subquery and adds boolean field
    (let [doc (:doc delta)
          mult (:mult delta)
          doc-id (:xt/id doc)
          current-time (System/currentTimeMillis)

          ;; Check cache first
          cached (get-in @state [:cache doc-id])
          cache-valid (not (cache-expired? cached current-time))]

      (if (pos? mult)
        ;; Addition: execute subquery or use cache
        (let [exists-result (if cache-valid
                              (:result cached)
                              ;; Execute subquery
                              (let [;; Execute nested graph with this document
                                    results (if nested-graph
                                              (try
                                                (require 'xtflow.dataflow)
                                                (let [execute-fn (resolve 'xtflow.dataflow/execute-nested-graph)]
                                                  (execute-fn nested-graph doc))
                                                (catch Exception e
                                                  (throw (ex-info "Error executing EXISTS subquery"
                                                                  {:doc-id doc-id
                                                                   :error (.getMessage e)}
                                                                  e))))
                                              [])
                                    result (seq results)]
                                ;; Cache result
                                (swap! state assoc-in [:cache doc-id]
                                       {:result (boolean result)
                                        :expires-at (+ current-time ttl-ms)})
                                (boolean result)))

              ;; Add boolean field to document
              updated-doc (assoc doc field-name exists-result)]

          ;; Track in current set
          (swap! state update :current-set conj doc-id)
          [(delta/make-delta updated-doc mult)])

        ;; Removal: remove from current set and cache
        (do
          (swap! state update :current-set disj doc-id)
          (swap! state update :cache dissoc doc-id)
          [(delta/make-delta (assoc doc field-name false) mult)]))))

  (get-state [_] @state)

  (reset-state! [_]
    (reset! state {:cache {} :current-set #{}})))

(defn create-exists-operator
  "Create an EXISTS operator for subquery filtering.

  EXISTS executes a subquery for each input document and adds a boolean
  field indicating whether the subquery returned any results. Results
  are cached with TTL to improve performance.

  Args:
    field-name - Keyword name for the boolean field to add (e.g., :has-gpl-components)
    nested-graph - NestedGraph instance (from xtflow.dataflow) containing subquery
    options - Optional map with:
              :ttl-ms - Cache TTL in milliseconds (default 60000 = 60 seconds)

  Returns: ExistsOperator instance

  Example:
    (create-exists-operator :has-gpl-components nested-graph {:ttl-ms 30000})"
  ([field-name nested-graph]
   (create-exists-operator field-name nested-graph {}))
  ([field-name nested-graph options]
   (let [ttl-ms (get options :ttl-ms 60000)]
     (->ExistsOperator field-name nested-graph ttl-ms
                       (atom {:cache {} :current-set #{}})))))

(comment
  ;; Usage examples

  ;; Nested field access
  (def doc {:predicate {:builder {:id "https://github.com/actions"}}})
  (get-nested doc [:predicate :builder :id])
  ;; => "https://github.com/actions"

  ;; Array access
  (def doc {:predicate {:components [{:name "foo"} {:name "bar"}]}})
  (get-nested doc [:predicate :components 0 :name])
  ;; => "foo"

  ;; From operator
  (def from-op (create-from-operator :prod_attestations [:predicate_type :predicate]))
  (process-delta from-op (delta/add-delta {:xt/id "123" :xt/table :prod_attestations
                                           :predicate_type "slsa"}))

  ;; Where operator with nested field
  (def where-op (create-where-operator [:like [:.. :predicate :builder :id] "%github%"]))
  (process-delta where-op (delta/add-delta {:predicate {:builder {:id "https://github.com/actions"}}}))

  ;; Aggregate operator
  (def agg-op (create-aggregate-operator :predicate_type {:count [:row-count]}))
  (process-delta agg-op (delta/add-delta {:predicate_type "slsa"}))
  (process-delta agg-op (delta/add-delta {:predicate_type "slsa"}))
  (get-state agg-op))

;;; PULL Operator

(defrecord PullOperator [field-name left-key right-key state]
  Operator
  (process-delta [_ delta]
    (let [doc (:doc delta)
          mult (:mult delta)
          source (delta/get-source delta)
          current-state @state
          left-by-key (:left-by-key current-state {})
          right-by-key (:right-by-key current-state {})
          nested-docs (:nested-docs current-state {})]

      (cond
        ;; Delta from left input - BATCHED UPDATES
        (= source :left)
        (let [join-key-val (get doc left-key)
              matching-right (get right-by-key join-key-val #{})
              doc-id (:xt/id doc)]
          (if (pos? ^long mult)
            ;; Addition to left - batch all state updates
            (let [new-left-by-key (assoc left-by-key
                                         join-key-val
                                         (conj (get left-by-key join-key-val #{}) doc))
                  new-current-set (conj (get current-state :current-set #{}) doc-id)]
              (if (seq matching-right)
                ;; Has matching right rows - take first and warn if multiple
                (let [first-match (first matching-right)
                      new-nested-docs (assoc nested-docs doc-id first-match)]
                  (when (> (count matching-right) 1)
                    (println (str "WARNING: PULL operator found multiple matches for doc "
                                  doc-id " on field " field-name
                                  " (count: " (count matching-right) "). Using first match.")))
                  ;; SINGLE SWAP - update all state at once
                  (swap! state (fn [s]
                                 (-> s
                                     (assoc :left-by-key new-left-by-key)
                                     (assoc :current-set new-current-set)
                                     (assoc :nested-docs new-nested-docs))))
                  [(delta/make-delta (assoc doc field-name first-match) mult)])
                ;; No matching right rows - emit left with nil field
                (let [new-nested-docs (dissoc nested-docs doc-id)]
                  ;; SINGLE SWAP
                  (swap! state (fn [s]
                                 (-> s
                                     (assoc :left-by-key new-left-by-key)
                                     (assoc :current-set new-current-set)
                                     (assoc :nested-docs new-nested-docs))))
                  [(delta/make-delta (assoc doc field-name nil) mult)])))
            ;; Removal from left - batch all state updates
            (let [new-left-by-key (update left-by-key join-key-val disj doc)
                  new-current-set (disj (get current-state :current-set #{}) doc-id)
                  new-nested-docs (dissoc nested-docs doc-id)
                  nested (get nested-docs doc-id)]
              ;; SINGLE SWAP
              (swap! state (fn [s]
                             (-> s
                                 (assoc :left-by-key new-left-by-key)
                                 (assoc :current-set new-current-set)
                                 (assoc :nested-docs new-nested-docs))))
              [(delta/make-delta (assoc doc field-name nested) mult)])))

        ;; Delta from right input - BATCHED UPDATES
        (= source :right)
        (let [join-key-val (get doc right-key)
              matching-left (get left-by-key join-key-val #{})]
          (if (pos? ^long mult)
            ;; Addition to right - batch all state updates
            (let [new-right-by-key (assoc right-by-key
                                          join-key-val
                                          (conj (get right-by-key join-key-val #{}) doc))
                  ;; Calculate new nested docs for this key (after adding doc)
                  all-right (conj (get right-by-key join-key-val #{}) doc)
                  first-match (first all-right)]
              (when (> (count all-right) 1)
                (println (str "WARNING: PULL operator found multiple matches on field " field-name
                              " (count: " (count all-right) "). Using first match.")))
              ;; Process all matching left docs and accumulate updates
              (let [[results nested-updates]
                    (reduce (fn [[^clojure.lang.ITransientVector res updates] left-doc]
                              (let [left-id (:xt/id left-doc)
                                    old-nested (get nested-docs left-id)
                                    new-updates (assoc updates left-id first-match)
                                    deltas (if old-nested
                                             [(delta/make-delta (assoc left-doc field-name old-nested) -1)
                                              (delta/make-delta (assoc left-doc field-name first-match) 1)]
                                             [(delta/make-delta (assoc left-doc field-name nil) -1)
                                              (delta/make-delta (assoc left-doc field-name first-match) 1)])
                                    new-res (reduce conj! res deltas)]
                                [new-res new-updates]))
                            [(transient []) {}]
                            matching-left)
                    final-results (persistent! results)]
                ;; SINGLE SWAP - update right-by-key and all nested docs
                (swap! state (fn [s]
                               (-> s
                                   (assoc :right-by-key new-right-by-key)
                                   (update :nested-docs merge nested-updates))))
                final-results))
            ;; Removal from right - batch all state updates
            (let [new-right-by-key (update right-by-key join-key-val disj doc)
                  remaining-right (disj (get right-by-key join-key-val #{}) doc)]
              (let [[results nested-updates]
                    (reduce (fn [[^clojure.lang.ITransientVector res updates] left-doc]
                              (let [left-id (:xt/id left-doc)
                                    old-nested (get nested-docs left-id)
                                    [new-updates deltas]
                                    (if (seq remaining-right)
                                      ;; Still has other matches - update to first remaining
                                      (let [first-match (first remaining-right)]
                                        [(assoc updates left-id first-match)
                                         [(delta/make-delta (assoc left-doc field-name old-nested) -1)
                                          (delta/make-delta (assoc left-doc field-name first-match) 1)]])
                                      ;; No more matches - becomes nil
                                      [(assoc updates left-id ::remove)
                                       [(delta/make-delta (assoc left-doc field-name old-nested) -1)
                                        (delta/make-delta (assoc left-doc field-name nil) 1)]])]
                                [(reduce conj! res deltas) new-updates]))
                            [(transient []) {}]
                            matching-left)
                    final-results (persistent! results)]
                ;; SINGLE SWAP - update right-by-key and nested docs
                (swap! state (fn [s]
                               (let [s' (assoc s :right-by-key new-right-by-key)]
                                 (reduce (fn [state' [left-id new-nested]]
                                           (if (= new-nested ::remove)
                                             (update state' :nested-docs dissoc left-id)
                                             (assoc-in state' [:nested-docs left-id] new-nested)))
                                         s'
                                         nested-updates))))
                final-results))))

        ;; Untagged delta - error
        :else
        (throw (ex-info "PULL operator requires tagged deltas with :source"
                        {:delta delta})))))

  (get-state [_] @state)
  (reset-state! [_] (reset! state {:left-by-key {} :right-by-key {} :nested-docs {} :current-set #{}})))

(defn create-pull-operator
  "Create a PULL operator for nesting a single related document.

  Performs a join-like operation but nests the first matching right document
  as a field on the left document, rather than merging. Warns if multiple
  matches are found.

  Requires tagged deltas with :source :left or :source :right.

  Args:
    field-name - Name of field to nest the result in
    left-key - Key field to join on from left input
    right-key - Key field to join on from right input

  Returns: PullOperator instance

  Example:
    (create-pull-operator :attestation :digest :digest)
    ;; Result: {:xt/id \"sbom-1\" :digest \"sha256:abc\" :attestation {...}}"
  [field-name left-key right-key]
  (->PullOperator field-name left-key right-key
                  (atom {:left-by-key {} :right-by-key {} :nested-docs {} :current-set #{}})))

;;; PULL* Operator

(defrecord PullStarOperator [field-name left-key right-key state]
  Operator
  (process-delta [_ delta]
    (let [doc (:doc delta)
          mult (:mult delta)
          source (delta/get-source delta)
          current-state @state
          left-by-key (:left-by-key current-state {})
          right-by-key (:right-by-key current-state {})
          nested-arrays (:nested-arrays current-state {})]

      (cond
        ;; Delta from left input - BATCHED UPDATES
        (= source :left)
        (let [join-key-val (get doc left-key)
              matching-right (get right-by-key join-key-val #{})
              doc-id (:xt/id doc)]
          (if (pos? ^long mult)
            ;; Addition to left - batch all state updates
            (let [new-left-by-key (assoc left-by-key
                                         join-key-val
                                         (conj (get left-by-key join-key-val #{}) doc))
                  new-current-set (conj (get current-state :current-set #{}) doc-id)
                  array-value (vec matching-right)
                  new-nested-arrays (assoc nested-arrays doc-id array-value)]
              ;; SINGLE SWAP - update all state at once
              (swap! state (fn [s]
                             (-> s
                                 (assoc :left-by-key new-left-by-key)
                                 (assoc :current-set new-current-set)
                                 (assoc :nested-arrays new-nested-arrays))))
              [(delta/make-delta (assoc doc field-name array-value) mult)])
            ;; Removal from left - batch all state updates
            (let [new-left-by-key (update left-by-key join-key-val disj doc)
                  new-current-set (disj (get current-state :current-set #{}) doc-id)
                  new-nested-arrays (dissoc nested-arrays doc-id)
                  array-value (get nested-arrays doc-id [])]
              ;; SINGLE SWAP
              (swap! state (fn [s]
                             (-> s
                                 (assoc :left-by-key new-left-by-key)
                                 (assoc :current-set new-current-set)
                                 (assoc :nested-arrays new-nested-arrays))))
              [(delta/make-delta (assoc doc field-name array-value) mult)])))

        ;; Delta from right input - BATCHED UPDATES
        (= source :right)
        (let [join-key-val (get doc right-key)
              matching-left (get left-by-key join-key-val #{})]
          (if (pos? ^long mult)
            ;; Addition to right - batch all state updates
            (let [new-right-by-key (assoc right-by-key
                                          join-key-val
                                          (conj (get right-by-key join-key-val #{}) doc))
                  ;; Calculate new array for this key (after adding doc)
                  new-array (vec (conj (get right-by-key join-key-val #{}) doc))]
              ;; Process all matching left docs and accumulate updates
              (let [[results array-updates]
                    (reduce (fn [[^clojure.lang.ITransientVector res updates] left-doc]
                              (let [left-id (:xt/id left-doc)
                                    old-array (get nested-arrays left-id [])
                                    new-updates (assoc updates left-id new-array)
                                    deltas [(delta/make-delta (assoc left-doc field-name old-array) -1)
                                            (delta/make-delta (assoc left-doc field-name new-array) 1)]
                                    new-res (reduce conj! res deltas)]
                                [new-res new-updates]))
                            [(transient []) {}]
                            matching-left)
                    final-results (persistent! results)]
                ;; SINGLE SWAP - update right-by-key and all nested arrays
                (swap! state (fn [s]
                               (-> s
                                   (assoc :right-by-key new-right-by-key)
                                   (update :nested-arrays merge array-updates))))
                final-results))
            ;; Removal from right - batch all state updates
            (let [new-right-by-key (update right-by-key join-key-val disj doc)
                  ;; Calculate new array after removal
                  remaining-right (disj (get right-by-key join-key-val #{}) doc)
                  new-array (vec remaining-right)]
              (let [[results array-updates]
                    (reduce (fn [[^clojure.lang.ITransientVector res updates] left-doc]
                              (let [left-id (:xt/id left-doc)
                                    old-array (get nested-arrays left-id [])
                                    new-updates (assoc updates left-id new-array)
                                    deltas [(delta/make-delta (assoc left-doc field-name old-array) -1)
                                            (delta/make-delta (assoc left-doc field-name new-array) 1)]
                                    new-res (reduce conj! res deltas)]
                                [new-res new-updates]))
                            [(transient []) {}]
                            matching-left)
                    final-results (persistent! results)]
                ;; SINGLE SWAP - update right-by-key and all nested arrays
                (swap! state (fn [s]
                               (-> s
                                   (assoc :right-by-key new-right-by-key)
                                   (update :nested-arrays merge array-updates))))
                final-results))))

        ;; Untagged delta - error
        :else
        (throw (ex-info "PULL* operator requires tagged deltas with :source"
                        {:delta delta})))))

  (get-state [_] @state)
  (reset-state! [_] (reset! state {:left-by-key {} :right-by-key {} :nested-arrays {} :current-set #{}})))

(defn create-pull-star-operator
  "Create a PULL* operator for nesting multiple related documents as an array.

  Performs a join-like operation but nests all matching right documents
  as an array field on the left document. Empty arrays are always included.

  Requires tagged deltas with :source :left or :source :right.

  Args:
    field-name - Name of field to nest the result array in
    left-key - Key field to join on from left input
    right-key - Key field to join on from right input

  Returns: PullStarOperator instance

  Example:
    (create-pull-star-operator :orders :user-id :user-id)
    ;; Result: {:xt/id \"user-1\" :name \"Alice\" :orders [{...} {...}]}"
  [field-name left-key right-key]
  (->PullStarOperator field-name left-key right-key
                      (atom {:left-by-key {} :right-by-key {} :nested-arrays {} :current-set #{}})))
