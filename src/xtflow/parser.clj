(ns xtflow.parser
  "XTQL query parser - converts XTQL strings to operator graphs.

  Parses XTQL threading macros (->  style) and builds differential dataflow
  operator graphs."
  (:require [xtflow.operators :as ops]
            [xtflow.dataflow :as df]))

;;; XTQL Form Parsing

(defn parse-xtql-string
  "Parse XTQL string into Clojure data structure.

  Args:
    xtql-str - XTQL query string

  Returns: Parsed form or throws on error"
  [xtql-str]
  (try
    (read-string xtql-str)
    (catch Exception e
      (throw (ex-info "Failed to parse XTQL string"
                      {:xtql xtql-str
                       :error (.getMessage e)}
                      e)))))

(defn extract-threading-forms
  "Extract forms from threading macro (->).

  Args:
    form - Parsed XTQL form

  Returns: Sequence of operator forms"
  [form]
  (cond
    ;; Threading macro: (-> (from ...) (where ...) ...)
    (and (list? form) (= '-> (first form)))
    (rest form)

    ;; Direct operator call
    (list? form)
    [form]

    ;; Unknown
    :else
    (throw (ex-info "Expected threading macro or operator call"
                    {:form form}))))

;;; Operator Form Parsing

(defn parse-from-form
  "Parse a FROM form.

  Examples:
    (from :prod_attestations [_id predicate_type])
    (from :prod_attestations [*])

  Returns: {:op :from :table ... :fields ...}"
  [form]
  (let [[_ table fields] form]
    {:op :from
     :table table
     :fields (if (= fields '[*]) :* (vec fields))}))

(defn parse-where-form
  "Parse a WHERE form.

  Examples:
    (where (= predicate_type \"slsa\"))
    (where (like predicate_type \"%slsa%\"))

  Returns: {:op :where :expr ...}"
  [form]
  (let [[_ expr] form]
    {:op :where
     :expr expr}))

(defn parse-unnest-form
  "Parse an UNNEST form.

  Examples:
    (unnest {:subject subjects})
    (unnest {:comp (.. predicate :components)})

  Returns: {:op :unnest :bindings ...}"
  [form]
  (let [[_ bindings] form]
    {:op :unnest
     :bindings bindings}))

(defn parse-with-form
  "Parse a WITH form.

  Examples:
    (with {:digest (.. subject :digest :sha256)})
    (with {:builder_id (.. predicate :builder :id)})

  Returns: {:op :with :fields ...}"
  [form]
  (let [[_ fields] form]
    {:op :with
     :fields fields}))

(defn parse-aggregate-form
  "Parse an AGGREGATE form.

  Examples:
    (aggregate predicate_type {:count (row-count)})
    (aggregate {:total (sum amount)})

  Returns: {:op :aggregate :group-by ... :aggs ...}"
  [form]
  (cond
    ;; Two-arg form: (aggregate group-by aggs)
    (= 3 (count form))
    (let [[_ group-by aggs] form]
      {:op :aggregate
       :group-by group-by
       :aggs aggs})

    ;; One-arg form: (aggregate aggs)
    (= 2 (count form))
    (let [[_ aggs] form]
      {:op :aggregate
       :group-by nil
       :aggs aggs})

    :else
    (throw (ex-info "Invalid aggregate form" {:form form}))))

(defn parse-limit-form
  "Parse a LIMIT form.

  Examples:
    (limit 10)

  Returns: {:op :limit :n ...}"
  [form]
  (let [[_ n] form]
    {:op :limit
     :n n}))

(defn parse-return-form
  "Parse a RETURN form.

  Examples:
    (return name age)
    (return field1 field2 field3)

  Returns: {:op :return :fields ...}"
  [form]
  (let [fields (rest form)]
    {:op :return
     :fields (vec fields)}))

(defn parse-without-form
  "Parse a WITHOUT form.

  Examples:
    (without password secret_key)
    (without field1 field2 field3)

  Returns: {:op :without :fields ...}"
  [form]
  (let [fields (rest form)]
    {:op :without
     :fields (vec fields)}))

(defn parse-offset-form
  "Parse an OFFSET form.

  Examples:
    (offset 10)
    (offset 50)

  Returns: {:op :offset :n ...}"
  [form]
  (let [[_ n] form]
    {:op :offset
     :n n}))

(defn parse-rel-form
  "Parse a REL form.

  Examples:
    (rel [{:x 1 :y 2} {:x 3 :y 4}])
    (rel [{:name \"Alice\" :age 30} {:name \"Bob\" :age 25}])

  Returns: {:op :rel :tuples ...}"
  [form]
  (let [[_ tuples] form]
    {:op :rel
     :tuples (vec tuples)}))

(defn parse-join-form
  "Parse a JOIN form.

  Examples:
    (join (from :orders [...]) {:user-id user-id})

  Returns: {:op :join :right-query ... :join-keys ...}"
  [form]
  (let [[_ right-query join-map] form
        ;; Extract the join keys from the map (e.g., {:user-id user-id} -> :user-id)
        ;; Assumes left and right use same field name
        join-keys (keys join-map)]
    {:op :join
     :right-query right-query
     :join-keys (vec join-keys)}))

(defn parse-left-join-form
  "Parse a LEFT JOIN form.

  Examples:
    (left-join (from :orders [...]) {:user-id user-id})

  Returns: {:op :left-join :right-query ... :join-keys ...}"
  [form]
  (let [[_ right-query join-map] form
        join-keys (keys join-map)]
    {:op :left-join
     :right-query right-query
     :join-keys (vec join-keys)}))

(defn parse-unify-form
  "Parse a UNIFY form.

  Examples:
    (unify (from :users [...]) (from :permissions [...]) {:user-id user-id :org org})

  Returns: {:op :unify :left-query ... :right-query ... :unify-keys ...}"
  [form]
  (let [[_ left-query right-query unify-map] form
        ;; Extract unify keys - assumes same field names on both sides
        unify-keys (keys unify-map)]
    {:op :unify
     :left-query left-query
     :right-query right-query
     :unify-keys (vec unify-keys)}))

(defn parse-exists-form
  "Parse an EXISTS form.

  EXISTS executes a subquery for each document and adds a boolean field
  indicating whether the subquery returned any results.

  Examples:
    (exists :has-gpl-components
            (-> (unnest {:comp (.. :components)})
                (where (= (.. comp :license :id) \"GPL-3.0\"))))

    (exists :has-vulnerabilities
            (-> (from :vulnerabilities [severity])
                (where (= severity \"critical\")))
            {:ttl-ms 30000})

  Returns: {:op :exists :field-name ... :subquery ... :options ...}"
  [form]
  (let [[_ field-name subquery & [options]] form]
    {:op :exists
     :field-name field-name
     :subquery subquery
     :options (or options {})}))

(defn parse-pull-form
  "Parse a PULL form.

  PULL performs a join and nests the first matching document as a field
  on the left document. Warns if multiple matches are found.

  Examples:
    (pull :attestation
          (from :attestations [predicate_type timestamp])
          {:digest digest})

    (pull :user
          (from :users [name email])
          {:user-id user-id})

  Returns: {:op :pull :field-name ... :right-query ... :join-keys ...}"
  [form]
  (let [[_ field-name right-query join-keys] form]
    {:op :pull
     :field-name field-name
     :right-query right-query
     :join-keys join-keys}))

(defn parse-pull-star-form
  "Parse a PULL* form.

  PULL* performs a join and nests all matching documents as an array field
  on the left document. Empty arrays are always included.

  Examples:
    (pull* :orders
           (from :orders [order-id amount status])
           {:user-id user-id})

    (pull* :components
           (from :components [name version license])
           {:sbom-id sbom-id})

  Returns: {:op :pull* :field-name ... :right-query ... :join-keys ...}"
  [form]
  (let [[_ field-name right-query join-keys] form]
    {:op :pull*
     :field-name field-name
     :right-query right-query
     :join-keys join-keys}))

(defn parse-operator-form
  "Parse a single operator form.

  Args:
    form - Operator form (e.g., (from :table [fields]))

  Returns: Parsed operator map"
  [form]
  (when-not (list? form)
    (throw (ex-info "Expected operator form to be a list" {:form form})))

  (let [op-name (first form)]
    (case op-name
      from (parse-from-form form)
      where (parse-where-form form)
      unnest (parse-unnest-form form)
      with (parse-with-form form)
      aggregate (parse-aggregate-form form)
      limit (parse-limit-form form)
      return (parse-return-form form)
      without (parse-without-form form)
      offset (parse-offset-form form)
      rel (parse-rel-form form)
      join (parse-join-form form)
      left-join (parse-left-join-form form)
      unify (parse-unify-form form)
      exists (parse-exists-form form)
      pull (parse-pull-form form)
      pull* (parse-pull-star-form form)
      (throw (ex-info "Unknown operator" {:op op-name :form form})))))

;;; Operator Graph Construction

(defn create-operator-from-parsed
  "Create an operator instance from parsed form.

  Args:
    parsed - Parsed operator map

  Returns: Operator instance"
  [parsed]
  (case (:op parsed)
    :from
    (ops/create-from-operator (:table parsed) (:fields parsed))

    :where
    (ops/create-where-operator (:expr parsed))

    :unnest
    (let [bindings (:bindings parsed)
          ;; Expect single binding like {:subject subjects}
          [as-field array-field] (first bindings)]
      (ops/create-unnest-operator array-field as-field))

    :with
    (ops/create-with-operator (:fields parsed))

    :aggregate
    (if (:group-by parsed)
      (ops/create-aggregate-operator (:group-by parsed) (:aggs parsed))
      (ops/create-aggregate-operator (:aggs parsed)))

    :limit
    (ops/create-limit-operator (:n parsed))

    :return
    (ops/create-return-operator (:fields parsed))

    :without
    (ops/create-without-operator (:fields parsed))

    :offset
    (ops/create-offset-operator (:n parsed))

    :rel
    (ops/create-rel-operator (:tuples parsed))

    :join
    (let [join-keys (:join-keys parsed)
          ;; Assume same field names on both sides for now
          join-key (first join-keys)]
      (ops/create-join-operator join-key join-key))

    :left-join
    (let [join-keys (:join-keys parsed)
          ;; Assume same field names on both sides for now
          join-key (first join-keys)]
      (ops/create-left-join-operator join-key join-key))

    :unify
    (let [unify-keys (:unify-keys parsed)
          ;; Convert [:user-id :org] to [[:user-id :user-id] [:org :org]]
          unify-fields (mapv (fn [k] [k k]) unify-keys)]
      (ops/create-unify-operator unify-fields))

    :exists
    (throw (ex-info "EXISTS operator requires graph-level support for nested queries. Not yet implemented in simple linear parser."
                    {:parsed parsed
                     :hint "EXISTS operators need special graph construction to execute subqueries"}))

    :pull
    (throw (ex-info "PULL operator requires graph-level support for nested queries. Not yet implemented in simple linear parser."
                    {:parsed parsed
                     :hint "PULL operators need special graph construction to execute subqueries and nest results"}))

    :pull*
    (throw (ex-info "PULL* operator requires graph-level support for nested queries. Not yet implemented in simple linear parser."
                    {:parsed parsed
                     :hint "PULL* operators need special graph construction to execute subqueries and nest results as arrays"}))))

(defn extract-tables
  "Extract table references from parsed operators.

  Args:
    parsed-ops - Sequence of parsed operator maps

  Returns: Set of table keywords"
  [parsed-ops]
  (reduce (fn [acc op]
            (if (= :from (:op op))
              (conj acc (:table op))
              acc))
          #{}
          parsed-ops))

(defn multi-input-operator?
  "Check if a parsed operator is a multi-input operator (JOIN, LEFT-JOIN, UNIFY)."
  [parsed-op]
  (contains? #{:join :left-join :unify} (:op parsed-op)))

(defn find-multi-input-operator-index
  "Find the index of the first multi-input operator in the parsed pipeline.
   Returns nil if none found."
  [parsed-ops]
  (first (keep-indexed #(when (multi-input-operator? %2) %1) parsed-ops)))

(defn build-multi-input-graph
  "Build a graph for a pipeline containing multi-input operators.

  Args:
    parsed-ops - Sequence of parsed operator maps
    operators - Sequence of operator instances

  Returns: OperatorGraph instance"
  [parsed-ops operators]
  (let [multi-input-idx (find-multi-input-operator-index parsed-ops)

        ;; Split pipeline into: left-ops, multi-input-op, continuation-ops
        multi-input-parsed (nth parsed-ops multi-input-idx)

        left-ops (subvec (vec operators) 0 multi-input-idx)
        multi-input-op (nth operators multi-input-idx)
        continuation-ops (subvec (vec operators) (inc multi-input-idx))

        ;; Parse the nested right query (for JOIN/LEFT-JOIN) or both queries (for UNIFY)
        right-query-form (:right-query multi-input-parsed)
        left-query-form (when (= :unify (:op multi-input-parsed))
                          (:left-query multi-input-parsed))

        ;; Build right pipeline
        right-parsed-ops (when right-query-form
                           (let [forms (extract-threading-forms right-query-form)]
                             (mapv parse-operator-form forms)))
        right-operators (when right-parsed-ops
                          (mapv create-operator-from-parsed right-parsed-ops))

        ;; Build explicit left pipeline for UNIFY (it has both left and right queries)
        unify-left-parsed-ops (when left-query-form
                                (let [forms (extract-threading-forms left-query-form)]
                                  (mapv parse-operator-form forms)))
        unify-left-operators (when unify-left-parsed-ops
                               (mapv create-operator-from-parsed unify-left-parsed-ops))

        ;; Determine which left pipeline to use
        actual-left-ops (if unify-left-operators unify-left-operators left-ops)

        ;; Create operator IDs
        left-ids (vec (map-indexed (fn [idx _] (keyword (str "left-" idx))) actual-left-ops))
        right-ids (vec (map-indexed (fn [idx _] (keyword (str "right-" idx))) right-operators))
        multi-input-id ::multi-input
        continuation-ids (vec (map-indexed (fn [idx _] (keyword (str "cont-" idx))) continuation-ops))

        ;; Build operators map
        operators-map (merge
                       (zipmap left-ids actual-left-ops)
                       (zipmap right-ids right-operators)
                       {multi-input-id multi-input-op}
                       (zipmap continuation-ids continuation-ops))

        ;; Build edges map
        edges (merge
               ;; Left pipeline edges
               (into {} (map vector
                             (butlast left-ids)
                             (map vector (rest left-ids))))

               ;; Right pipeline edges
               (into {} (map vector
                             (butlast right-ids)
                             (map vector (rest right-ids))))

               ;; Connect left pipeline to multi-input (left port)
               {(last left-ids) [{:to multi-input-id :input-port :left}]}

               ;; Connect right pipeline to multi-input (right port)
               (when (seq right-ids)
                 {(last right-ids) [{:to multi-input-id :input-port :right}]})

               ;; Connect multi-input to continuation
               (when (seq continuation-ids)
                 {multi-input-id [(first continuation-ids)]})

               ;; Continuation pipeline edges
               (into {} (map vector
                             (butlast continuation-ids)
                             (map vector (rest continuation-ids)))))]

    (df/create-operator-graph operators-map edges)))

(defn parse-xtql-to-graph
  "Parse XTQL string to operator graph.

  Handles both linear pipelines and multi-input operators (JOIN, LEFT-JOIN, UNIFY).
  Automatically detects multi-input operators and builds appropriate graph structure.

  Args:
    xtql-str - XTQL query string

  Returns: Map with:
           :graph - OperatorGraph instance
           :parsed - Sequence of parsed operator maps
           :tables - Set of referenced tables"
  [xtql-str]
  (let [form (parse-xtql-string xtql-str)
        forms (extract-threading-forms form)
        parsed (mapv parse-operator-form forms)
        operators (mapv create-operator-from-parsed parsed)
        tables (extract-tables parsed)
        ;; Check if pipeline contains multi-input operators
        has-multi-input? (some multi-input-operator? parsed)
        graph (if has-multi-input?
                (build-multi-input-graph parsed operators)
                (df/create-linear-pipeline operators))]
    {:graph graph
     :parsed parsed
     :tables tables}))

(comment
  ;; Usage examples

  ;; Simple query
  (parse-xtql-to-graph
   "(-> (from :prod_attestations [predicate_type])
         (where (like predicate_type \"%slsa%\")))")

  ;; Aggregate query
  (parse-xtql-to-graph
   "(-> (from :prod_attestations [predicate_type])
         (aggregate predicate_type {:count (row-count)}))")

  ;; Complex nested query
  (parse-xtql-to-graph
   "(-> (from :prod_attestations [predicate])
         (with {:builder (.. predicate :builder :id)})
         (where (like builder \"%github%\"))
         (aggregate builder {:count (row-count)}))")

  ;; Unnest query
  (parse-xtql-to-graph
   "(-> (from :prod_attestations [subjects])
         (unnest {:s subjects})
         (with {:digest (.. s :digest :sha256)}))"))
