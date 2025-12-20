(ns xtflow.dataflow
  "Operator graph execution and delta propagation engine.

  This module implements the core dataflow execution that propagates deltas
  through an operator graph, updating operator states incrementally."
  (:require [xtflow.delta :as delta]
            [xtflow.operators :as ops]
            [clojure.set :as set]
            [clojure.stacktrace]))

;;; Operator Graph Structure

(defrecord OperatorGraph
           [operators       ; Map of operator-id -> operator instance
            edges           ; Map of operator-id -> [downstream-specs]
                            ; downstream-spec can be:
                            ;   - operator-id (single input, no tagging)
                            ;   - {:to op-id :input-port :left/:right} (multi-input)
            root-operators  ; Set of root operator IDs (no upstream)
            leaf-operators  ; Set of leaf operator IDs (no downstream)
            metadata])      ; Additional metadata

(defn normalize-downstream-spec
  "Normalize a downstream spec to map format.

  Args:
    spec - Either operator-id or {:to op-id :input-port ...}

  Returns: Map with :to and :input-port keys"
  [spec]
  (if (map? spec)
    spec
    {:to spec :input-port nil}))

(defn extract-downstream-id
  "Extract operator ID from a downstream spec.

  Args:
    spec - Either operator-id or {:to op-id :input-port ...}

  Returns: Operator ID"
  [spec]
  (if (map? spec)
    (:to spec)
    spec))

(defn create-operator-graph
  "Create an operator graph.

  Args:
    operators - Map of operator-id -> operator instance
    edges - Map of operator-id -> vector of downstream specs
            Each spec can be:
              - operator-id (for single-input operators)
              - {:to op-id :input-port :left/:right} (for multi-input)
    metadata - Optional metadata map

  Returns: OperatorGraph instance"
  [operators edges & [metadata]]
  (let [all-ids (set (keys operators))
        downstream-ids (set (mapcat #(map extract-downstream-id %) (vals edges)))
        upstream-ids (set (keys edges))
        root-ids (set/difference all-ids downstream-ids)
        leaf-ids (set/difference all-ids upstream-ids)]
    (->OperatorGraph operators edges root-ids leaf-ids (or metadata {}))))

;;; Delta Propagation

(defn rel-operator?
  "Check if an operator is a REL (source) operator.

  Args:
    operator - Operator instance

  Returns: true if operator is a RelOperator"
  [operator]
  (instance? xtflow.operators.RelOperator operator))

(defn get-initial-deltas-for-operator
  "Get initial deltas for an operator.

  For REL operators, generates deltas from inline data.
  For other operators, uses provided input-deltas.

  Args:
    operator - Operator instance
    input-deltas - Default input deltas

  Returns: Sequence of deltas"
  [operator input-deltas]
  (if (rel-operator? operator)
    (ops/rel-initial-deltas operator)
    input-deltas))

(defn propagate-through-operator
  "Propagate a delta through a single operator.

  Args:
    operator - Operator instance
    delta - Input delta

  Returns: Sequence of output deltas"
  [operator delta]
  (try
    (ops/process-delta operator delta)
    (catch Exception e
      (throw (ex-info "Error processing delta through operator"
                      {:operator operator
                       :delta delta
                       :error (.getMessage e)}
                      e)))))

(defn propagate-to-downstream
  "Propagate deltas to downstream operators.

  For multi-input operators, tags deltas with the input port.
  For single-input operators, passes deltas through unchanged.

  Args:
    graph - OperatorGraph
    operator-id - ID of current operator
    output-deltas - Deltas produced by current operator

  Returns: Map of downstream-operator-id -> deltas"
  [graph operator-id output-deltas]
  (let [downstream-specs (get-in graph [:edges operator-id] [])]
    (reduce (fn [acc spec]
              (let [normalized (normalize-downstream-spec spec)
                    target-id (:to normalized)
                    input-port (:input-port normalized)
                    ;; Tag deltas if this is a multi-input connection
                    tagged-deltas (if input-port
                                    (mapv #(delta/tag-delta % input-port) output-deltas)
                                    output-deltas)]
                ;; If target already has deltas, append; otherwise create new entry
                (update acc target-id (fnil into []) tagged-deltas)))
            {}
            downstream-specs)))

(defn propagate-deltas
  "Propagate deltas through the operator graph using topological ordering.

  This performs a breadth-first traversal of the graph, processing operators
  level by level to ensure deltas flow correctly through the pipeline.

  For multi-input operators, deltas from each input are processed separately,
  allowing the operator to maintain state across multiple inputs.

  Args:
    graph - OperatorGraph
    input-deltas - Initial deltas to inject at root operators (ignored for REL operators)

  Returns: Map of leaf-operator-id -> final output deltas"
  [graph input-deltas]
  (loop [;; Queue of (operator-id, deltas) pairs to process
         ;; For each root, get appropriate initial deltas (REL generates its own)
         queue (for [root-id (:root-operators graph)
                     :let [operator (get-in graph [:operators root-id])
                           deltas (get-initial-deltas-for-operator operator input-deltas)]]
                 [root-id deltas])
         ;; Track outputs from leaf operators
         ;; For multi-input, we need to accumulate outputs across multiple processings
         leaf-outputs {}]
    (if (empty? queue)
      ;; Done - return leaf outputs
      leaf-outputs

      ;; Process next operator
      (let [[operator-id deltas] (first queue)
            remaining (rest queue)
            ;; Process operator with deltas (no visited check - multi-input needs multiple passes)
            operator (get-in graph [:operators operator-id])
            ;; Process each delta through operator
            output-deltas (mapcat #(propagate-through-operator operator %) deltas)
            ;; Propagate to downstream
            downstream-map (propagate-to-downstream graph operator-id output-deltas)
            ;; Add downstream work to queue
            new-queue (concat remaining
                              (for [[down-id down-deltas] downstream-map]
                                [down-id down-deltas]))
            ;; Update leaf outputs if this is a leaf
            ;; Accumulate outputs across multiple processing
            new-leaf-outputs (if (contains? (:leaf-operators graph) operator-id)
                               (update leaf-outputs operator-id (fnil into []) output-deltas)
                               leaf-outputs)]
        (recur new-queue new-leaf-outputs)))))

;;; Simple Linear Pipeline Helper

(defn create-linear-pipeline
  "Create a linear operator pipeline (common case).

  Args:
    operators - Sequence of operators in pipeline order

  Returns: OperatorGraph"
  [operators]
  (let [ids (range (count operators))
        operator-map (zipmap ids operators)
        edges (reduce (fn [acc [i next-i]]
                        (assoc acc i [next-i]))
                      {}
                      (partition 2 1 ids))]
    (create-operator-graph operator-map edges)))

(defn execute-linear-pipeline
  "Execute a linear pipeline with input deltas.

  Args:
    pipeline - Sequence of operators
    deltas - Input deltas

  Returns: Final output deltas from last operator"
  [pipeline deltas]
  (let [graph (create-linear-pipeline pipeline)
        leaf-outputs (propagate-deltas graph deltas)
        ;; Get output from last operator (highest ID)
        last-id (dec (count pipeline))]
    (get leaf-outputs last-id [])))

;;; State Management

(defn reset-all-operator-states!
  "Reset all operators in graph to initial state.

  Args:
    graph - OperatorGraph

  Returns: nil"
  [graph]
  (doseq [[_ operator] (:operators graph)]
    (ops/reset-state! operator))
  nil)

;;; Nested Graph Execution
;;
;; Nested graphs are used for subquery operations (EXISTS, PULL, PULL*).
;; They execute independently with their own operator graph, taking input
;; from the parent graph and returning results.

(defrecord NestedGraph
           [graph          ; OperatorGraph instance
            parent-context ; Context from parent graph (for variable binding)
            nesting-depth  ; Current nesting depth (for validation)
            max-depth])    ; Maximum allowed nesting depth

(defn validate-nesting-depth
  "Validate that nesting depth doesn't exceed maximum.

  Args:
    current-depth - Current nesting depth
    max-depth - Maximum allowed depth (default 3)

  Throws: ex-info if depth exceeded
  Returns: nil if valid"
  [current-depth max-depth]
  (when (> current-depth max-depth)
    (throw (ex-info "Nested query depth exceeds limit"
                    {:max-depth max-depth
                     :current-depth current-depth
                     :message (format "Nested subquery depth %d exceeds maximum %d"
                                      current-depth max-depth)}))))

(defn create-nested-graph
  "Create a nested dataflow graph for a subquery expression.

  Nested graphs are used by EXISTS, PULL, and PULL* operators to execute
  subqueries. They maintain their own operator graph and state, isolated
  from the parent graph.

  Args:
    subquery-graph - OperatorGraph for the subquery
    parent-context - Context map from parent graph (optional)
                     Can include variable bindings, metadata, etc.
    options - Optional map with:
              :nesting-depth - Current depth (default 1)
              :max-depth - Maximum allowed depth (default 3)

  Returns: NestedGraph instance

  Throws: ex-info if nesting depth exceeds limit"
  ([subquery-graph]
   (create-nested-graph subquery-graph {} {}))
  ([subquery-graph parent-context]
   (create-nested-graph subquery-graph parent-context {}))
  ([subquery-graph parent-context options]
   (let [nesting-depth (get options :nesting-depth 1)
         max-depth (get options :max-depth 3)]
     ;; Validate depth before creating
     (validate-nesting-depth nesting-depth max-depth)
     (->NestedGraph subquery-graph parent-context nesting-depth max-depth))))

(defn execute-nested-graph
  "Execute a nested graph with input document and return results.

  This is the core execution function for subqueries. It:
  1. Creates input deltas from the parent document
  2. Propagates deltas through the nested graph
  3. Collects and returns results from leaf operators

  Args:
    nested-graph - NestedGraph instance
    input-doc - Document from parent graph to feed as input
                Can be nil for subqueries with no input context

  Returns: Vector of result documents from leaf operators
           Empty vector if no results"
  [nested-graph input-doc]
  (let [graph (:graph nested-graph)
        ;; Create input delta from parent document
        ;; If input-doc is nil, propagate with empty deltas (for standalone subqueries)
        input-deltas (if input-doc
                       [(delta/add-delta input-doc)]
                       [])
        ;; Propagate through nested graph
        leaf-outputs (propagate-deltas graph input-deltas)
        ;; Collect all results from leaf operators
        all-results (mapcat second leaf-outputs)]
    ;; Extract documents from output deltas
    (vec (map :doc all-results))))

(defn reset-nested-graph!
  "Reset all operators in a nested graph to initial state.

  Args:
    nested-graph - NestedGraph instance

  Returns: nil"
  [nested-graph]
  (reset-all-operator-states! (:graph nested-graph))
  nil)

(comment
  ;; Usage examples

  ;; Linear pipeline
  (def pipeline
    [(ops/create-from-operator :prod_attestations [:predicate_type])
     (ops/create-where-operator [:like :predicate_type "%slsa%"])
     (ops/create-aggregate-operator :predicate_type {:count [:row-count]})])

  (def input-deltas
    [(delta/add-delta {:xt/id "1" :xt/table :prod_attestations :predicate_type "slsa-v1"})
     (delta/add-delta {:xt/id "2" :xt/table :prod_attestations :predicate_type "slsa-v1"})])

  (def results (execute-linear-pipeline pipeline input-deltas))
  ;; => deltas with {:predicate_type "slsa-v1" :count 2}
  )
