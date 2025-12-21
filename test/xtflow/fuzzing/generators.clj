(ns xtflow.fuzzing.generators
  "Generators for fuzzing tests: documents, queries, and transactions.

  This namespace provides test.check generators for:
  - Documents conforming to table schemas
  - Valid XTQL queries
  - Transaction sequences"
  (:require [clojure.test.check.generators :as gen]
            [clojure.set :as set]
            [xtflow.fuzzing.schemas :as schemas]))

;;; ============================================================================
;;; Basic Value Generators
;;; ============================================================================

(def gen-uuid
  "Generate random UUID string."
  (gen/fmap (fn [_] (str (java.util.UUID/randomUUID))) (gen/return nil)))

(def gen-string
  "Generate random string (alphanumeric, 1-20 chars)."
  (gen/such-that not-empty gen/string-alphanumeric))

(def gen-small-string
  "Generate short string (1-10 chars)."
  (gen/fmap #(apply str (take 10 %)) gen-string))

(def gen-int
  "Generate random integer (0-1000)."
  (gen/choose 0 1000))

(def gen-small-int
  "Generate small integer (0-100)."
  (gen/choose 0 100))

(def gen-double
  "Generate random double (0.0-1000.0)."
  (gen/double* {:min 0.0 :max 1000.0 :infinite? false :NaN? false}))

(def gen-boolean
  "Generate random boolean."
  gen/boolean)

;;; ============================================================================
;;; Schema-Based Document Generation
;;; ============================================================================

(declare gen-value-for-type)

(defn gen-value-for-field-spec
  "Generate value conforming to a field specification."
  [field-spec]
  (let [required? (not= false (:required? field-spec))]
    (if required?
      (gen-value-for-type (:type field-spec) field-spec)
      (gen/one-of [(gen/return nil)
                   (gen-value-for-type (:type field-spec) field-spec)]))))

(defn gen-value-for-type
  "Generate value for a given type specification."
  [type-kw type-spec]
  (case type-kw
    :string gen-string
    :int gen-int
    :double gen-double
    :boolean gen-boolean
    :uuid gen-uuid

    :enum (gen/elements (:values type-spec))

    :map
    (let [nested-schema (:nested type-spec)]
      (gen/let [entries (gen/vector
                         (gen/tuple
                          (gen/elements (keys nested-schema))
                          (gen/return nil))
                         0 (count nested-schema))]
        (into {}
              (for [[k _] entries
                    :let [field-spec (get nested-schema k)]
                    :when field-spec]
                [k (gen/generate (gen-value-for-field-spec field-spec))]))))

    :array
    (let [item-schema (:items type-spec)]
      (gen/vector (gen-value-for-type (:type item-schema) item-schema) 0 10))

    ;; Default: string
    gen-string))

(defn gen-document-for-table
  "Generate a document conforming to a table schema.

  Options:
    :require-all-fields? - Generate all fields including optional ones (default false)"
  ([table] (gen-document-for-table table {}))
  ([table opts]
   (let [schema (schemas/get-table-schema table)
         require-all? (:require-all-fields? opts false)]
     (gen/let [field-values (gen/list
                             (gen/tuple
                              (gen/elements (keys schema))
                              (gen/return nil))
                             (count schema))]
       (into {}
             (for [[field-kw _] field-values
                   :let [field-spec (get schema field-kw)]
                   :when field-spec
                   :let [required? (or require-all? (not= false (:required? field-spec)))]
                   :when required?]
               [field-kw (gen/generate (gen-value-for-field-spec field-spec))]))))))

;;; ============================================================================
;;; Specific Table Generators (leveraging existing patterns)
;;; ============================================================================

(def gen-sbom-component
  "Generate realistic SBOM component."
  (gen/let [idx gen-int
            license-id (gen/elements ["MIT" "Apache-2.0" "GPL-3.0" "BSD-3-Clause"
                                      "ISC" "MPL-2.0" "LGPL-3.0" "EPL-2.0"])]
    {:type (gen/generate (gen/elements ["library" "application" "framework"]))
     :name (str "github.com/org" (mod idx 1000) "/pkg" idx)
     :version (str "v" (gen/generate (gen/choose 0 10)) "."
                   (gen/generate (gen/choose 0 20)) "."
                   (gen/generate (gen/choose 0 100)))
     :licenses [{:license {:id license-id
                           :name license-id
                           :url (str "https://spdx.org/licenses/" license-id)}}]}))

(defn gen-sbom
  "Generate complete CycloneDX SBOM."
  [id n-components]
  (gen/let [components (gen/vector gen-sbom-component n-components n-components)]
    {:xt/id id
     :predicate_type "cyclonedx-sbom-v1.4"
     :predicate {:bomFormat "CycloneDX"
                 :specVersion "1.4"
                 :serialNumber (gen/generate gen-uuid)
                 :components components}
     :subjects [{:name (str "gcr.io/project/image-" id)
                 :digest {:sha256 (gen/generate gen-uuid)}}]}))

(defn gen-user
  "Generate user record (returns a generator)."
  [id]
  (gen/let [tier (gen/elements ["free" "basic" "premium"])
            region (gen/elements ["us-east" "us-west" "eu-west" "ap-south"])
            score (gen/choose 50 100)]
    {:xt/id (str "user-" id)
     :user-id id
     :name (str "User-" id)
     :tier tier
     :region region
     :score score}))

(defn make-user
  "Create a simple user record with deterministic values (for testing)."
  [id]
  {:xt/id (str "user-" id)
   :user-id id
   :name (str "User-" id)
   :tier (["free" "basic" "premium"] (mod id 3))
   :region "us-east"
   :score (+ 50 (mod id 50))})

(defn gen-order
  "Generate order record."
  [id user-id]
  (gen/let [amount (gen/fmap #(* 10.0 %) (gen/choose 1 100))
            status (gen/elements ["pending" "completed" "cancelled"])
            timestamp (gen/return (System/currentTimeMillis))]
    {:xt/id (str "order-" id)
     :order-id id
     :user-id user-id
     :amount amount
     :status status
     :timestamp timestamp}))

(defn gen-permission
  "Generate permission record."
  [user-id org]
  (gen/let [role (gen/elements ["admin" "user" "viewer" "editor"])
            scope (gen/elements ["read" "write" "delete" "all"])]
    {:xt/id (str "perm-" user-id "-" org)
     :user-id user-id
     :org org
     :role role
     :scope scope}))

;;; ============================================================================
;;; Bulk Data Generation
;;; ============================================================================

(defn gen-bulk-documents
  "Generate N documents for a table."
  [table n]
  (gen/vector (gen-document-for-table table) n n))

(defn gen-initial-dataset
  "Generate initial dataset across multiple tables.

  Returns a map of table -> [documents]

  NOTE: Uses sequential IDs to ensure uniqueness and avoid XTDB upsert collisions."
  [{:keys [n-sboms n-users n-orders n-permissions]
    :or {n-sboms 10 n-users 20 n-orders 50 n-permissions 30}}]
  (gen/let [sboms (gen/vector
                   (gen/let [id gen-uuid
                             n-components (gen/choose 5 20)]
                     (gen-sbom id n-components))
                   n-sboms)
            ;; Generate users with sequential unique IDs (0, 1, 2, ..., n-users-1)
            users (gen/return (mapv make-user (range n-users)))
            ;; Generate orders with sequential unique IDs
            orders (gen/vector
                    (gen/let [user-id (if (pos? n-users)
                                        (gen/choose 0 (dec n-users))
                                        (gen/return 0))]
                      (gen/return {:xt/id (str "order-" (gensym))
                                   :order-id (rand-int 10000)
                                   :user-id user-id
                                   :amount (* 10.0 (inc (rand-int 100)))
                                   :status (rand-nth ["pending" "completed" "cancelled"])
                                   :timestamp (System/currentTimeMillis)}))
                    n-orders)
            ;; Generate permissions with user IDs from valid range
            ;; Use gensym to ensure unique IDs even if user-id/org combinations repeat
            permissions (gen/vector
                         (gen/let [user-id (if (pos? n-users)
                                             (gen/choose 0 (dec n-users))
                                             (gen/return 0))
                                   org gen-small-string
                                   role (gen/elements ["admin" "user" "viewer" "editor"])
                                   scope (gen/elements ["read" "write" "delete" "all"])]
                           (gen/return {:xt/id (str "perm-" user-id "-" org "-" (gensym))
                                        :user-id user-id
                                        :org org
                                        :role role
                                        :scope scope}))
                         n-permissions)]
    {:prod_attestations sboms
     :users users
     :orders orders
     :permissions permissions}))

;;; ============================================================================
;;; Transaction Generation (Phase 4 - placeholder for now)
;;; ============================================================================

(defn gen-transaction-op
  "Generate a single transaction operation (:put, :delete, or :update).

  This is a placeholder - will be fully implemented in Phase 4."
  [table existing-ids]
  (gen/frequency
   [[7 (gen/let [doc (gen-document-for-table table)]
         [:put table doc])]
    [2 (if (seq existing-ids)
         (gen/let [id (gen/elements (vec existing-ids))]
           [:delete table id])
         (gen/return [:noop]))]
    [1 (if (seq existing-ids)
         (gen/let [id (gen/elements (vec existing-ids))
                   doc (gen-document-for-table table)]
           [:put table (assoc doc :xt/id id)])
         (gen/return [:noop]))]]))

(defn gen-transaction-sequence
  "Generate sequence of transaction operations.

  This is a placeholder - will be fully implemented in Phase 4."
  [table n-ops]
  (gen/vector (gen-transaction-op table #{}) n-ops))

;;; ============================================================================
;;; Query Generation (Phase 2)
;;; ============================================================================

;;; Query Context Tracking

(defrecord QueryContext
           [table               ; Current table
            available-fields    ; Set of available field keywords
            field-types         ; Map of field -> type
            array-fields        ; Set of fields that are arrays
            complexity          ; Current operator count
            has-aggregated?     ; Boolean - has AGGREGATE been used
            operators])         ; Vector of operators so far

(defn make-initial-context
  "Create initial query context from a table."
  [table]
  (let [schema (schemas/get-table-schema table)
        fields (set (keys schema))
        field-types (into {} (for [[k v] schema] [k (:type v)]))
        array-fields (schemas/get-array-fields table)]
    (->QueryContext table fields field-types array-fields 0 false [])))

(defn update-context-with-operator
  "Update context after adding an operator."
  [context operator]
  (case (:op operator)
    :from
    ;; FROM resets available-fields to only the projected fields
    (assoc context :available-fields (set (:fields operator)))

    :where context

    :with
    (let [new-fields (set (keys (:fields operator)))]
      (-> context
          (update :available-fields set/union new-fields)
          (update :complexity inc)))

    :without
    (let [removed-fields (set (:fields operator))]
      (-> context
          (update :available-fields set/difference removed-fields)
          (update :complexity inc)))

    :return
    (let [returned-fields (set (:fields operator))]
      (-> context
          (assoc :available-fields returned-fields)
          (update :complexity inc)))

    :unnest
    (let [binding-var (first (keys (:binding operator)))]
      (-> context
          (update :available-fields conj binding-var)
          (update :complexity inc)))

    :aggregate
    (-> context
        (assoc :available-fields (set (keys (:aggregates operator))))
        (assoc :has-aggregated? true)
        (update :complexity inc))

    :limit
    (update context :complexity inc)

    :offset
    (update context :complexity inc)

    ;; Default
    (update context :complexity inc)))

;;; Expression Generation

(defn gen-comparison-expr
  "Generate comparison expression for a field."
  [field field-type table]
  (case field-type
    (:int :double)
    (gen/let [op (gen/elements [:= :> :< :>= :<=])
              value (if (= field-type :int) gen-int gen-double)]
      [op field value])

    :string
    (gen/frequency
     [[3 (gen/let [value gen-string]
           [:= field value])]
      [1 (gen/let [pattern gen-small-string]
           [:like field (str "%" pattern "%")])]])

    :boolean
    (gen/let [value gen-boolean]
      [:= field value])

    :enum
    (let [field-spec (schemas/get-field-spec table field)
          values (:values field-spec)]
      (if (seq values)
        (gen/let [value (gen/elements values)]
          [:= field value])
        ;; No enum values defined, use nil
        (gen/return [:= field nil])))

    ;; Default: equality
    (gen/let [value gen-string]
      [:= field value])))

(defn gen-predicate-expr
  "Generate predicate expression for WHERE clause."
  [context]
  (let [comparable-fields (filter #(#{:int :double :string :boolean :enum}
                                    (get (:field-types context) %))
                                  (:available-fields context))]
    (if (seq comparable-fields)
      (gen/let [field (gen/elements (vec comparable-fields))
                field-type (gen/return (get (:field-types context) field))
                expr (gen-comparison-expr field field-type (:table context))]
        expr)
      ;; No comparable fields, return always-true predicate
      (gen/return [:= :xt/id :xt/id]))))

;;; Operator Generators

(defn gen-from-operator
  "Generate FROM operator."
  [table]
  (let [schema (schemas/get-table-schema table)
        all-fields (vec (keys schema))]
    (gen/let [field-subset (gen/elements
                            [all-fields  ; All fields
                             (vec (take 3 all-fields))  ; First 3
                             [:xt/id]])]  ; Just ID
      {:op :from
       :table table
       :fields field-subset})))

(defn gen-where-operator
  "Generate WHERE operator."
  [context]
  (gen/let [pred (gen-predicate-expr context)]
    {:op :where
     :predicate pred}))

(defn gen-with-operator
  "Generate WITH operator (add computed fields)."
  [context]
  (gen/let [field-name (gen/fmap keyword gen-small-string)
            source-field (gen/elements (vec (:available-fields context)))]
    {:op :with
     :fields {field-name source-field}}))

(defn gen-without-operator
  "Generate WITHOUT operator (remove fields)."
  [context]
  (let [removable-fields (disj (:available-fields context) :xt/id)]
    (if (seq removable-fields)
      (gen/let [fields-to-remove (gen/vector (gen/elements (vec removable-fields)) 1 2)]
        {:op :without
         :fields (vec fields-to-remove)})
      (gen/return nil))))

(defn gen-unnest-operator
  "Generate UNNEST operator (expand array field)."
  [context]
  (let [array-fields (:array-fields context)]
    (if (seq array-fields)
      (gen/let [array-field (gen/elements (vec array-fields))
                var-name (gen/fmap keyword gen-small-string)]
        {:op :unnest
         :binding {var-name array-field}})
      (gen/return nil))))

(defn gen-aggregate-operator
  "Generate AGGREGATE operator."
  [context]
  (let [groupable-fields (filter #(#{:string :int :enum}
                                   (get (:field-types context) %))
                                 (:available-fields context))
        ;; FIXED: Only use numeric fields that are in available-fields
        all-numeric-fields (set (schemas/get-numeric-fields (:table context)))
        available-numeric-fields (vec (filter all-numeric-fields (:available-fields context)))]
    (gen/let [group-by-field (if (seq groupable-fields)
                               (gen/elements (vec groupable-fields))
                               (gen/return nil))
              ;; Only generate numeric aggregates if there are numeric fields available
              agg-fn (if (seq available-numeric-fields)
                       (gen/elements [:row-count :sum :avg :min :max])
                       (gen/return :row-count))
              agg-field (if (and (#{:sum :avg :min :max} agg-fn)
                                 (seq available-numeric-fields))
                          (gen/elements available-numeric-fields)
                          (gen/return nil))]
      {:op :aggregate
       :group-by group-by-field
       :aggregates (if agg-field
                     {:count [agg-fn agg-field]}
                     {:count [:row-count]})})))

(defn gen-limit-operator
  "Generate LIMIT operator."
  []
  (gen/let [n (gen/choose 1 50)]
    {:op :limit
     :n n}))

(defn gen-offset-operator
  "Generate OFFSET operator."
  []
  (gen/let [n (gen/choose 0 20)]
    {:op :offset
     :n n}))

(defn gen-return-operator
  "Generate RETURN operator (project specific fields)."
  [context]
  (let [available (vec (:available-fields context))]
    (if (seq available)
      (gen/let [fields (gen/vector (gen/elements available) 1 (min 5 (count available)))]
        {:op :return
         :fields (vec (distinct fields))})
      (gen/return nil))))

;;; Operator Chain Generation

(defn can-add-operator?
  "Check if operator can be added given current context."
  [op-type context]
  (case op-type
    :where (not (:has-aggregated? context))
    :with (not (:has-aggregated? context))
    :without (and (not (:has-aggregated? context))
                  (> (count (:available-fields context)) 1))
    :unnest (and (not (:has-aggregated? context))
                 (seq (:array-fields context)))
    :aggregate (not (:has-aggregated? context))
    :limit true
    :offset true
    :return true
    true))

(defn gen-operator-for-context
  "Generate a valid operator for the current context."
  [context complexity-level]
  (let [available-ops (filterv #(can-add-operator? % context)
                               (case complexity-level
                                 :simple [:where :aggregate :limit]
                                 :medium [:where :with :without :aggregate :limit :offset]
                                 :complex [:where :with :without :unnest :aggregate :limit :offset :return]
                                 [:where :with :without :unnest :aggregate :limit :offset :return]))]
    (if (seq available-ops)
      (gen/let [op-type (gen/elements available-ops)
                operator (case op-type
                           :where (gen-where-operator context)
                           :with (gen-with-operator context)
                           :without (gen-without-operator context)
                           :unnest (gen-unnest-operator context)
                           :aggregate (gen-aggregate-operator context)
                           :limit (gen-limit-operator)
                           :offset (gen-offset-operator)
                           :return (gen-return-operator context)
                           (gen/return nil))]
        operator)
      (gen/return nil))))

(defn gen-operator-chain
  "Generate a chain of operators for a query."
  [initial-context max-operators complexity-level]
  (letfn [(gen-chain [context operators remaining]
            (if (<= remaining 0)
              (gen/return operators)
              (gen/bind (gen-operator-for-context context complexity-level)
                        (fn [operator]
                          (if (nil? operator)
                            (gen/return operators)
                            (let [new-context (update-context-with-operator context operator)
                                  new-operators (conj operators operator)]
                              (gen-chain new-context new-operators (dec remaining))))))))]
    (gen/bind (gen/choose 1 max-operators)
              (fn [n-operators]
                (gen-chain initial-context [] n-operators)))))

(defn gen-query
  "Generate a complete query with specified complexity.

  Complexity levels:
    :simple - FROM + WHERE + AGGREGATE (1-3 operators)
    :medium - + WITH/WITHOUT/LIMIT/OFFSET (3-6 operators)
    :complex - + UNNEST/RETURN (5-10 operators)"
  ([complexity-level]
   (gen-query complexity-level {}))
  ([complexity-level _opts]
   (let [max-ops (case complexity-level
                   :simple 3
                   :medium 6
                   :complex 10
                   5)]
     (gen/let [table (gen/elements (schemas/get-all-tables))
               from-op (gen-from-operator table)]
       (let [initial-context (-> (make-initial-context table)
                                 (update :operators conj from-op)
                                 ;; CRITICAL: Update available-fields based on FROM projection
                                 (update-context-with-operator from-op))]
         (gen/let [operator-chain (gen-operator-chain initial-context (dec max-ops) complexity-level)]
           {:table table
            :operators (vec (cons from-op operator-chain))}))))))

(defn gen-simple-query
  "Generate a simple query (FROM + WHERE/AGGREGATE)."
  []
  (gen-query :simple))

(defn gen-medium-query
  "Generate a medium complexity query."
  []
  (gen-query :medium))

(defn gen-complex-query
  "Generate a complex query."
  []
  (gen-query :complex))

(defn build-xtql-string
  "Build XTQL string from query spec.

  This converts our internal query representation to an XTQL string
  that can be parsed and executed."
  [query-spec]
  (let [operators (:operators query-spec)
        from-op (first operators)
        rest-ops (rest operators)
        ;; Convert :* to [*] for XTQL, otherwise use fields as vector
        from-fields (if (= (:fields from-op) :*)
                      "[*]"
                      (pr-str (vec (:fields from-op))))]
    (str "(-> "
         ;; FROM
         "(from " (:table from-op) " " from-fields ")"
         ;; Other operators
         (apply str
                (for [op rest-ops]
                  (case (:op op)
                    :where (str " (where " (pr-str (:predicate op)) ")")
                    :with (str " (with " (pr-str (:fields op)) ")")
                    :without (str " (without " (apply str (interpose " " (:fields op))) ")")
                    :unnest (str " (unnest " (pr-str (:binding op)) ")")
                    :aggregate (str " (aggregate "
                                    (when (:group-by op) (pr-str (:group-by op)))
                                    " " (pr-str (:aggregates op)) ")")
                    :limit (str " (limit " (:n op) ")")
                    :offset (str " (offset " (:n op) ")")
                    :return (str " (return " (if (= (:fields op) :*)
                                               "[*]"
                                               (pr-str (vec (:fields op)))) ")")
                    "")))
         ")")))

(comment
  ;; Example usage

  ;; Generate a user document
  (gen/generate (gen-user 1))

  ;; Generate an SBOM
  (gen/generate (gen-sbom "sbom-1" 10))

  ;; Generate initial dataset
  (gen/generate (gen-initial-dataset {}))

  ;; Generate bulk documents
  (gen/generate (gen-bulk-documents :users 5)))
