(ns xtflow.fuzzing.schemas
  "Table schemas for fuzzing test data generation.

  Schemas define the structure and types of fields for each table,
  enabling type-safe query and data generation.")

(def table-schemas
  "Schema definitions for test tables.

  Each schema maps field names to type specifications:
  - :type - The field type (:string, :int, :double, :boolean, :uuid, :map, :array)
  - :nested - For :map types, the nested schema
  - :items - For :array types, the item schema
  - :required? - Whether the field must be present (default true)
  - :values - For enum types, the allowed values"

  {:prod_attestations
   {:xt/id {:type :string :required? true}
    :predicate_type {:type :enum
                     :values ["slsa-provenance-v1" "cyclonedx-sbom-v1.4" "policy-ticket-v1"]
                     :required? true}
    :predicate {:type :map
                :required? false
                :nested {:bomFormat {:type :string}
                         :specVersion {:type :string}
                         :serialNumber {:type :string}
                         :builder {:type :map
                                   :nested {:id {:type :string}}}
                         :components {:type :array
                                      :items {:type :map
                                              :nested {:type {:type :string}
                                                       :name {:type :string}
                                                       :version {:type :string}
                                                       :licenses {:type :array
                                                                  :items {:type :map
                                                                          :nested {:license {:type :map
                                                                                             :nested {:id {:type :string}
                                                                                                      :name {:type :string}
                                                                                                      :url {:type :string}}}}}}}}}}}
    :subjects {:type :array
               :required? false
               :items {:type :map
                       :nested {:name {:type :string}
                                :digest {:type :map
                                         :nested {:sha256 {:type :string}}}}}}}

   :users
   {:xt/id {:type :string :required? true}
    :user-id {:type :int :required? true}
    :name {:type :string :required? true}
    :tier {:type :enum :values ["free" "basic" "premium"] :required? true}
    :region {:type :enum :values ["us-east" "us-west" "eu-west" "ap-south"] :required? false}
    :org {:type :string :required? false}
    :score {:type :int :required? false}}

   :orders
   {:xt/id {:type :string :required? true}
    :order-id {:type :int :required? true}
    :user-id {:type :int :required? true}
    :amount {:type :double :required? true}
    :status {:type :enum :values ["pending" "completed" "cancelled"] :required? true}
    :timestamp {:type :int :required? true}}

   :permissions
   {:xt/id {:type :string :required? true}
    :user-id {:type :int :required? true}
    :org {:type :string :required? true}
    :role {:type :enum :values ["admin" "user" "viewer" "editor"] :required? true}
    :scope {:type :enum :values ["read" "write" "delete" "all"] :required? true}}})

(defn get-table-schema
  "Get schema for a table."
  [table]
  (get table-schemas table))

(defn get-field-type
  "Get type of a field in a table."
  [table field]
  (get-in table-schemas [table field :type]))

(defn get-field-spec
  "Get full specification of a field in a table."
  [table field]
  (get-in table-schemas [table field]))

(defn get-nested-path-type
  "Get type at a nested path in a table schema.

  Example:
    (get-nested-path-type :prod_attestations [:predicate :builder :id])
    => :string"
  [table path]
  (loop [schema (get-table-schema table)
         remaining-path path]
    (if (empty? remaining-path)
      (:type schema)
      (let [field (first remaining-path)
            field-spec (get schema field)]
        (cond
          ;; Terminal field
          (and field-spec (empty? (rest remaining-path)))
          (:type field-spec)

          ;; Map with nested schema
          (and (= (:type field-spec) :map) (:nested field-spec))
          (recur (:nested field-spec) (rest remaining-path))

          ;; Array with item schema
          (and (= (:type field-spec) :array) (:items field-spec))
          (if (number? (second remaining-path))
            ;; Array index access, navigate into items
            (recur (:nested (:items field-spec)) (drop 2 remaining-path))
            ;; Array itself
            :array)

          ;; Can't navigate further
          :else nil)))))

(defn get-fields-of-type
  "Get all top-level fields of a given type in a table.

  Example:
    (get-fields-of-type :users :string)
    => #{:xt/id :name :org}"
  [table field-type]
  (let [schema (get-table-schema table)]
    (->> schema
         (filter (fn [[_ spec]] (= (:type spec) field-type)))
         (map first)
         set)))

(defn get-array-fields
  "Get all top-level array fields in a table."
  [table]
  (get-fields-of-type table :array))

(defn get-numeric-fields
  "Get all numeric fields (int or double) in a table."
  [table]
  (let [schema (get-table-schema table)]
    (->> schema
         (filter (fn [[_ spec]] (#{:int :double} (:type spec))))
         (map first)
         set)))

(defn get-required-fields
  "Get all required fields in a table."
  [table]
  (let [schema (get-table-schema table)]
    (->> schema
         (filter (fn [[_ spec]] (not= false (:required? spec))))
         (map first)
         set)))

(defn get-all-tables
  "Get list of all tables."
  []
  (keys table-schemas))
