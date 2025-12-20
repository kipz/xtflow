(ns xtflow.compiler
  "Expression compiler for high-performance evaluation.

  Compiles XTQL expressions to native Clojure functions at operator creation time,
  eliminating the overhead of interpreting expressions for every document.

  Example:
    (def compiled-fn (compile-expr [:> :age 18]))
    (compiled-fn {:age 25})  ; => true (direct execution, no interpretation)"
  (:require [clojure.string :as str]))

;;; Feature Flag

(def ^:dynamic *use-compiled-exprs*
  "Feature flag to enable/disable expression compilation.
  Default: true for production use.
  Set to false for debugging or if compilation issues arise."
  true)

;;; Compilation Core

(defn compile-expr
  "Compile an expression to a native Clojure function.

  Takes an XTQL expression and returns a function (fn [doc] ...) that
  executes the expression directly without interpretation overhead.

  Supports a subset of XTQL operators initially, with gradual expansion.
  Falls back to interpreted evaluation for unsupported operators.

  Args:
    expr - XTQL expression to compile

  Returns: Function (fn [doc] ...) that evaluates the expression"
  [expr]
  (cond
    ;; Literal values - constant function
    (or (string? expr) (number? expr) (boolean? expr) (nil? expr))
    (constantly expr)

    ;; Field access (keyword) - direct get
    (keyword? expr)
    (fn [doc] (get doc expr))

    ;; Field access (symbol - convert to keyword)
    (symbol? expr)
    (let [kw (keyword expr)]
      (fn [doc] (get doc kw)))

    ;; List - convert to vector first
    (list? expr)
    (let [op (first expr)
          op-kw (if (symbol? op) (keyword op) op)
          rest-expr (rest expr)]
      (compile-expr (vec (cons op-kw rest-expr))))

    ;; Vector - operator expression
    (vector? expr)
    (let [op (first expr)]
      (case op
        ;; Nested field access
        :..
        (let [path (mapv #(if (symbol? %) (keyword %) %) (rest expr))]
          (fn [doc]
            (reduce (fn [m k]
                      (cond
                        (nil? m) nil
                        (number? k) (if (and (sequential? m) (< k (count m)))
                                      (nth m k) nil)
                        (map? m) (or (get m k)
                                     (when (keyword? k) (get m (name k)))
                                     (when (string? k) (get m (keyword k)))
                                     nil)
                        :else nil))
                    doc path)))

        ;; Comparison operators - compile to native comparisons
        :=
        (let [[_ field value] expr
              field-fn (compile-expr field)
              value-fn (compile-expr value)]
          (fn [doc]
            (= (field-fn doc) (value-fn doc))))

        :>
        (let [[_ field value] expr
              field-fn (compile-expr field)
              value-fn (compile-expr value)]
          (fn [doc]
            (let [field-val (field-fn doc)
                  comp-val (value-fn doc)]
              (and field-val comp-val
                   (> (double field-val) (double comp-val))))))

        :<
        (let [[_ field value] expr
              field-fn (compile-expr field)
              value-fn (compile-expr value)]
          (fn [doc]
            (let [field-val (field-fn doc)
                  comp-val (value-fn doc)]
              (and field-val comp-val
                   (< (double field-val) (double comp-val))))))

        :>=
        (let [[_ field value] expr
              field-fn (compile-expr field)
              value-fn (compile-expr value)]
          (fn [doc]
            (let [field-val (field-fn doc)
                  comp-val (value-fn doc)]
              (and field-val comp-val
                   (>= (double field-val) (double comp-val))))))

        :<=
        (let [[_ field value] expr
              field-fn (compile-expr field)
              value-fn (compile-expr value)]
          (fn [doc]
            (let [field-val (field-fn doc)
                  comp-val (value-fn doc)]
              (and field-val comp-val
                   (<= (double field-val) (double comp-val))))))

        :!=
        (let [[_ field value] expr
              field-fn (compile-expr field)
              value-fn (compile-expr value)]
          (fn [doc]
            (not= (field-fn doc) (value-fn doc))))

        :<>
        (let [[_ field value] expr
              field-fn (compile-expr field)
              value-fn (compile-expr value)]
          (fn [doc]
            (not= (field-fn doc) (value-fn doc))))

        ;; Arithmetic operators - compile to native operations
        :+
        (let [arg-fns (mapv compile-expr (rest expr))]
          (fn [doc]
            (apply + (map #(% doc) arg-fns))))

        :-
        (let [arg-fns (mapv compile-expr (rest expr))]
          (fn [doc]
            (apply - (map #(% doc) arg-fns))))

        :*
        (let [arg-fns (mapv compile-expr (rest expr))]
          (fn [doc]
            (apply * (map #(% doc) arg-fns))))

        :/
        (let [arg-fns (mapv compile-expr (rest expr))]
          (fn [doc]
            (apply / (map #(% doc) arg-fns))))

        ;; Logic operators
        :and
        (let [arg-fns (mapv compile-expr (rest expr))]
          (fn [doc]
            (every? #(% doc) arg-fns)))

        :or
        (let [arg-fns (mapv compile-expr (rest expr))]
          (fn [doc]
            (some #(% doc) arg-fns)))

        :not
        (let [[_ arg] expr
              arg-fn (compile-expr arg)]
          (fn [doc]
            (not (arg-fn doc))))

        ;; Math operators
        :mod
        (let [[_ dividend divisor] expr
              dividend-fn (compile-expr dividend)
              divisor-fn (compile-expr divisor)]
          (fn [doc]
            (mod (long (dividend-fn doc)) (long (divisor-fn doc)))))

        :abs
        (let [[_ field] expr
              field-fn (compile-expr field)]
          (fn [doc]
            (let [v (field-fn doc)]
              (when v (Math/abs (double v))))))

        :floor
        (let [[_ field] expr
              field-fn (compile-expr field)]
          (fn [doc]
            (let [v (field-fn doc)]
              (when v (Math/floor (double v))))))

        :ceil
        (let [[_ field] expr
              field-fn (compile-expr field)]
          (fn [doc]
            (let [v (field-fn doc)]
              (when v (Math/ceil (double v))))))

        :round
        (let [[_ field] expr
              field-fn (compile-expr field)]
          (fn [doc]
            (let [v (field-fn doc)]
              (when v (Math/round (double v))))))

        :sqrt
        (let [[_ field] expr
              field-fn (compile-expr field)]
          (fn [doc]
            (let [v (field-fn doc)]
              (when v (Math/sqrt (double v))))))

        :pow
        (let [[_ base exponent] expr
              base-fn (compile-expr base)
              exp-fn (compile-expr exponent)]
          (fn [doc]
            (let [b (base-fn doc)
                  e (exp-fn doc)]
              (when (and b e)
                (Math/pow (double b) (double e))))))

        ;; Control flow
        :if
        (let [[_ condition then-expr else-expr] expr
              cond-fn (compile-expr condition)
              then-fn (compile-expr then-expr)
              else-fn (compile-expr else-expr)]
          (fn [doc]
            (if (cond-fn doc)
              (then-fn doc)
              (else-fn doc))))

        ;; Predicates
        :nil?
        (let [[_ field] expr
              field-fn (compile-expr field)]
          (fn [doc]
            (nil? (field-fn doc))))

        :true?
        (let [[_ field] expr
              field-fn (compile-expr field)]
          (fn [doc]
            (true? (field-fn doc))))

        :false?
        (let [[_ field] expr
              field-fn (compile-expr field)]
          (fn [doc]
            (false? (field-fn doc))))

        ;; String operations
        :like
        (let [[_ field pattern] expr
              field-fn (compile-expr field)
              pattern-str (str pattern)
              regex-str (-> pattern-str
                            (str/replace "%" ".*")
                            (str/replace "_" "."))
              regex (re-pattern regex-str)]
          (fn [doc]
            (boolean (re-matches regex (str (field-fn doc))))))

        :trim
        (let [[_ field] expr
              field-fn (compile-expr field)]
          (fn [doc]
            (let [v (field-fn doc)]
              (when v (str/trim (str v))))))

        :lower
        (let [[_ field] expr
              field-fn (compile-expr field)]
          (fn [doc]
            (let [v (field-fn doc)]
              (when v (str/lower-case (str v))))))

        :upper
        (let [[_ field] expr
              field-fn (compile-expr field)]
          (fn [doc]
            (let [v (field-fn doc)]
              (when v (str/upper-case (str v))))))

        ;; Fallback: throw exception for unsupported operators
        ;; This will be caught and we'll fall back to interpreted evaluation
        (throw (ex-info "Unsupported operator in compilation"
                        {:operator op :expr expr}))))

    ;; Unknown expression type - throw exception
    :else
    (throw (ex-info "Cannot compile expression" {:expr expr}))))

(defn compile-expr-safe
  "Safely compile an expression with fallback to nil on compilation errors.

  Args:
    expr - Expression to compile

  Returns: Compiled function or nil if compilation fails"
  [expr]
  (try
    (compile-expr expr)
    (catch Exception _
      ;; Compilation failed - return nil to signal fallback needed
      nil)))
