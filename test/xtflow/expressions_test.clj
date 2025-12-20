(ns xtflow.expressions-test
  "Comprehensive tests for XTDB standard library expression operators."
  (:require [clojure.test :refer [deftest testing is]]
            [xtflow.operators :as ops]))

;;; Comparison Operators

(deftest test-not-equal-operators
  (testing "!= operator"
    (let [doc {:age 25 :name "Alice"}]
      (is (true? (ops/eval-expr [:!= :age 30] doc)))
      (is (false? (ops/eval-expr [:!= :age 25] doc)))
      (is (true? (ops/eval-expr [:!= :name "Bob"] doc)))))

  (testing "<> operator (SQL-style not equal)"
    (let [doc {:age 25 :name "Alice"}]
      (is (true? (ops/eval-expr [:<> :age 30] doc)))
      (is (false? (ops/eval-expr [:<> :age 25] doc)))
      (is (true? (ops/eval-expr [:<> :name "Bob"] doc))))))

;;; Null and Boolean Predicates

(deftest test-nil-predicate
  (testing "nil? predicate"
    (let [doc {:name "Alice" :age 25 :email nil}]
      (is (false? (ops/eval-expr [:nil? :name] doc)))
      (is (false? (ops/eval-expr [:nil? :age] doc)))
      (is (true? (ops/eval-expr [:nil? :email] doc)))
      (is (true? (ops/eval-expr [:nil? :missing] doc))))))

(deftest test-true-false-predicates
  (testing "true? predicate"
    (let [doc {:active true :inactive false :maybe nil}]
      (is (true? (ops/eval-expr [:true? :active] doc)))
      (is (false? (ops/eval-expr [:true? :inactive] doc)))
      (is (false? (ops/eval-expr [:true? :maybe] doc)))))

  (testing "false? predicate"
    (let [doc {:active true :inactive false :maybe nil}]
      (is (false? (ops/eval-expr [:false? :active] doc)))
      (is (true? (ops/eval-expr [:false? :inactive] doc)))
      (is (false? (ops/eval-expr [:false? :maybe] doc))))))

;;; String Functions

(deftest test-like-regex
  (testing "like-regex pattern matching"
    (let [doc {:email "alice@example.com" :name "Alice"}]
      (is (true? (ops/eval-expr [:like-regex :email ".*@example\\.com"] doc)))
      (is (false? (ops/eval-expr [:like-regex :email ".*@other\\.com"] doc)))
      (is (true? (ops/eval-expr [:like-regex :name "^A.*"] doc)))
      (is (false? (ops/eval-expr [:like-regex :name "^B.*"] doc))))))

(deftest test-trim-functions
  (testing "trim function"
    (let [doc {:text "  hello world  "}]
      (is (= "hello world" (ops/eval-expr [:trim :text] doc)))))

  (testing "trim-leading function"
    (let [doc {:text "  hello world  "}]
      (is (= "hello world  " (ops/eval-expr [:trim-leading :text] doc)))))

  (testing "trim-trailing function"
    (let [doc {:text "  hello world  "}]
      (is (= "  hello world" (ops/eval-expr [:trim-trailing :text] doc)))))

  (testing "trim with nil value"
    (let [doc {:text nil}]
      (is (nil? (ops/eval-expr [:trim :text] doc))))))

(deftest test-overlay
  (testing "overlay string replacement"
    (let [doc {:text "Hello World"}]
      ;; Replace "World" starting at position 7 with length 5
      (is (= "Hello Clojure" (ops/eval-expr [:overlay :text "Clojure" 7 5] doc)))
      ;; Replace without explicit length
      (is (= "Hello Beautiful" (ops/eval-expr [:overlay :text "Beautiful" 7 5] doc))))))

;;; Arithmetic Operators

(deftest test-addition
  (testing "addition with two values"
    (let [doc {:a 10 :b 20}]
      (is (= 30 (ops/eval-expr [:+ :a :b] doc)))))

  (testing "addition with multiple values"
    (let [doc {:a 10 :b 20 :c 30}]
      (is (= 60 (ops/eval-expr [:+ :a :b :c] doc)))))

  (testing "addition with literals"
    (let [doc {:a 10}]
      (is (= 15 (ops/eval-expr [:+ :a 5] doc))))))

(deftest test-subtraction
  (testing "subtraction with two values"
    (let [doc {:a 30 :b 10}]
      (is (= 20 (ops/eval-expr [:- :a :b] doc)))))

  (testing "negation with single value"
    (let [doc {:a 10}]
      (is (= -10 (ops/eval-expr [:- :a] doc)))))

  (testing "subtraction with multiple values"
    (let [doc {:a 100 :b 20 :c 30}]
      (is (= 50 (ops/eval-expr [:- :a :b :c] doc))))))

(deftest test-multiplication
  (testing "multiplication with two values"
    (let [doc {:a 5 :b 4}]
      (is (= 20 (ops/eval-expr [:* :a :b] doc)))))

  (testing "multiplication with multiple values"
    (let [doc {:a 2 :b 3 :c 4}]
      (is (= 24 (ops/eval-expr [:* :a :b :c] doc)))))

  (testing "multiplication with literals"
    (let [doc {:a 7}]
      (is (= 35 (ops/eval-expr [:* :a 5] doc))))))

(deftest test-division
  (testing "division with two values"
    (let [doc {:a 20 :b 4}]
      (is (= 5 (ops/eval-expr [:/ :a :b] doc)))))

  (testing "division with multiple values"
    (let [doc {:a 100 :b 2 :c 5}]
      (is (= 10 (ops/eval-expr [:/ :a :b :c] doc)))))

  (testing "division with decimal result"
    (let [doc {:a 10 :b 3}]
      (is (< 3.33 (ops/eval-expr [:/ :a :b] doc) 3.34)))))

(deftest test-modulo
  (testing "modulo operation"
    (let [doc {:a 17 :b 5}]
      (is (= 2 (ops/eval-expr [:mod :a :b] doc))))

    (let [doc {:a 20 :b 4}]
      (is (= 0 (ops/eval-expr [:mod :a :b] doc))))))

;;; Math Functions

(deftest test-abs
  (testing "absolute value of positive number"
    (let [doc {:a 10}]
      (is (= 10.0 (ops/eval-expr [:abs :a] doc)))))

  (testing "absolute value of negative number"
    (let [doc {:a -10}]
      (is (= 10.0 (ops/eval-expr [:abs :a] doc)))))

  (testing "absolute value of zero"
    (let [doc {:a 0}]
      (is (= 0.0 (ops/eval-expr [:abs :a] doc))))))

(deftest test-floor
  (testing "floor of decimal number"
    (let [doc {:a 3.7}]
      (is (= 3.0 (ops/eval-expr [:floor :a] doc))))

    (let [doc {:a 3.2}]
      (is (= 3.0 (ops/eval-expr [:floor :a] doc))))

    (let [doc {:a -3.7}]
      (is (= -4.0 (ops/eval-expr [:floor :a] doc))))))

(deftest test-ceil
  (testing "ceiling of decimal number"
    (let [doc {:a 3.2}]
      (is (= 4.0 (ops/eval-expr [:ceil :a] doc))))

    (let [doc {:a 3.7}]
      (is (= 4.0 (ops/eval-expr [:ceil :a] doc))))

    (let [doc {:a -3.2}]
      (is (= -3.0 (ops/eval-expr [:ceil :a] doc))))))

(deftest test-round
  (testing "rounding of decimal numbers"
    (let [doc {:a 3.4}]
      (is (= 3 (ops/eval-expr [:round :a] doc))))

    (let [doc {:a 3.5}]
      (is (= 4 (ops/eval-expr [:round :a] doc))))

    (let [doc {:a 3.6}]
      (is (= 4 (ops/eval-expr [:round :a] doc))))))

(deftest test-sqrt
  (testing "square root"
    (let [doc {:a 16}]
      (is (= 4.0 (ops/eval-expr [:sqrt :a] doc))))

    (let [doc {:a 25}]
      (is (= 5.0 (ops/eval-expr [:sqrt :a] doc))))

    (let [doc {:a 2}]
      (is (< 1.414 (ops/eval-expr [:sqrt :a] doc) 1.415)))))

(deftest test-pow
  (testing "power function"
    (let [doc {:a 2 :b 3}]
      (is (= 8.0 (ops/eval-expr [:pow :a :b] doc))))

    (let [doc {:a 5 :b 2}]
      (is (= 25.0 (ops/eval-expr [:pow :a :b] doc))))

    (let [doc {:a 10 :b 0}]
      (is (= 1.0 (ops/eval-expr [:pow :a :b] doc))))))

;;; Control Structures

(deftest test-if-expression
  (testing "if with true condition"
    (let [doc {:age 25}]
      (is (= "adult" (ops/eval-expr [:if [:> :age 18] "adult" "minor"] doc)))))

  (testing "if with false condition"
    (let [doc {:age 15}]
      (is (= "minor" (ops/eval-expr [:if [:> :age 18] "adult" "minor"] doc)))))

  (testing "if with nested expressions"
    (let [doc {:score 85}]
      (is (= "high" (ops/eval-expr [:if [:>= :score 80] "high" "low"] doc))))))

(deftest test-case-expression
  (testing "case with first condition matching"
    (let [doc {:grade "A"}]
      (is (= "excellent" (ops/eval-expr [:case
                                         [:= :grade "A"] "excellent"
                                         [:= :grade "B"] "good"
                                         [:= :grade "C"] "average"
                                         "fail"] doc)))))

  (testing "case with second condition matching"
    (let [doc {:grade "B"}]
      (is (= "good" (ops/eval-expr [:case
                                    [:= :grade "A"] "excellent"
                                    [:= :grade "B"] "good"
                                    [:= :grade "C"] "average"
                                    "fail"] doc)))))

  (testing "case with default value"
    (let [doc {:grade "F"}]
      (is (= "fail" (ops/eval-expr [:case
                                    [:= :grade "A"] "excellent"
                                    [:= :grade "B"] "good"
                                    [:= :grade "C"] "average"
                                    "fail"] doc))))))

(deftest test-cond-expression
  (testing "cond with first condition matching"
    (let [doc {:score 95}]
      (is (= "A" (ops/eval-expr [:cond
                                 [:>= :score 90] "A"
                                 [:>= :score 80] "B"
                                 [:>= :score 70] "C"
                                 "F"] doc)))))

  (testing "cond with second condition matching"
    (let [doc {:score 85}]
      (is (= "B" (ops/eval-expr [:cond
                                 [:>= :score 90] "A"
                                 [:>= :score 80] "B"
                                 [:>= :score 70] "C"
                                 "F"] doc)))))

  (testing "cond with no conditions matching"
    (let [doc {:score 50}]
      (is (nil? (ops/eval-expr [:cond
                                [:>= :score 90] "A"
                                [:>= :score 80] "B"
                                [:>= :score 70] "C"] doc))))))

(deftest test-let-expression
  (testing "let with single binding"
    (let [doc {:x 10}]
      ;; y = 10 + 10 = 20, result = 20 * 2 = 40
      (is (= 40 (ops/eval-expr [:let [:y [:+ :x 10]]
                                [:* :y 2]] doc)))))

  (testing "let with multiple bindings"
    (let [doc {:x 5}]
      ;; y = 5 + 10 = 15, z = 15 * 2 = 30, result = 30 + 5 = 35
      (is (= 35 (ops/eval-expr [:let [:y [:+ :x 10]
                                      :z [:* :y 2]]
                                [:+ :z 5]] doc)))))

  (testing "let with bindings referencing earlier bindings"
    (let [doc {:base 10}]
      ;; a = 10 + 5 = 15, b = 15 + 10 = 25, result = 25
      (is (= 25 (ops/eval-expr [:let [:a [:+ :base 5]
                                      :b [:+ :a 10]]
                                :b] doc))))))

(deftest test-coalesce
  (testing "coalesce with first value non-nil"
    (let [doc {:a 10 :b 20}]
      (is (= 10 (ops/eval-expr [:coalesce :a :b] doc)))))

  (testing "coalesce with first value nil"
    (let [doc {:a nil :b 20}]
      (is (= 20 (ops/eval-expr [:coalesce :a :b] doc)))))

  (testing "coalesce with all values nil"
    (let [doc {:a nil :b nil :c nil}]
      (is (nil? (ops/eval-expr [:coalesce :a :b :c] doc)))))

  (testing "coalesce with multiple values"
    (let [doc {:a nil :b nil :c 30 :d 40}]
      (is (= 30 (ops/eval-expr [:coalesce :a :b :c :d] doc))))))

(deftest test-null-if
  (testing "null-if with equal values"
    (let [doc {:a 10 :b 10}]
      (is (nil? (ops/eval-expr [:null-if :a :b] doc)))))

  (testing "null-if with different values"
    (let [doc {:a 10 :b 20}]
      (is (= 10 (ops/eval-expr [:null-if :a :b] doc)))))

  (testing "null-if with nil values"
    (let [doc {:a nil :b nil}]
      (is (nil? (ops/eval-expr [:null-if :a :b] doc))))))

;;; Complex Expression Combinations

(deftest test-complex-arithmetic
  (testing "nested arithmetic operations"
    (let [doc {:a 10 :b 5 :c 2}]
      ;; (a + b) * c = 30
      (is (= 30 (ops/eval-expr [:* [:+ :a :b] :c] doc)))
      ;; (a - b) / c = 2.5 (or 5/2 ratio)
      (is (== 2.5 (ops/eval-expr [:/ [:- :a :b] :c] doc)))
      ;; a + (b * c) = 20
      (is (= 20 (ops/eval-expr [:+ :a [:* :b :c]] doc))))))

(deftest test-complex-conditionals
  (testing "nested if expressions"
    (let [doc {:age 25 :score 85}]
      (is (= "adult-high"
             (ops/eval-expr [:if [:> :age 18]
                             [:if [:>= :score 80] "adult-high" "adult-low"]
                             "minor"] doc)))))

  (testing "if with arithmetic in condition"
    (let [doc {:a 10 :b 5}]
      (is (= "greater" (ops/eval-expr [:if [:> [:+ :a :b] 10]
                                       "greater"
                                       "not-greater"] doc))))))

(deftest test-complex-string-operations
  (testing "trim with coalesce"
    (let [doc {:name nil :fallback "  default  "}]
      (is (= "default" (ops/eval-expr [:trim [:coalesce :name :fallback]] doc)))))

  (testing "like-regex with literal pattern"
    (let [doc {:email "user@example.com"}]
      (is (true? (ops/eval-expr [:like-regex :email ".*@example\\.com"] doc))))))

(deftest test-complex-math
  (testing "sqrt of arithmetic result"
    (let [doc {:a 9 :b 16}]
      (is (= 5.0 (ops/eval-expr [:sqrt [:+ :a :b]] doc)))))

  (testing "pow with computed values"
    (let [doc {:base 2 :exp1 2 :exp2 1}]
      (is (= 8.0 (ops/eval-expr [:pow :base [:+ :exp1 :exp2]] doc))))))

;;; Integration with Existing Operators

(deftest test-integration-with-comparisons
  (testing "new operators with existing comparison logic"
    (let [doc {:age 25 :min-age 18 :score nil}]
      ;; != with arithmetic
      (is (true? (ops/eval-expr [:!= [:+ :age 5] 25] doc)))
      ;; nil? in boolean logic
      (is (true? (ops/eval-expr [:and [:> :age :min-age]
                                 [:nil? :score]] doc)))
      ;; Arithmetic in comparison
      (is (true? (ops/eval-expr [:> [:* :age 2] 40] doc))))))

(deftest test-integration-with-boolean-logic
  (testing "new operators with and/or/not"
    (let [doc {:a 10 :b nil :c 20}]
      ;; and with nil? checks
      (is (false? (ops/eval-expr [:and [:nil? :b] [:nil? :c]] doc)))
      ;; or with arithmetic
      (is (true? (ops/eval-expr [:or [:> [:+ :a :c] 25]
                                 [:< :a 5]] doc)))
      ;; not with coalesce - coalesce returns c (20), nil? returns false, not returns true
      (is (true? (ops/eval-expr [:not [:nil? [:coalesce :b :c]]] doc))))))
