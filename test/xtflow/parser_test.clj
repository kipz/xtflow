(ns xtflow.parser-test
  (:require [clojure.test :refer [deftest testing is]]
            [xtflow.parser :as parser]))

;;; Parse Function Tests

(deftest test-parse-return-form
  (testing "RETURN form parsing"
    (let [form '(return name age)
          result (parser/parse-return-form form)]
      (is (= :return (:op result)))
      (is (= '[name age] (:fields result))))))

(deftest test-parse-without-form
  (testing "WITHOUT form parsing"
    (let [form '(without password secret)
          result (parser/parse-without-form form)]
      (is (= :without (:op result)))
      (is (= '[password secret] (:fields result))))))

(deftest test-parse-offset-form
  (testing "OFFSET form parsing"
    (let [form '(offset 10)
          result (parser/parse-offset-form form)]
      (is (= :offset (:op result)))
      (is (= 10 (:n result))))))

(deftest test-parse-rel-form
  (testing "REL form parsing"
    (let [form '(rel [{:x 1 :y 2} {:x 3 :y 4}])
          result (parser/parse-rel-form form)]
      (is (= :rel (:op result)))
      (is (= [{:x 1 :y 2} {:x 3 :y 4}] (:tuples result))))))

(deftest test-parse-join-form
  (testing "JOIN form parsing"
    (let [form '(join (from :orders [order-id user-id]) {:user-id user-id})
          result (parser/parse-join-form form)]
      (is (= :join (:op result)))
      (is (= '(from :orders [order-id user-id]) (:right-query result)))
      (is (= [:user-id] (:join-keys result))))))

(deftest test-parse-left-join-form
  (testing "LEFT JOIN form parsing"
    (let [form '(left-join (from :orders [order-id user-id]) {:user-id user-id})
          result (parser/parse-left-join-form form)]
      (is (= :left-join (:op result)))
      (is (= '(from :orders [order-id user-id]) (:right-query result)))
      (is (= [:user-id] (:join-keys result))))))

(deftest test-parse-unify-form
  (testing "UNIFY form parsing"
    (let [form '(unify (from :users [user-id]) (from :permissions [user-id]) {:user-id user-id})
          result (parser/parse-unify-form form)]
      (is (= :unify (:op result)))
      (is (= '(from :users [user-id]) (:left-query result)))
      (is (= '(from :permissions [user-id]) (:right-query result)))
      (is (= [:user-id] (:unify-keys result))))))

(deftest test-parse-unify-form-multiple-keys
  (testing "UNIFY form parsing with multiple unification keys"
    (let [form '(unify (from :users [user-id org]) (from :permissions [user-id org]) {:user-id user-id :org org})
          result (parser/parse-unify-form form)]
      (is (= :unify (:op result)))
      (is (= 2 (count (:unify-keys result))))
      (is (contains? (set (:unify-keys result)) :user-id))
      (is (contains? (set (:unify-keys result)) :org)))))

;;; Operator Form Parsing Tests

(deftest test-parse-operator-form-return
  (testing "parse-operator-form recognizes RETURN"
    (let [form '(return name age)
          result (parser/parse-operator-form form)]
      (is (= :return (:op result))))))

(deftest test-parse-operator-form-without
  (testing "parse-operator-form recognizes WITHOUT"
    (let [form '(without password)
          result (parser/parse-operator-form form)]
      (is (= :without (:op result))))))

(deftest test-parse-operator-form-offset
  (testing "parse-operator-form recognizes OFFSET"
    (let [form '(offset 5)
          result (parser/parse-operator-form form)]
      (is (= :offset (:op result))))))

(deftest test-parse-operator-form-rel
  (testing "parse-operator-form recognizes REL"
    (let [form '(rel [{:x 1}])
          result (parser/parse-operator-form form)]
      (is (= :rel (:op result))))))

(deftest test-parse-operator-form-join
  (testing "parse-operator-form recognizes JOIN"
    (let [form '(join (from :orders [id]) {:id id})
          result (parser/parse-operator-form form)]
      (is (= :join (:op result))))))

(deftest test-parse-operator-form-left-join
  (testing "parse-operator-form recognizes LEFT JOIN"
    (let [form '(left-join (from :orders [id]) {:id id})
          result (parser/parse-operator-form form)]
      (is (= :left-join (:op result))))))

(deftest test-parse-operator-form-unify
  (testing "parse-operator-form recognizes UNIFY"
    (let [form '(unify (from :a [x]) (from :b [x]) {:x x})
          result (parser/parse-operator-form form)]
      (is (= :unify (:op result))))))

(deftest test-parse-operator-form-unknown
  (testing "parse-operator-form throws for unknown operators"
    (is (thrown? Exception
                 (parser/parse-operator-form '(unknown-op arg))))))

;;; Integration Tests

(deftest test-parse-simple-pipeline-with-return
  (testing "Parse simple pipeline with RETURN"
    (let [xtql "(-> (from :users [name age email]) (return name age))"
          result (parser/parse-xtql-to-graph xtql)]
      (is (= 2 (count (:parsed result))))
      (is (= #{:users} (:tables result))))))

(deftest test-parse-simple-pipeline-with-without
  (testing "Parse simple pipeline with WITHOUT"
    (let [xtql "(-> (from :users [name age password]) (without password))"
          result (parser/parse-xtql-to-graph xtql)]
      (is (= 2 (count (:parsed result)))))))

(deftest test-parse-simple-pipeline-with-offset
  (testing "Parse simple pipeline with OFFSET"
    (let [xtql "(-> (from :users [name]) (offset 10) (limit 5))"
          result (parser/parse-xtql-to-graph xtql)]
      (is (= 3 (count (:parsed result)))))))

(deftest test-parse-pipeline-with-multi-operators
  (testing "Parse pipeline with multiple new operators"
    (let [xtql "(-> (from :users [name age email score]) (offset 5) (without email) (return name score) (limit 10))"
          result (parser/parse-xtql-to-graph xtql)]
      (is (= 5 (count (:parsed result))))
      (is (= #{:users} (:tables result))))))

