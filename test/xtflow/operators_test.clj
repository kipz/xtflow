(ns xtflow.operators-test
  "Unit tests for differential dataflow operators.

  Tests each operator in isolation, verifying delta processing logic."
  (:require [clojure.test :refer [deftest testing is]]
            [xtflow.operators :as ops]
            [xtflow.delta :as delta]))

;;; RETURN Operator Tests

(deftest test-return-operator-basic
  (testing "RETURN operator projects only specified fields"
    (let [op (ops/create-return-operator [:name :age])
          delta (delta/add-delta {:name "Alice" :age 30 :email "alice@example.com" :phone "555-1234"})
          results (ops/process-delta op delta)
          result (first results)]
      (is (= 1 (count results)))
      (is (= {:name "Alice" :age 30} (:doc result)))
      (is (= 1 (:mult result))))))

(deftest test-return-operator-preserves-multiplicity
  (testing "RETURN operator preserves delta multiplicity"
    (let [op (ops/create-return-operator [:name])
          ;; Test addition
          add-delta (delta/add-delta {:name "Bob" :age 25})]
      (let [results (ops/process-delta op add-delta)]
        (is (= 1 (count results)))
        (is (= 1 (:mult (first results)))))

      ;; Test removal
      (let [remove-delta (delta/remove-delta {:name "Bob" :age 25})
            results (ops/process-delta op remove-delta)]
        (is (= 1 (count results)))
        (is (= -1 (:mult (first results))))))))

(deftest test-return-operator-handles-symbols
  (testing "RETURN operator converts symbols to keywords"
    (let [op (ops/create-return-operator '[name age])  ;; symbols instead of keywords
          delta (delta/add-delta {:name "Charlie" :age 35 :city "NYC"})
          results (ops/process-delta op delta)]
      (is (= 1 (count results)))
      (is (= {:name "Charlie" :age 35} (:doc (first results)))))))

(deftest test-return-operator-missing-fields
  (testing "RETURN operator handles missing fields gracefully"
    (let [op (ops/create-return-operator [:name :age :nonexistent])
          delta (delta/add-delta {:name "Dave" :age 40})
          results (ops/process-delta op delta)]
      (is (= 1 (count results)))
      ;; select-keys omits keys that don't exist in the source map
      (is (= {:name "Dave" :age 40} (:doc (first results)))))))

(deftest test-return-operator-empty-doc
  (testing "RETURN operator handles empty documents"
    (let [op (ops/create-return-operator [:name :age])
          delta (delta/add-delta {})
          results (ops/process-delta op delta)]
      (is (= 1 (count results)))
      (is (= {} (:doc (first results)))))))

(deftest test-return-operator-state
  (testing "RETURN operator state management"
    (let [op (ops/create-return-operator [:name])]
      ;; RETURN is stateless, state should be nil
      (is (nil? (ops/get-state op)))

      ;; Process some deltas
      (ops/process-delta op (delta/add-delta {:name "Eve" :age 28}))
      (ops/process-delta op (delta/add-delta {:name "Frank" :age 32}))

      ;; State should still be nil (stateless)
      (is (nil? (ops/get-state op)))

      ;; Reset should work
      (ops/reset-state! op)
      (is (nil? (ops/get-state op))))))

;;; WITHOUT Operator Tests

(deftest test-without-operator-basic
  (testing "WITHOUT operator removes only specified fields"
    (let [op (ops/create-without-operator [:email :phone])
          delta (delta/add-delta {:name "Alice" :age 30 :email "alice@example.com" :phone "555-1234"})
          results (ops/process-delta op delta)
          result (first results)]
      (is (= 1 (count results)))
      (is (= {:name "Alice" :age 30} (:doc result)))
      (is (= 1 (:mult result))))))

(deftest test-without-operator-preserves-multiplicity
  (testing "WITHOUT operator preserves delta multiplicity"
    (let [op (ops/create-without-operator [:age])
          ;; Test addition
          add-delta (delta/add-delta {:name "Bob" :age 25})]
      (let [results (ops/process-delta op add-delta)]
        (is (= 1 (count results)))
        (is (= 1 (:mult (first results)))))

      ;; Test removal
      (let [remove-delta (delta/remove-delta {:name "Bob" :age 25})
            results (ops/process-delta op remove-delta)]
        (is (= 1 (count results)))
        (is (= -1 (:mult (first results))))))))

(deftest test-without-operator-handles-symbols
  (testing "WITHOUT operator converts symbols to keywords"
    (let [op (ops/create-without-operator '[age city])  ;; symbols instead of keywords
          delta (delta/add-delta {:name "Charlie" :age 35 :city "NYC"})
          results (ops/process-delta op delta)]
      (is (= 1 (count results)))
      (is (= {:name "Charlie"} (:doc (first results)))))))

(deftest test-without-operator-nonexistent-fields
  (testing "WITHOUT operator handles nonexistent fields gracefully"
    (let [op (ops/create-without-operator [:email :nonexistent])
          delta (delta/add-delta {:name "Dave" :age 40})
          results (ops/process-delta op delta)]
      (is (= 1 (count results)))
      ;; dissoc ignores keys that don't exist
      (is (= {:name "Dave" :age 40} (:doc (first results)))))))

(deftest test-without-operator-all-fields
  (testing "WITHOUT operator can remove all fields"
    (let [op (ops/create-without-operator [:name :age])
          delta (delta/add-delta {:name "Eve" :age 28})
          results (ops/process-delta op delta)]
      (is (= 1 (count results)))
      (is (= {} (:doc (first results)))))))

(deftest test-without-operator-state
  (testing "WITHOUT operator state management"
    (let [op (ops/create-without-operator [:email])]
      ;; WITHOUT is stateless, state should be nil
      (is (nil? (ops/get-state op)))

      ;; Process some deltas
      (ops/process-delta op (delta/add-delta {:name "Frank" :email "frank@test.com"}))
      (ops/process-delta op (delta/add-delta {:name "Grace" :email "grace@test.com"}))

      ;; State should still be nil (stateless)
      (is (nil? (ops/get-state op)))

      ;; Reset should work
      (ops/reset-state! op)
      (is (nil? (ops/get-state op))))))

;;; OFFSET Operator Tests

(deftest test-offset-operator-basic
  (testing "OFFSET operator skips first N documents"
    (let [op (ops/create-offset-operator 2)]
      ;; First two documents should be skipped
      (let [delta1 (delta/add-delta {:id 1 :name "First"})]
        (is (empty? (ops/process-delta op delta1)) "First doc should be skipped"))

      (let [delta2 (delta/add-delta {:id 2 :name "Second"})]
        (is (empty? (ops/process-delta op delta2)) "Second doc should be skipped"))

      ;; Third document should be emitted
      (let [delta3 (delta/add-delta {:id 3 :name "Third"})
            results (ops/process-delta op delta3)]
        (is (= 1 (count results)) "Third doc should be emitted")
        (is (= {:id 3 :name "Third"} (:doc (first results))))))))

(deftest test-offset-operator-preserves-multiplicity
  (testing "OFFSET operator preserves delta multiplicity for emitted docs"
    (let [op (ops/create-offset-operator 1)]
      ;; Skip first doc
      (ops/process-delta op (delta/add-delta {:id 1}))

      ;; Second doc should be emitted with correct multiplicity
      (let [delta (delta/add-delta {:id 2})
            results (ops/process-delta op delta)]
        (is (= 1 (count results)))
        (is (= 1 (:mult (first results))))))))

(deftest test-offset-operator-removal
  (testing "OFFSET operator handles document removals correctly"
    (let [op (ops/create-offset-operator 1)]
      ;; Add docs
      (ops/process-delta op (delta/add-delta {:id 1}))
      (ops/process-delta op (delta/add-delta {:id 2}))
      (ops/process-delta op (delta/add-delta {:id 3}))

      ;; Remove a doc that was emitted (id 2)
      (let [remove-delta (delta/remove-delta {:id 2})
            results (ops/process-delta op remove-delta)]
        (is (= 1 (count results)) "Should emit removal for emitted doc")
        (is (= -1 (:mult (first results)))))

      ;; Remove a doc that was not emitted (id 1)
      (let [remove-delta (delta/remove-delta {:id 1})
            results (ops/process-delta op remove-delta)]
        (is (empty? results) "Should not emit removal for skipped doc")))))

(deftest test-offset-operator-zero-offset
  (testing "OFFSET operator with offset 0 emits all documents"
    (let [op (ops/create-offset-operator 0)
          delta1 (delta/add-delta {:id 1})
          delta2 (delta/add-delta {:id 2})]
      (is (= 1 (count (ops/process-delta op delta1))))
      (is (= 1 (count (ops/process-delta op delta2)))))))

(deftest test-offset-operator-large-offset
  (testing "OFFSET operator with large offset skips many documents"
    (let [op (ops/create-offset-operator 100)]
      ;; Add 50 documents
      (dotimes [i 50]
        (is (empty? (ops/process-delta op (delta/add-delta {:id i})))))

      ;; State should track 50 seen
      (let [state (ops/get-state op)]
        (is (= 50 (:seen-count state)))
        (is (empty? (:emitted state)))))))

(deftest test-offset-operator-state
  (testing "OFFSET operator state management"
    (let [op (ops/create-offset-operator 2)]
      ;; Initial state
      (let [state (ops/get-state op)]
        (is (= 0 (:seen-count state)))
        (is (= #{} (:emitted state))))

      ;; Process some deltas
      (ops/process-delta op (delta/add-delta {:id 1}))
      (ops/process-delta op (delta/add-delta {:id 2}))
      (ops/process-delta op (delta/add-delta {:id 3}))

      ;; State should track seen count (stops at offset) and emitted docs
      (let [state (ops/get-state op)]
        (is (= 2 (:seen-count state)))  ; Stops at offset value
        (is (= #{{:id 3}} (:emitted state))))

      ;; Reset should clear state
      (ops/reset-state! op)
      (let [state (ops/get-state op)]
        (is (= 0 (:seen-count state)))
        (is (= #{} (:emitted state)))))))

;;; REL Operator Tests

(deftest test-rel-operator-initial-deltas
  (testing "REL operator generates initial deltas from tuples"
    (let [tuples [{:x 1 :y 2} {:x 3 :y 4} {:x 5 :y 6}]
          op (ops/create-rel-operator tuples)
          initial-deltas (ops/rel-initial-deltas op)]
      (is (= 3 (count initial-deltas)) "Should have 3 initial deltas")
      (is (every? #(= 1 (:mult %)) initial-deltas) "All deltas should be additions")
      (is (= #{{:x 1 :y 2} {:x 3 :y 4} {:x 5 :y 6}}
             (set (map :doc initial-deltas)))
          "Should have all tuple documents"))))

(deftest test-rel-operator-empty-tuples
  (testing "REL operator with empty tuples generates no deltas"
    (let [op (ops/create-rel-operator [])
          initial-deltas (ops/rel-initial-deltas op)]
      (is (empty? initial-deltas) "Should have no initial deltas"))))

(deftest test-rel-operator-single-tuple
  (testing "REL operator with single tuple"
    (let [op (ops/create-rel-operator [{:name "Alice" :age 30}])
          initial-deltas (ops/rel-initial-deltas op)]
      (is (= 1 (count initial-deltas)))
      (is (= {:name "Alice" :age 30} (:doc (first initial-deltas))))
      (is (= 1 (:mult (first initial-deltas)))))))

(deftest test-rel-operator-process-delta
  (testing "REL operator passes through deltas unchanged"
    (let [op (ops/create-rel-operator [{:x 1}])
          delta (delta/add-delta {:a 10 :b 20})
          results (ops/process-delta op delta)]
      (is (= 1 (count results)))
      (is (= delta (first results)) "Should pass through unchanged"))))

(deftest test-rel-operator-state
  (testing "REL operator state management"
    (let [op (ops/create-rel-operator [{:x 1} {:x 2}])]
      ;; REL is stateless, state should be nil
      (is (nil? (ops/get-state op)))

      ;; Process some deltas
      (ops/process-delta op (delta/add-delta {:a 1}))

      ;; State should still be nil (stateless)
      (is (nil? (ops/get-state op)))

      ;; Reset should work
      (ops/reset-state! op)
      (is (nil? (ops/get-state op))))))

;;; JOIN Operator Tests

(deftest test-join-operator-basic
  (testing "JOIN operator performs inner join on matching keys"
    (let [op (ops/create-join-operator :id :user-id)]
      ;; Add documents to left input
      (let [left-delta1 (delta/tag-delta (delta/add-delta {:id 1 :name "Alice"}) :left)
            left-delta2 (delta/tag-delta (delta/add-delta {:id 2 :name "Bob"}) :left)]
        (ops/process-delta op left-delta1)
        (ops/process-delta op left-delta2))

      ;; Add documents to right input - only one matches
      (let [right-delta1 (delta/tag-delta (delta/add-delta {:user-id 1 :email "alice@example.com"}) :right)
            results (ops/process-delta op right-delta1)]
        (is (= 1 (count results)) "Should emit one join result")
        (let [result (first results)]
          (is (= {:id 1 :name "Alice" :user-id 1 :email "alice@example.com"} (:doc result)))
          (is (= 1 (:mult result))))))))

(deftest test-join-operator-no-matches
  (testing "JOIN operator emits no results when keys don't match"
    (let [op (ops/create-join-operator :id :user-id)]
      ;; Add document to left
      (ops/process-delta op (delta/tag-delta (delta/add-delta {:id 1 :name "Alice"}) :left))

      ;; Add non-matching document to right
      (let [right-delta (delta/tag-delta (delta/add-delta {:user-id 99 :email "other@example.com"}) :right)
            results (ops/process-delta op right-delta)]
        (is (empty? results) "Should emit no results for non-matching keys")))))

(deftest test-join-operator-multiple-matches
  (testing "JOIN operator handles one-to-many joins"
    (let [op (ops/create-join-operator :dept-id :dept-id)]
      ;; Add multiple documents to left with same key
      (ops/process-delta op (delta/tag-delta (delta/add-delta {:dept-id 1 :name "Alice"}) :left))
      (ops/process-delta op (delta/tag-delta (delta/add-delta {:dept-id 1 :name "Bob"}) :left))

      ;; Add one matching document to right
      (let [right-delta (delta/tag-delta (delta/add-delta {:dept-id 1 :dept-name "Engineering"}) :right)
            results (ops/process-delta op right-delta)]
        (is (= 2 (count results)) "Should emit two join results")
        ;; Both Alice and Bob should join with Engineering
        (is (= #{{:dept-id 1 :name "Alice" :dept-name "Engineering"}
                 {:dept-id 1 :name "Bob" :dept-name "Engineering"}}
               (set (map :doc results))))))))

(deftest test-join-operator-removal
  (testing "JOIN operator handles document removals correctly"
    (let [op (ops/create-join-operator :id :user-id)]
      ;; Add documents to both sides
      (ops/process-delta op (delta/tag-delta (delta/add-delta {:id 1 :name "Alice"}) :left))
      (let [right-delta (delta/tag-delta (delta/add-delta {:user-id 1 :email "alice@example.com"}) :right)]
        (ops/process-delta op right-delta))

      ;; Remove from left
      (let [remove-left (delta/tag-delta (delta/remove-delta {:id 1 :name "Alice"}) :left)
            results (ops/process-delta op remove-left)]
        (is (= 1 (count results)) "Should emit removal delta")
        (is (= -1 (:mult (first results))) "Should have negative multiplicity")))))

(deftest test-join-operator-state
  (testing "JOIN operator state management"
    (let [op (ops/create-join-operator :id :user-id)]
      ;; Initial state
      (let [state (ops/get-state op)]
        (is (= {} (:left-by-key state)))
        (is (= {} (:right-by-key state)))
        (is (= #{} (:emitted state))))

      ;; Add documents
      (ops/process-delta op (delta/tag-delta (delta/add-delta {:id 1 :name "Alice"}) :left))
      (ops/process-delta op (delta/tag-delta (delta/add-delta {:user-id 1 :email "alice@example.com"}) :right))

      ;; State should track both sides and emitted results
      (let [state (ops/get-state op)]
        (is (= #{{:id 1 :name "Alice"}} (get-in state [:left-by-key 1])))
        (is (= #{{:user-id 1 :email "alice@example.com"}} (get-in state [:right-by-key 1])))
        (is (= 1 (count (:emitted state)))))

      ;; Reset should clear state
      (ops/reset-state! op)
      (let [state (ops/get-state op)]
        (is (= {} (:left-by-key state)))
        (is (= {} (:right-by-key state)))
        (is (= #{} (:emitted state)))))))

(deftest test-join-operator-requires-tagged-deltas
  (testing "JOIN operator throws error for untagged deltas"
    (let [op (ops/create-join-operator :id :user-id)
          untagged-delta (delta/add-delta {:id 1 :name "Alice"})]
      (is (thrown? Exception (ops/process-delta op untagged-delta))
          "Should throw exception for untagged delta"))))

;;; LEFT JOIN Operator Tests

(deftest test-left-join-operator-basic
  (testing "LEFT JOIN operator performs left outer join"
    (let [op (ops/create-left-join-operator :id :user-id)]
      ;; Add documents to left input
      (let [left-delta1 (delta/tag-delta (delta/add-delta {:id 1 :name "Alice"}) :left)
            left-delta2 (delta/tag-delta (delta/add-delta {:id 2 :name "Bob"}) :left)
            left-delta3 (delta/tag-delta (delta/add-delta {:id 3 :name "Charlie"}) :left)]
        (ops/process-delta op left-delta1)
        (ops/process-delta op left-delta2)
        (ops/process-delta op left-delta3))

      ;; Add documents to right input - only matches id 1 and 2
      (let [right-delta1 (delta/tag-delta (delta/add-delta {:user-id 1 :email "alice@example.com"}) :right)
            right-delta2 (delta/tag-delta (delta/add-delta {:user-id 2 :email "bob@example.com"}) :right)]
        (ops/process-delta op right-delta1)
        (ops/process-delta op right-delta2))

      ;; Check state - Charlie should be in unmatched-left
      (let [state (ops/get-state op)]
        (is (= 1 (count (:unmatched-left state))) "Charlie should be unmatched")
        (is (contains? (:unmatched-left state) {:id 3 :name "Charlie"}))))))

(deftest test-left-join-operator-emits-unmatched
  (testing "LEFT JOIN emits left rows even without right match"
    (let [op (ops/create-left-join-operator :id :user-id)
          ;; Add left document with no matching right
          left-delta (delta/tag-delta (delta/add-delta {:id 99 :name "Orphan"}) :left)
          results (ops/process-delta op left-delta)
          result (first results)]
      (is (= 1 (count results)) "Should emit left-only result")
      (is (= {:id 99 :name "Orphan"} (:doc result)) "Should emit left doc as-is")
      (is (= 1 (:mult result))))))

(deftest test-left-join-operator-upgrades-unmatched
  (testing "LEFT JOIN upgrades unmatched left when right arrives"
    (let [op (ops/create-left-join-operator :id :user-id)]
      ;; Add left without match - emits left-only
      (let [left-delta (delta/tag-delta (delta/add-delta {:id 1 :name "Alice"}) :left)]
        (ops/process-delta op left-delta))

      ;; Add matching right - should emit removal of left-only and addition of joined
      (let [right-delta (delta/tag-delta (delta/add-delta {:user-id 1 :email "alice@example.com"}) :right)
            results (ops/process-delta op right-delta)]
        (is (= 2 (count results)) "Should emit removal of left-only and addition of joined")
        ;; First delta: removal of left-only
        (let [removal (first results)]
          (is (= {:id 1 :name "Alice"} (:doc removal)))
          (is (= -1 (:mult removal))))
        ;; Second delta: addition of joined
        (let [addition (second results)]
          (is (= {:id 1 :name "Alice" :user-id 1 :email "alice@example.com"} (:doc addition)))
          (is (= 1 (:mult addition))))))))

(deftest test-left-join-operator-downgrades-to-unmatched
  (testing "LEFT JOIN downgrades to unmatched when right is removed"
    (let [op (ops/create-left-join-operator :id :user-id)]
      ;; Add both sides - creates joined result
      (ops/process-delta op (delta/tag-delta (delta/add-delta {:id 1 :name "Alice"}) :left))
      (ops/process-delta op (delta/tag-delta (delta/add-delta {:user-id 1 :email "alice@example.com"}) :right))

      ;; Remove right - should emit removal of joined and addition of left-only
      (let [remove-right (delta/tag-delta (delta/remove-delta {:user-id 1 :email "alice@example.com"}) :right)
            results (ops/process-delta op remove-right)]
        (is (= 2 (count results)) "Should emit removal of joined and addition of left-only")
        ;; First delta: removal of joined
        (let [removal (first results)]
          (is (= {:id 1 :name "Alice" :user-id 1 :email "alice@example.com"} (:doc removal)))
          (is (= -1 (:mult removal))))
        ;; Second delta: addition of left-only
        (let [addition (second results)]
          (is (= {:id 1 :name "Alice"} (:doc addition)))
          (is (= 1 (:mult addition))))))))

(deftest test-left-join-operator-state
  (testing "LEFT JOIN operator state management"
    (let [op (ops/create-left-join-operator :id :user-id)]
      ;; Initial state
      (let [state (ops/get-state op)]
        (is (= {} (:left-by-key state)))
        (is (= {} (:right-by-key state)))
        (is (= #{} (:emitted state)))
        (is (= #{} (:unmatched-left state))))

      ;; Add unmatched left
      (ops/process-delta op (delta/tag-delta (delta/add-delta {:id 1 :name "Alice"}) :left))
      (let [state (ops/get-state op)]
        (is (= 1 (count (:unmatched-left state)))))

      ;; Reset should clear state
      (ops/reset-state! op)
      (let [state (ops/get-state op)]
        (is (= {} (:left-by-key state)))
        (is (= {} (:right-by-key state)))
        (is (= #{} (:emitted state)))
        (is (= #{} (:unmatched-left state)))))))

(deftest test-left-join-operator-requires-tagged-deltas
  (testing "LEFT JOIN operator throws error for untagged deltas"
    (let [op (ops/create-left-join-operator :id :user-id)
          untagged-delta (delta/add-delta {:id 1 :name "Alice"})]
      (is (thrown? Exception (ops/process-delta op untagged-delta))
          "Should throw exception for untagged delta"))))

;;; Unify Operator Tests

(deftest test-unify-operator-basic-single-field
  (testing "UNIFY operator performs single-field unification"
    (let [op (ops/create-unify-operator [[:user-id :user-id]])
          left-delta (delta/tag-delta (delta/add-delta {:user-id 1 :name "Alice"}) :left)
          right-delta (delta/tag-delta (delta/add-delta {:user-id 1 :email "alice@example.com"}) :right)]
      ;; Process left first (no right yet, no output)
      (let [result1 (ops/process-delta op left-delta)]
        (is (empty? result1) "No output until right side matches"))
      ;; Process right (should unify with left)
      (let [result2 (ops/process-delta op right-delta)]
        (is (= 1 (count result2)) "Should have 1 unified result")
        (let [unified (:doc (first result2))]
          (is (= 1 (:user-id unified)))
          (is (= "Alice" (:name unified)))
          (is (= "alice@example.com" (:email unified))))))))

(deftest test-unify-operator-multi-field
  (testing "UNIFY operator performs multi-field unification"
    (let [op (ops/create-unify-operator [[:user-id :user-id] [:org :org]])
          left-delta (delta/tag-delta (delta/add-delta {:user-id 1 :org "acme" :name "Alice"}) :left)
          right-delta1 (delta/tag-delta (delta/add-delta {:user-id 1 :org "acme" :role "admin"}) :right)
          right-delta2 (delta/tag-delta (delta/add-delta {:user-id 1 :org "other" :role "user"}) :right)]
      ;; Process left
      (ops/process-delta op left-delta)
      ;; Process right with matching org
      (let [result1 (ops/process-delta op right-delta1)]
        (is (= 1 (count result1)) "Should match on both fields")
        (is (= "acme" (get-in result1 [0 :doc :org]))))
      ;; Process right with different org (should not match)
      (let [result2 (ops/process-delta op right-delta2)]
        (is (empty? result2) "Should not match when org differs")))))

(deftest test-unify-operator-no-matches
  (testing "UNIFY operator handles non-matching documents"
    (let [op (ops/create-unify-operator [[:id :id]])
          left-delta (delta/tag-delta (delta/add-delta {:id 1 :name "Alice"}) :left)
          right-delta (delta/tag-delta (delta/add-delta {:id 2 :email "bob@example.com"}) :right)
          result1 (ops/process-delta op left-delta)
          result2 (ops/process-delta op right-delta)]
      (is (empty? result1) "No match for left")
      (is (empty? result2) "No match for right"))))

(deftest test-unify-operator-multiple-matches
  (testing "UNIFY operator handles one-to-many relationships"
    (let [op (ops/create-unify-operator [[:user-id :user-id]])
          left-delta (delta/tag-delta (delta/add-delta {:user-id 1 :name "Alice"}) :left)
          right-delta1 (delta/tag-delta (delta/add-delta {:user-id 1 :order-id "ord-1"}) :right)
          right-delta2 (delta/tag-delta (delta/add-delta {:user-id 1 :order-id "ord-2"}) :right)]
      ;; Process left
      (ops/process-delta op left-delta)
      ;; Process first right (should unify)
      (let [result1 (ops/process-delta op right-delta1)]
        (is (= 1 (count result1)) "Should have 1 result for first order"))
      ;; Process second right (should also unify with same left)
      (let [result2 (ops/process-delta op right-delta2)]
        (is (= 1 (count result2)) "Should have 1 result for second order")
        (is (= "ord-2" (get-in result2 [0 :doc :order-id])))))))

(deftest test-unify-operator-removal
  (testing "UNIFY operator handles document removal"
    (let [op (ops/create-unify-operator [[:id :id]])
          left-delta (delta/tag-delta (delta/add-delta {:id 1 :name "Alice"}) :left)
          right-delta (delta/tag-delta (delta/add-delta {:id 1 :email "alice@example.com"}) :right)]
      ;; Add both sides
      (ops/process-delta op left-delta)
      (ops/process-delta op right-delta)
      ;; Remove left side
      (let [removal (delta/tag-delta (delta/remove-delta {:id 1 :name "Alice"}) :left)
            result (ops/process-delta op removal)]
        (is (= 1 (count result)) "Should emit removal delta")
        (is (= -1 (:mult (first result))) "Should have negative multiplicity")))))

(deftest test-unify-operator-state
  (testing "UNIFY operator maintains state correctly"
    (let [op (ops/create-unify-operator [[:key :key]])
          left-delta (delta/tag-delta (delta/add-delta {:key 1 :left "L"}) :left)
          right-delta (delta/tag-delta (delta/add-delta {:key 1 :right "R"}) :right)]
      (ops/process-delta op left-delta)
      (ops/process-delta op right-delta)
      (let [state (ops/get-state op)]
        (is (= 1 (count (:left-docs state))) "Should track left docs")
        (is (= 1 (count (:right-docs state))) "Should track right docs")
        (is (= 1 (count (:emitted state))) "Should track emitted results"))
      ;; Reset state
      (ops/reset-state! op)
      (let [state (ops/get-state op)]
        (is (= #{} (:left-docs state)))
        (is (= #{} (:right-docs state)))
        (is (= #{} (:emitted state)))))))

(deftest test-unify-operator-requires-tagged-deltas
  (testing "UNIFY operator throws error for untagged deltas"
    (let [op (ops/create-unify-operator [[:id :id]])
          untagged-delta (delta/add-delta {:id 1 :name "Alice"})]
      (is (thrown? Exception (ops/process-delta op untagged-delta))
          "Should throw exception for untagged delta"))))

;;; EXISTS Operator Tests

(deftest test-exists-operator-basic-true
  (testing "EXISTS operator adds boolean field when subquery has results"
    (let [;; Create a simple nested graph that always returns results
          ;; For testing, we'll create a REL operator with inline data
          _ (require 'xtflow.dataflow)
          create-nested (resolve 'xtflow.dataflow/create-nested-graph)
          rel-op (ops/create-rel-operator [{:value 1}])
          graph ((resolve 'xtflow.dataflow/create-operator-graph)
                 {0 rel-op}
                 {}
                 {})
          nested (create-nested graph)
          op (ops/create-exists-operator :has-results nested {:ttl-ms 60000})
          delta (delta/add-delta {:xt/id "doc1" :name "test"})
          results (ops/process-delta op delta)]

      (is (= 1 (count results)) "Should emit one delta")
      (is (= 1 (:mult (first results))) "Should have positive multiplicity")
      (is (true? (get-in (first results) [:doc :has-results]))
          "Should add :has-results field as true")
      (is (= "test" (get-in (first results) [:doc :name]))
          "Should preserve original fields"))))

(deftest test-exists-operator-basic-false
  (testing "EXISTS operator adds false field when subquery has no results"
    (let [;; Create a nested graph that returns no results
          ;; Use a WHERE that filters everything out
          _ (require 'xtflow.dataflow)
          create-nested (resolve 'xtflow.dataflow/create-nested-graph)
          where-op (ops/create-where-operator [:= :nonexistent "value"])
          graph ((resolve 'xtflow.dataflow/create-operator-graph)
                 {0 where-op}
                 {}
                 {})
          nested (create-nested graph)
          op (ops/create-exists-operator :has-results nested)
          delta (delta/add-delta {:xt/id "doc1" :name "test"})
          results (ops/process-delta op delta)]

      (is (= 1 (count results)))
      (is (false? (get-in (first results) [:doc :has-results]))
          "Should add :has-results field as false when no subquery results"))))

(deftest test-exists-operator-caching
  (testing "EXISTS operator caches subquery results"
    (let [_ (require 'xtflow.dataflow)
          create-nested (resolve 'xtflow.dataflow/create-nested-graph)
          where-op (ops/create-where-operator [:= :always true])
          graph ((resolve 'xtflow.dataflow/create-operator-graph)
                 {0 where-op}
                 {}
                 {})
          nested (create-nested graph)
          op (ops/create-exists-operator :cached nested {:ttl-ms 60000})
          delta1 (delta/add-delta {:xt/id "doc1" :name "first"})
          delta2 (delta/add-delta {:xt/id "doc1" :name "second"})]  ; Same ID

      ;; First call should execute subquery
      (ops/process-delta op delta1)

      ;; Second call with same doc ID should use cache
      (ops/process-delta op delta2)

      ;; Check state has cached entry
      (let [state (ops/get-state op)]
        (is (contains? (:cache state) "doc1")
            "Should have cached entry for doc1")
        (is (contains? (get-in state [:cache "doc1"]) :expires-at)
            "Cache entry should have expiration timestamp")))))

(deftest test-exists-operator-ttl-expiration
  (testing "EXISTS operator respects TTL"
    (let [_ (require 'xtflow.dataflow)
          create-nested (resolve 'xtflow.dataflow/create-nested-graph)
          rel-op (ops/create-rel-operator [{:value 1}])
          graph ((resolve 'xtflow.dataflow/create-operator-graph)
                 {0 rel-op}
                 {}
                 {})
          nested (create-nested graph)
          ;; Create EXISTS with very short TTL (1ms)
          op (ops/create-exists-operator :has-results nested {:ttl-ms 1})
          delta (delta/add-delta {:xt/id "doc1" :name "test"})]

      ;; First call
      (ops/process-delta op delta)

      ;; Wait for TTL to expire
      (Thread/sleep 10)

      ;; Second call should re-execute (cache expired)
      (let [results (ops/process-delta op delta)]
        (is (= 1 (count results)) "Should still return result after cache expiration")
        (is (true? (get-in (first results) [:doc :has-results]))
            "Result should still be true")))))

(deftest test-exists-operator-removal
  (testing "EXISTS operator handles removals"
    (let [_ (require 'xtflow.dataflow)
          create-nested (resolve 'xtflow.dataflow/create-nested-graph)
          rel-op (ops/create-rel-operator [{:value 1}])
          graph ((resolve 'xtflow.dataflow/create-operator-graph)
                 {0 rel-op}
                 {}
                 {})
          nested (create-nested graph)
          op (ops/create-exists-operator :has-results nested)
          add-delta (delta/add-delta {:xt/id "doc1" :name "test"})
          remove-delta (delta/remove-delta {:xt/id "doc1" :name "test"})]

      ;; Add first
      (ops/process-delta op add-delta)

      ;; Then remove
      (let [results (ops/process-delta op remove-delta)]
        (is (= 1 (count results)) "Should emit removal delta")
        (is (= -1 (:mult (first results))) "Should have negative multiplicity")
        (is (false? (get-in (first results) [:doc :has-results]))
            "Removal should have false field"))

      ;; Check cache was cleared
      (let [state (ops/get-state op)]
        (is (not (contains? (:cache state) "doc1"))
            "Cache should be cleared for removed document")
        (is (not (contains? (:current-set state) "doc1"))
            "Current set should not contain removed document")))))

(deftest test-exists-operator-state-management
  (testing "EXISTS operator state management"
    (let [_ (require 'xtflow.dataflow)
          create-nested (resolve 'xtflow.dataflow/create-nested-graph)
          rel-op (ops/create-rel-operator [{:value 1}])
          graph ((resolve 'xtflow.dataflow/create-operator-graph)
                 {0 rel-op}
                 {}
                 {})
          nested (create-nested graph)
          op (ops/create-exists-operator :has-results nested)
          delta1 (delta/add-delta {:xt/id "doc1" :name "first"})
          delta2 (delta/add-delta {:xt/id "doc2" :name "second"})]

      ;; Process some deltas
      (ops/process-delta op delta1)
      (ops/process-delta op delta2)

      ;; Check state
      (let [state (ops/get-state op)]
        (is (= 2 (count (:cache state))) "Should cache 2 documents")
        (is (= 2 (count (:current-set state))) "Should track 2 current documents"))

      ;; Reset state
      (ops/reset-state! op)
      (let [state (ops/get-state op)]
        (is (= {} (:cache state)) "Cache should be empty after reset")
        (is (= #{} (:current-set state)) "Current set should be empty after reset")))))

(deftest test-exists-operator-nil-nested-graph
  (testing "EXISTS operator handles nil nested graph"
    (let [;; Create EXISTS with nil nested graph (for testing edge cases)
          op (ops/create-exists-operator :has-results nil)
          delta (delta/add-delta {:xt/id "doc1" :name "test"})
          results (ops/process-delta op delta)]

      (is (= 1 (count results)))
      (is (false? (get-in (first results) [:doc :has-results]))
          "Should return false when nested graph is nil"))))

;;; PULL Operator Tests

(deftest test-pull-operator-basic
  (testing "PULL operator nests single matching document as field"
    (let [op (ops/create-pull-operator :user :user-id :id)
          ;; Add right doc first
          right-delta (delta/tag-delta (delta/add-delta {:id 42 :name "Alice" :email "alice@example.com"}) :right)
          _ (ops/process-delta op right-delta)
          ;; Add left doc
          left-delta (delta/tag-delta (delta/add-delta {:xt/id "order1" :user-id 42 :amount 100}) :left)
          results (ops/process-delta op left-delta)]

      (is (= 1 (count results)))
      (let [result-doc (get-in (first results) [:doc])]
        (is (= "order1" (:xt/id result-doc)))
        (is (= 42 (:user-id result-doc)))
        (is (= 100 (:amount result-doc)))
        (is (map? (:user result-doc)) "User field should be a map")
        (is (= "Alice" (get-in result-doc [:user :name])))
        (is (= "alice@example.com" (get-in result-doc [:user :email])))))))

(deftest test-pull-operator-no-match
  (testing "PULL operator returns nil field when no matching document"
    (let [op (ops/create-pull-operator :user :user-id :id)
          ;; Add left doc with no matching right doc
          left-delta (delta/tag-delta (delta/add-delta {:xt/id "order1" :user-id 99 :amount 100}) :left)
          results (ops/process-delta op left-delta)]

      (is (= 1 (count results)))
      (let [result-doc (get-in (first results) [:doc])]
        (is (= "order1" (:xt/id result-doc)))
        (is (= 99 (:user-id result-doc)))
        (is (nil? (:user result-doc)) "User field should be nil when no match")))))

(deftest test-pull-operator-multiple-matches
  (testing "PULL operator takes first match and warns on multiple matches"
    (let [op (ops/create-pull-operator :attestation :digest :digest)
          ;; Add multiple right docs with same digest
          right-delta1 (delta/tag-delta (delta/add-delta {:digest "sha256:abc" :type "slsa" :timestamp 1000}) :right)
          right-delta2 (delta/tag-delta (delta/add-delta {:digest "sha256:abc" :type "provenance" :timestamp 2000}) :right)
          _ (ops/process-delta op right-delta1)
          _ (ops/process-delta op right-delta2)
          ;; Add left doc - should trigger warning
          left-delta (delta/tag-delta (delta/add-delta {:xt/id "sbom1" :digest "sha256:abc" :name "test-sbom"}) :left)
          results (ops/process-delta op left-delta)]

      (is (= 1 (count results)))
      (let [result-doc (get-in (first results) [:doc])]
        (is (= "sbom1" (:xt/id result-doc)))
        (is (map? (:attestation result-doc)) "Should have attestation field")
        ;; Should have one of the two attestations (first one added)
        (is (contains? #{"slsa" "provenance"} (get-in result-doc [:attestation :type])))))))

(deftest test-pull-operator-right-addition-after-left
  (testing "PULL operator updates nested field when right doc is added after left"
    (let [op (ops/create-pull-operator :user :user-id :id)
          ;; Add left doc first (no match)
          left-delta (delta/tag-delta (delta/add-delta {:xt/id "order1" :user-id 42 :amount 100}) :left)
          results1 (ops/process-delta op left-delta)]

      ;; Initially should have nil user field
      (is (= 1 (count results1)))
      (is (nil? (get-in (first results1) [:doc :user])))

      ;; Add matching right doc
      (let [right-delta (delta/tag-delta (delta/add-delta {:id 42 :name "Alice"}) :right)
            results2 (ops/process-delta op right-delta)]

        ;; Should emit: remove old (with nil), add new (with user)
        (is (= 2 (count results2)))
        (let [removal (first results2)
              addition (second results2)]
          (is (= -1 (:mult removal)) "First delta should be removal")
          (is (= 1 (:mult addition)) "Second delta should be addition")
          (is (= "Alice" (get-in addition [:doc :user :name])) "New doc should have nested user"))))))

(deftest test-pull-operator-right-removal
  (testing "PULL operator updates nested field when right doc is removed"
    (let [op (ops/create-pull-operator :user :user-id :id)
          ;; Add right doc first
          right-delta (delta/tag-delta (delta/add-delta {:id 42 :name "Alice"}) :right)
          _ (ops/process-delta op right-delta)
          ;; Add left doc
          left-delta (delta/tag-delta (delta/add-delta {:xt/id "order1" :user-id 42 :amount 100}) :left)
          _ (ops/process-delta op left-delta)
          ;; Now remove right doc
          right-removal (delta/tag-delta (delta/remove-delta {:id 42 :name "Alice"}) :right)
          results (ops/process-delta op right-removal)]

      ;; Should emit: remove old (with user), add new (with nil)
      (is (= 2 (count results)))
      (let [removal (first results)
            addition (second results)]
        (is (= -1 (:mult removal)) "First delta should be removal")
        (is (= 1 (:mult addition)) "Second delta should be addition")
        (is (nil? (get-in addition [:doc :user])) "New doc should have nil user")))))

(deftest test-pull-operator-left-removal
  (testing "PULL operator handles left document removal"
    (let [op (ops/create-pull-operator :user :user-id :id)
          ;; Add right doc
          right-delta (delta/tag-delta (delta/add-delta {:id 42 :name "Alice"}) :right)
          _ (ops/process-delta op right-delta)
          ;; Add left doc
          left-delta (delta/tag-delta (delta/add-delta {:xt/id "order1" :user-id 42 :amount 100}) :left)
          _ (ops/process-delta op left-delta)
          ;; Remove left doc
          left-removal (delta/tag-delta (delta/remove-delta {:xt/id "order1" :user-id 42 :amount 100}) :left)
          results (ops/process-delta op left-removal)]

      (is (= 1 (count results)))
      (let [result (first results)]
        (is (= -1 (:mult result)) "Should be removal delta")
        (is (= "order1" (get-in result [:doc :xt/id])))
        ;; Should have the nested user field in removal
        (is (= "Alice" (get-in result [:doc :user :name])))))))

(deftest test-pull-operator-state-management
  (testing "PULL operator properly manages state"
    (let [op (ops/create-pull-operator :details :key :key)
          ;; Add right doc
          right-delta (delta/tag-delta (delta/add-delta {:key "k1" :value "v1"}) :right)
          _ (ops/process-delta op right-delta)
          ;; Add left doc
          left-delta (delta/tag-delta (delta/add-delta {:xt/id "doc1" :key "k1"}) :left)
          _ (ops/process-delta op left-delta)
          state (ops/get-state op)]

      ;; Check state structure
      (is (contains? state :left-by-key))
      (is (contains? state :right-by-key))
      (is (contains? state :nested-docs))
      (is (contains? state :current-set))
      ;; Check state contents
      (is (contains? (:current-set state) "doc1"))
      (is (= "v1" (get-in state [:nested-docs "doc1" :value])))

      ;; Reset state
      (ops/reset-state! op)
      (let [reset-state (ops/get-state op)]
        (is (empty? (:left-by-key reset-state)))
        (is (empty? (:right-by-key reset-state)))
        (is (empty? (:nested-docs reset-state)))
        (is (empty? (:current-set reset-state)))))))

(deftest test-pull-operator-untagged-delta
  (testing "PULL operator throws error on untagged delta"
    (let [op (ops/create-pull-operator :user :user-id :id)
          untagged-delta (delta/add-delta {:xt/id "order1" :user-id 42})]

      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"PULL operator requires tagged deltas"
                            (ops/process-delta op untagged-delta))))))

;;; PULL* Operator Tests

(deftest test-pull-star-operator-basic
  (testing "PULL* operator nests all matching documents as array field"
    (let [op (ops/create-pull-star-operator :orders :user-id :user-id)
          ;; Add right docs first
          right-delta1 (delta/tag-delta (delta/add-delta {:user-id 42 :order-id 1 :amount 100}) :right)
          right-delta2 (delta/tag-delta (delta/add-delta {:user-id 42 :order-id 2 :amount 50}) :right)
          _ (ops/process-delta op right-delta1)
          _ (ops/process-delta op right-delta2)
          ;; Add left doc
          left-delta (delta/tag-delta (delta/add-delta {:xt/id "user1" :user-id 42 :name "Alice"}) :left)
          results (ops/process-delta op left-delta)]

      (is (= 1 (count results)))
      (let [result-doc (get-in (first results) [:doc])]
        (is (= "user1" (:xt/id result-doc)))
        (is (= "Alice" (:name result-doc)))
        (is (vector? (:orders result-doc)) "Orders field should be a vector")
        (is (= 2 (count (:orders result-doc))) "Should have 2 orders")
        ;; Check that both orders are in the array
        (let [order-ids (set (map :order-id (:orders result-doc)))]
          (is (= #{1 2} order-ids)))))))

(deftest test-pull-star-operator-empty-array
  (testing "PULL* operator returns empty array when no matching documents"
    (let [op (ops/create-pull-star-operator :orders :user-id :user-id)
          ;; Add left doc with no matching right docs
          left-delta (delta/tag-delta (delta/add-delta {:xt/id "user1" :user-id 99 :name "Bob"}) :left)
          results (ops/process-delta op left-delta)]

      (is (= 1 (count results)))
      (let [result-doc (get-in (first results) [:doc])]
        (is (= "user1" (:xt/id result-doc)))
        (is (= "Bob" (:name result-doc)))
        (is (vector? (:orders result-doc)) "Orders field should be a vector")
        (is (empty? (:orders result-doc)) "Orders array should be empty")))))

(deftest test-pull-star-operator-array-addition
  (testing "PULL* operator adds to array when right doc is added"
    (let [op (ops/create-pull-star-operator :orders :user-id :user-id)
          ;; Add right doc first
          right-delta1 (delta/tag-delta (delta/add-delta {:user-id 42 :order-id 1 :amount 100}) :right)
          _ (ops/process-delta op right-delta1)
          ;; Add left doc
          left-delta (delta/tag-delta (delta/add-delta {:xt/id "user1" :user-id 42 :name "Alice"}) :left)
          results1 (ops/process-delta op left-delta)]

      ;; Initially should have 1 order
      (is (= 1 (count results1)))
      (is (= 1 (count (get-in (first results1) [:doc :orders]))))

      ;; Add another matching right doc
      (let [right-delta2 (delta/tag-delta (delta/add-delta {:user-id 42 :order-id 2 :amount 50}) :right)
            results2 (ops/process-delta op right-delta2)]

        ;; Should emit: remove old (with 1 order), add new (with 2 orders)
        (is (= 2 (count results2)))
        (let [removal (first results2)
              addition (second results2)]
          (is (= -1 (:mult removal)) "First delta should be removal")
          (is (= 1 (:mult addition)) "Second delta should be addition")
          (is (= 1 (count (get-in removal [:doc :orders]))) "Old doc should have 1 order")
          (is (= 2 (count (get-in addition [:doc :orders]))) "New doc should have 2 orders"))))))

(deftest test-pull-star-operator-array-removal
  (testing "PULL* operator removes from array when right doc is removed"
    (let [op (ops/create-pull-star-operator :orders :user-id :user-id)
          ;; Add two right docs
          right-delta1 (delta/tag-delta (delta/add-delta {:user-id 42 :order-id 1 :amount 100}) :right)
          right-delta2 (delta/tag-delta (delta/add-delta {:user-id 42 :order-id 2 :amount 50}) :right)
          _ (ops/process-delta op right-delta1)
          _ (ops/process-delta op right-delta2)
          ;; Add left doc
          left-delta (delta/tag-delta (delta/add-delta {:xt/id "user1" :user-id 42 :name "Alice"}) :left)
          _ (ops/process-delta op left-delta)
          ;; Now remove one right doc
          right-removal (delta/tag-delta (delta/remove-delta {:user-id 42 :order-id 1 :amount 100}) :right)
          results (ops/process-delta op right-removal)]

      ;; Should emit: remove old (with 2 orders), add new (with 1 order)
      (is (= 2 (count results)))
      (let [removal (first results)
            addition (second results)]
        (is (= -1 (:mult removal)) "First delta should be removal")
        (is (= 1 (:mult addition)) "Second delta should be addition")
        (is (= 2 (count (get-in removal [:doc :orders]))) "Old doc should have 2 orders")
        (is (= 1 (count (get-in addition [:doc :orders]))) "New doc should have 1 order")
        (is (= 2 (get-in addition [:doc :orders 0 :order-id])) "Remaining order should be order 2")))))

(deftest test-pull-star-operator-array-to-empty
  (testing "PULL* operator transitions array to empty when all right docs removed"
    (let [op (ops/create-pull-star-operator :orders :user-id :user-id)
          ;; Add one right doc
          right-delta (delta/tag-delta (delta/add-delta {:user-id 42 :order-id 1 :amount 100}) :right)
          _ (ops/process-delta op right-delta)
          ;; Add left doc
          left-delta (delta/tag-delta (delta/add-delta {:xt/id "user1" :user-id 42 :name "Alice"}) :left)
          _ (ops/process-delta op left-delta)
          ;; Now remove the only right doc
          right-removal (delta/tag-delta (delta/remove-delta {:user-id 42 :order-id 1 :amount 100}) :right)
          results (ops/process-delta op right-removal)]

      ;; Should emit: remove old (with 1 order), add new (with empty array)
      (is (= 2 (count results)))
      (let [removal (first results)
            addition (second results)]
        (is (= -1 (:mult removal)) "First delta should be removal")
        (is (= 1 (:mult addition)) "Second delta should be addition")
        (is (= 1 (count (get-in removal [:doc :orders]))) "Old doc should have 1 order")
        (is (empty? (get-in addition [:doc :orders])) "New doc should have empty array")
        (is (vector? (get-in addition [:doc :orders])) "Should still be a vector")))))

(deftest test-pull-star-operator-right-addition-after-left
  (testing "PULL* operator updates array when right docs added after left"
    (let [op (ops/create-pull-star-operator :orders :user-id :user-id)
          ;; Add left doc first (no matches)
          left-delta (delta/tag-delta (delta/add-delta {:xt/id "user1" :user-id 42 :name "Alice"}) :left)
          results1 (ops/process-delta op left-delta)]

      ;; Initially should have empty array
      (is (= 1 (count results1)))
      (is (empty? (get-in (first results1) [:doc :orders])))

      ;; Add matching right doc
      (let [right-delta (delta/tag-delta (delta/add-delta {:user-id 42 :order-id 1 :amount 100}) :right)
            results2 (ops/process-delta op right-delta)]

        ;; Should emit: remove old (empty array), add new (with 1 order)
        (is (= 2 (count results2)))
        (let [removal (first results2)
              addition (second results2)]
          (is (= -1 (:mult removal)) "First delta should be removal")
          (is (= 1 (:mult addition)) "Second delta should be addition")
          (is (empty? (get-in removal [:doc :orders])) "Old doc should have empty array")
          (is (= 1 (count (get-in addition [:doc :orders]))) "New doc should have 1 order"))))))

(deftest test-pull-star-operator-left-removal
  (testing "PULL* operator handles left document removal"
    (let [op (ops/create-pull-star-operator :orders :user-id :user-id)
          ;; Add right docs
          right-delta1 (delta/tag-delta (delta/add-delta {:user-id 42 :order-id 1 :amount 100}) :right)
          right-delta2 (delta/tag-delta (delta/add-delta {:user-id 42 :order-id 2 :amount 50}) :right)
          _ (ops/process-delta op right-delta1)
          _ (ops/process-delta op right-delta2)
          ;; Add left doc
          left-delta (delta/tag-delta (delta/add-delta {:xt/id "user1" :user-id 42 :name "Alice"}) :left)
          _ (ops/process-delta op left-delta)
          ;; Remove left doc
          left-removal (delta/tag-delta (delta/remove-delta {:xt/id "user1" :user-id 42 :name "Alice"}) :left)
          results (ops/process-delta op left-removal)]

      (is (= 1 (count results)))
      (let [result (first results)]
        (is (= -1 (:mult result)) "Should be removal delta")
        (is (= "user1" (get-in result [:doc :xt/id])))
        ;; Should have the array field in removal
        (is (= 2 (count (get-in result [:doc :orders]))))))))

(deftest test-pull-star-operator-state-management
  (testing "PULL* operator properly manages state"
    (let [op (ops/create-pull-star-operator :items :key :key)
          ;; Add right docs
          right-delta1 (delta/tag-delta (delta/add-delta {:key "k1" :value "v1"}) :right)
          right-delta2 (delta/tag-delta (delta/add-delta {:key "k1" :value "v2"}) :right)
          _ (ops/process-delta op right-delta1)
          _ (ops/process-delta op right-delta2)
          ;; Add left doc
          left-delta (delta/tag-delta (delta/add-delta {:xt/id "doc1" :key "k1"}) :left)
          _ (ops/process-delta op left-delta)
          state (ops/get-state op)]

      ;; Check state structure
      (is (contains? state :left-by-key))
      (is (contains? state :right-by-key))
      (is (contains? state :nested-arrays))
      (is (contains? state :current-set))
      ;; Check state contents
      (is (contains? (:current-set state) "doc1"))
      (is (= 2 (count (get-in state [:nested-arrays "doc1"]))))

      ;; Reset state
      (ops/reset-state! op)
      (let [reset-state (ops/get-state op)]
        (is (empty? (:left-by-key reset-state)))
        (is (empty? (:right-by-key reset-state)))
        (is (empty? (:nested-arrays reset-state)))
        (is (empty? (:current-set reset-state)))))))

(deftest test-pull-star-operator-untagged-delta
  (testing "PULL* operator throws error on untagged delta"
    (let [op (ops/create-pull-star-operator :orders :user-id :user-id)
          untagged-delta (delta/add-delta {:xt/id "user1" :user-id 42})]

      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"PULL\* operator requires tagged deltas"
                            (ops/process-delta op untagged-delta))))))
