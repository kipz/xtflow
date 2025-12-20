(ns xtflow.delta
  "Delta data structures and operations for differential dataflow.

  Deltas represent changes to collections as multisets where each element
  has a multiplicity (+1 for addition, -1 for deletion).

  Example delta:
    {:doc {:xt/id \"hash123\" :predicate_type \"slsa-provenance\" ...}
     :mult +1}
  "
  (:require [clojure.data :refer [diff]]
            [clojure.set :as set]))

;;; Delta Creation

(defn make-delta
  "Create a delta for a document with given multiplicity.

  Args:
    doc - Document map
    mult - Multiplicity (+1 for add, -1 for remove)

  Returns: Delta map"
  [doc mult]
  {:doc doc :mult mult})

(defn add-delta
  "Create an addition delta (+1 multiplicity)"
  [doc]
  (make-delta doc 1))

(defn remove-delta
  "Create a removal delta (-1 multiplicity)"
  [doc]
  (make-delta doc -1))

;;; Tagged Deltas for Multi-Input Operators

(defn tag-delta
  "Tag a delta with a source indicator for multi-input operators.

  Args:
    delta - Delta map
    source - Source keyword (e.g., :left, :right)

  Returns: Tagged delta map"
  [delta source]
  (assoc delta :source source))

(defn untag-delta
  "Remove source tag from a delta.

  Args:
    delta - Tagged delta map

  Returns: Untagged delta map"
  [delta]
  (dissoc delta :source))

(defn get-source
  "Get the source tag from a delta, or nil if untagged.

  Args:
    delta - Delta map

  Returns: Source keyword or nil"
  [delta]
  (:source delta))

(defn tagged?
  "Check if a delta has a source tag.

  Args:
    delta - Delta map

  Returns: true if delta has :source key"
  [delta]
  (contains? delta :source))

;;; Diff Computation

(defn compute-result-diff
  "Compute differences between old and new result sets.

  Compares two collections and produces:
  - :added - Documents present in new but not old
  - :removed - Documents present in old but not new
  - :modified - Documents with same :xt/id but different content

  Args:
    old-results - Previous result collection
    new-results - Current result collection

  Returns: Map with :added, :removed, :modified keys"
  [old-results new-results]
  (let [old-set (set old-results)
        new-set (set new-results)

        ;; Simple set diff for additions and removals
        added-set (set/difference new-set old-set)
        removed-set (set/difference old-set new-set)

        ;; For modified, we need to check by :xt/id if present
        ;; Otherwise fall back to the entire doc as key
        old-by-id (if (every? :xt/id old-results)
                    (group-by :xt/id old-results)
                    {})
        new-by-id (if (every? :xt/id new-results)
                    (group-by :xt/id new-results)
                    {})

        ;; Find modified: same ID but different content
        modified (when (and (seq old-by-id) (seq new-by-id))
                   (let [common-ids (set/intersection (set (keys old-by-id))
                                                      (set (keys new-by-id)))]
                     (for [id common-ids
                           :let [old-doc (first (old-by-id id))
                                 new-doc (first (new-by-id id))]
                           :when (not= old-doc new-doc)]
                       {:id id
                        :old old-doc
                        :new new-doc
                        :changes (diff old-doc new-doc)})))]

    {:added (vec added-set)
     :removed (vec removed-set)
     :modified (vec modified)}))

;;; Helper Functions

(defn has-changes?
  "Check if a diff contains any changes.

  Args:
    diff - Diff map from compute-result-diff

  Returns: true if any changes present"
  [diff]
  (or (seq (:added diff))
      (seq (:removed diff))
      (seq (:modified diff))))

(comment
  ;; Usage examples

  ;; Create deltas
  (def d1 (add-delta {:xt/id "doc1" :value 10}))
  (def d2 (remove-delta {:xt/id "doc1" :value 10}))

  ;; Compute diff
  (def old [{:xt/id "doc1" :count 5}])
  (def new [{:xt/id "doc1" :count 6}])
  (compute-result-diff old new)
  ;; => {:added [] :removed [] :modified [{:id "doc1" :old {...} :new {...}}]}
  )
