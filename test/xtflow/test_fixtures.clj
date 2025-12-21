(ns xtflow.test-fixtures
  "Shared test fixtures for XTDB testcontainer management."
  (:require [clj-test-containers.core :as tc]
            [xtdb.api :as xt]
            [xtflow.core :as diff]))

(def ^:dynamic *xtdb-container* nil)
(def ^:dynamic *xtdb-client* nil)

(defn xtdb-container-fixture
  "Start XTDB container once for entire test namespace.
   Container runs until tests complete (~6-11 seconds startup)."
  [f]
  (let [container (-> (tc/create
                       {:image-name "ghcr.io/xtdb/xtdb:2.1.0"
                        :exposed-ports [5432 8080]
                        :wait-for {:strategy :log
                                   :message "Node started"}})
                      (tc/start!))
        host (:host container)
        port (get (:mapped-ports container) 5432)
        client (xt/client {:host host :port port :user "xtdb"})]

    (binding [*xtdb-container* container
              *xtdb-client* client]
      (try
        (f)
        (finally
          (tc/stop! container))))))

(defn clear-tables-fixture
  "Clear all tables and queries between individual tests.
   Provides test isolation without container restart overhead."
  [f]
  (let [tables [:prod_attestations :users :orders :permissions]]
    ;; Clear queries
    (diff/reset-all-queries!)

    ;; Clear tables and wait for completion
    (doseq [table tables]
      (try
        (let [ids (map :xt/id (xt/q *xtdb-client* (list 'from table ['xt/id])))
              tx-result (when (seq ids)
                          (xt/execute-tx *xtdb-client*
                                         [(into [:delete-docs table] ids)]))]
          ;; Wait for delete to complete AND verify table is empty
          (when tx-result
            @tx-result
            ;; Query again to ensure delete is processed
            (while (seq (xt/q *xtdb-client* (list 'from table ['xt/id])))
              (Thread/sleep 10))))
        (catch Exception _
          ;; Table might not exist, that's okay
          nil)))

    ;; Run test
    (f)))
