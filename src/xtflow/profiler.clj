(ns xtflow.profiler
  "Profiling utilities for benchmark analysis."
  (:require [clj-async-profiler.core :as prof]))

(defmacro with-profiling
  "Execute body with profiler enabled, save flamegraph.

  Args:
    filename - Output filename (without extension)
    body - Code to profile

  Returns: Result of body execution"
  [filename & body]
  `(do
     (prof/start {})
     (let [result# (do ~@body)]
       (prof/stop)
       (prof/generate-flamegraph ~filename)
       (println "Flamegraph saved to:" ~filename)
       result#)))

(defn profile-benchmark
  "Profile a specific benchmark test.

  Args:
    test-fn - Zero-arg function to profile
    output-name - Name for flamegraph output

  Returns: Test result"
  [test-fn output-name]
  (prof/start {})
  (let [result (test-fn)]
    (prof/stop)
    (prof/generate-flamegraph (str "profiles/" output-name ".html"))
    (println (str "Profile saved to profiles/" output-name ".html"))
    result))
