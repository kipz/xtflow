# XTFlow

[![CI](https://github.com/kipz/xtflow/actions/workflows/ci.yml/badge.svg)](https://github.com/kipz/xtflow/actions/workflows/ci.yml) [![Clojars Project](https://img.shields.io/clojars/v/org.kipz/xtflow.svg)](https://clojars.org/org.kipz/xtflow)

Differential dataflow for XTDB - incrementally maintain query results without re-execution.

## What is XTFlow?

XTFlow is a differential dataflow engine for XTDB that incrementally updates query results as data changes. Instead of re-running queries on every transaction, XTFlow propagates changes (deltas) through an operator graph to compute precise updates.

**Key Features**:
- True incremental computation - no query re-execution
- Complete XTQL operator support (16/17 operators - see below)
- Callback-based change notifications
- Deep nested field access for complex document structures
- Advanced subquery operations: EXISTS, PULL, PULL*
- Multi-input operations: JOIN, LEFT JOIN, UNIFY
- Efficient: only affected queries are updated
- Pure Clojure, no external dependencies

## Quick Start

Add to `deps.edn`:

**Via Git:**
```clojure
{:deps {org.kipz/xtflow {:git/url "https://github.com/kipz/xtflow"
                         :git/sha "..."}}}
```

**Via Maven/Clojars:**
```clojure
{:deps {org.kipz/xtflow {:mvn/version "x.y.z"}}}
```

Register a query with callback:
```clojure
(require '[xtflow.core :as flow]
         '[xtdb.api :as xt])

(def xtdb-client (xt/start-node {}))

(flow/register-query!
  {:query-id "my-query"
   :xtql "(-> (from :users [name age])
              (where (> age 18))
              (aggregate {:count (row-count)}))"
   :callback (fn [changes]
               (println "Total adults:"
                        (-> changes :modified first :new :count)))})
```

Execute transactions - callbacks fire automatically:
```clojure
(flow/execute-tx! xtdb-client
  [[:put-docs :users
    {:xt/id "user-1" :name "Alice" :age 25}]])

;; Output: Total adults: 1
```

## How It Works

XTFlow uses differential dataflow:
1. Parses XTQL queries into operator graphs
2. Indexes queries by affected tables
3. On transaction, computes deltas (document changes with +1/-1 multiplicity)
4. Propagates deltas through operator graphs
5. Computes result diffs (added/removed/modified)
6. Fires callbacks with precise changes

## Supported XTQL Operators

XTFlow supports **16 of 17** XTDB v2 XTQL operators (94% coverage):

### Source Operators
- ✅ `from` - Sources data from a table with field projection
- ✅ `rel` - Sources data from inline relation literals
- ✅ `unify` - Combines multiple input sources using Datalog-style unification

### Tail Operators
- ✅ `where` - Filters rows using predicates (=, >, <, >=, <=, like, and, or, not)
- ✅ `with` - Adds computed fields with deep nested access (`..`)
- ✅ `without` - Removes specified fields from documents
- ✅ `return` - Projects only specified fields
- ✅ `unnest` - Flattens arrays into individual rows
- ✅ `aggregate` - Groups and aggregates with count, sum, min, max, avg
- ✅ `limit` - Returns top N rows
- ✅ `offset` - Skips first N rows
- ❌ `order-by` - **Not yet supported** (rows returned in arbitrary order)

### Multi-Input Operators
- ✅ `join` - Inner join between two input streams
- ✅ `left-join` - Left outer join (preserves unmatched left rows)

### Subquery Expressions
- ✅ `exists` - Boolean check if subquery returns results (with TTL caching)
- ✅ `pull` - Retrieves single related row as nested map
- ✅ `pull*` - Retrieves multiple related rows as array

All operators support incremental updates with full differential dataflow semantics.

## Examples

See `examples/` directory for:
- `basic.clj` - Simple usage patterns
- `e2e_callbacks.clj` - Comprehensive callback demonstrations
- `repl_usage.clj` - REPL experimentation

## License

MIT License - see LICENSE file
