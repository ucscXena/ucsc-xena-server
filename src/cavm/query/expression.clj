(ns cavm.query.expression
  (:require [clojure.edn :as edn])
  (:require [honeysql.types :as hsqltypes])
  (:require [taoensso.timbre :as timbre :refer [info]])
  (:refer-clojure :exclude [eval letfn]))

(def eval)

(defn lambdafn [node scope]
  (let [[_ argnames body] node]
    (fn [& args]
      (eval body (cons (zipmap argnames args) scope)))))

(defn iffn [node scope]
  (let [[_ predicate then else] node]
    (cond
      (eval predicate scope) (eval then scope)
      :else (eval else scope))))

(defn letfn [node scope]
  (let [[_ assignments body] node]
    (if (empty? assignments)
      (eval body scope)
      (let [[avar aval & remaining] assignments]
        (eval `((~'fn ~[avar]
                  (~'let ~(vec remaining) ~body)) ~aval) scope)))))

(def specials
  {'fn lambdafn
   'quote (fn [n s] (second n))
   'if iffn
   'let letfn})

(def specialfns (set (vals specials)))

(defn fn-node [node scope]
  (let [func (eval (first node) scope)]
    (cond
      (contains? specialfns func) (func node scope)
      :else (apply func (map #(eval % scope) (rest node))))))

(def as-is #{keyword? string?})

(defn sqlcall-node [^honeysql.types.SqlCall node scope]
  (let [fname (.name node) args (.args node)]
    (apply hsqltypes/call (eval fname scope) (mapv #(eval % scope) args))))

(defn eval [node scope]
  (cond
    (symbol? node) ((first (filter #(contains? % node) scope)) node)
    (seq? node) (fn-node node scope)
    (instance? Number node) (double node)
    (instance? honeysql.types.SqlCall node) (sqlcall-node node scope)
    (some #(% node) as-is) node
    ; due to weird (empty <clojure.lang.MapEntry>) behavior, we have
    ; to handle map? separate from coll?
    (map? node) (into {} (mapv #(eval (vec %) scope) node))
    (coll? node) (into (empty node) (mapv #(eval % scope) node))
    :else (info "Unknown node type " (type node))))

(defn expression [exp & globals]
  (eval (edn/read-string {:readers {'sql/call hsqltypes/read-sql-call}} exp) (cons specials globals)))
