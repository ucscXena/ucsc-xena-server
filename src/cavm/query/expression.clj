(ns cavm.query.expression
  (:require [clojure.edn :as edn])
  (:refer-clojure :exclude  [eval]))

(def eval)

(defn lambdafn [node scope]
  (let [[_ argnames body] node]
    (fn [& args]
      (eval body (cons (zipmap argnames args) scope)))))

(defn iffn [node scope]
  (let [[_ test then else] node]
    (cond
      (eval test scope) (eval then scope)
      :else (eval else scope))))

(def specials
  {'fn lambdafn
   'quote (fn [n s] (second n))
   'if iffn })

(def specialfns (set (vals specials)))

(defn fn-node [node scope]
  (let [func (eval (first node) scope)]
    (cond
      (contains? specialfns func) (func node scope)
      :else (apply func (map #(eval % scope) (rest node))))))

(defn eval [node scope]
  (cond
    (symbol? node) ((first (filter #(contains? % node) scope)) node)
    (list? node) (fn-node node scope)
    (instance? Number node) (double node)
    :else node))

(defn expression [exp & globals]
  (eval (edn/read-string exp) (cons specials globals)))
