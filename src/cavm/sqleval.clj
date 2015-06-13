(ns cavm.sqleval
  (:require [clojure.core.match :refer [match]]))

(declare evaluate)

(defn op-or [rows fetch subexps]
  (reduce clojure.set/union
          (map #(evaluate rows fetch %) subexps)))

(defn op-and [rows fetch subexps]
  (reduce (fn [acc subexp]
            (clojure.set/intersection
              acc
              (evaluate acc fetch subexp)))
          rows subexps))

(defn op-in [rows fetch field values]
  (let [col (fetch rows field)
        s (set values)]
    (into (sorted-set) (filter #(s (get col %)) rows))))

(defn evaluate [rows fetch exp]
  (match [exp]
        [[:and & subexps]] (op-and rows fetch subexps)
        [[:or & subexps]]  (op-or rows fetch subexps)
        [[:in field values]] (op-in rows fetch field values)))
