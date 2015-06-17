(ns cavm.sqleval
  (:require [clojure.core.match :refer [match]]))

(declare evaluate1)

(defn op-or [all rows store subexps]
  (reduce clojure.set/union
          (map #(evaluate1 all rows store %) subexps)))

(defn exp-indexed? [indexed? exp]
  (match [exp]
         [[:in field values]] (indexed? field)
         [_] false))

; Sort simple terms with indexed fields to the front.
(defn sort-exps [indexed? exps]
  (let [idx (map #(exp-indexed? indexed? %) exps)]
    (map second
         (mapcat (group-by (comp boolean first) (map vector idx exps)) [true false]))))

; We try to limit rows fetched in each column by only fetching rows
; we require for the next term of the predicate. I.e. after each
; fetch we interesect the returned rows with the rows in the result
; set so far.
(defn op-and [all rows {:keys [indexed?] :as store} subexps]
  (let [subexps (if (= all rows)
                  (sort-exps indexed? subexps)
                  subexps)]
    (reduce (fn [acc subexp]
              (clojure.set/intersection
                acc
                (evaluate1 all acc store subexp)))
            rows subexps)))

(defn op-in [all rows {:keys [fetch indexed? fetch-indexed]} field values]
  (if (and (= all rows) (indexed? field))
    (fetch-indexed field values)
    (let [col (fetch rows field) ; scan
          s (set values)]
      (into (sorted-set) (filter #(s (col %)) rows)))))

; We optimize lookups over indexed fields, if possible. We only
; consider using the index if the list of rows has not already been
; narrowed, i.e. when (= rows all).  We pass 'all' through the code
; so we can track this.
(defn evaluate1 [all rows store exp]
  (match [exp]
         [[:and & subexps]] (op-and all rows store subexps)
         [[:or & subexps]]  (op-or all rows store subexps)
         [[:in field values]] (op-in all rows store field values)))

(defn evaluate [rows store exp]
  (evaluate1 rows rows store exp))
