(ns cavm.sqleval
  (:require [clojure.data.int-map :as i])
  (:require [clojure.core.match :refer [match]]))

(declare restrict)

(defn op-or [all rows store subexps]
  (reduce i/union
          (map #(restrict all rows store %) subexps)))

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
              (i/intersection
                acc
                (restrict all acc store subexp)))
            rows subexps)))

; Perform 'in' operator with given compare function.
(defn op-in-cmp [cmp all rows {:keys [fetch indexed? fetch-indexed]} field values]
  (if (and (= all rows) (indexed? field))
    (fetch-indexed field values)
    (let [col (fetch rows field) ; scan
          s (set values)]
      (into (i/int-set) (filter #(cmp s (col %)) rows)))))

; Scalar compare.
(defn in-set [a-set value] (a-set value))
; Array 'any' compare.
(defn any-in-set [a-set arrValue]
  (some #(a-set %) arrValue))
; Array 'all' compare.
(defn all-in-set [a-set arrValue]
  (every? #(a-set %) arrValue))

; We optimize lookups over indexed fields, if possible. We only
; consider using the index if the list of rows has not already been
; narrowed, i.e. when (= rows all).  We pass 'all' through the code
; so we can track this.
(defn restrict [all rows store exp]
  (match [exp]
         [[:and & subexps]] (op-and all rows store subexps)
         [[:or & subexps]]  (op-or all rows store subexps)
         [[:in field values]] (op-in-cmp in-set all rows store field values)
         [[:in :all field values]] (op-in-cmp all-in-set all rows store field values)
         [[:in :any field values]] (op-in-cmp any-in-set all rows store field values)
         [nil] rows)) ; No 'where' clause.

; XXX profile the map over the col function.
(defn project [rows {fetch :fetch} fields]
  (map (fn [field]
         (let [col (fetch rows field)]
           (mapv col rows)))
       fields))

; XXX This could be optimized in a top-level 'or' by early exit.
; As a subexpression, we can't exit early.
(defn limit-result [limit offset rows]
  (let [to-drop (if offset offset 0)]
    (if limit
      (take limit (drop to-drop rows))
      (drop to-drop rows))))

; XXX Add some param guards, esp. rows is a sorted-set, keys in store, select is vec of string.
(defn evaluate [rows store {:keys [select where limit offset]}]
  (let [result-rows (restrict rows rows store where)
        limited-rows (limit-result limit offset result-rows)]
    (zipmap select (project limited-rows store select))))
