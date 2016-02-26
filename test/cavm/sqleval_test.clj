(ns cavm.sqleval-test
  (:require [cavm.sqleval :as sqleval])
  (:require [clojure.test :as ct])
  (:require [clojure.test.check :as tc])
  (:require [clojure.test.check.generators :as gen])
  (:require [clojure.test.check.clojure-test :as tcct :refer [defspec]])
  (:require [clojure.test.check.properties :as prop]))

(def ^:dynamic *test-runs* 4000)

(ct/deftest in-test
  (ct/testing "basic :in"
    (let [data {"a" [:a :b :c :d :e :f :g :h]}
          rows (sorted-set 1 3 4 5 6 7)
          store {:indexed? #{}
                 :fetch (fn [rows-in field]
                          (let [r (or rows-in rows)]
                            (zipmap r
                                    (map (data field) r))))}]
      (ct/is (= (sorted-set 4 5 7)
                (sqleval/op-in rows
                               rows
                               store
                               "a"
                               [:e :f :h]))))))

(ct/deftest eval-test
  (ct/testing "eval")
  (let [data {"a" [:a :b :c :d :e :f :g :h]
              "b" (into [] (range 20 30))}
        rows (apply sorted-set (range 8))
        store {:indexed? #{}
               :fetch (fn [rows-in field]
                          (let [r (or rows-in rows)]
                            (zipmap r
                                    (map (data field) r))))}]
    (ct/is (= {"b" [24 25 27]}
              (sqleval/evaluate rows
                                store
                                {:select ["b"] :where [:in "a" [:e :f :h]]})))
    (ct/is (= {"b" [24 25 27]}
              (sqleval/evaluate rows
                                store
                                {:select ["b"] :where [:and [:in "a" [:e :f :h]]]})))
    (ct/is (= {"a" [:e :f] "b" [24 25]}
              (sqleval/evaluate rows
                                store
                                {:select ["a" "b"] :where [:and [:in "a" [:e :f :h]] [:in "b" [24 25]]]})))
    (ct/is (= {"a" [:a :b :e :f :h] "b" [20 21 24 25 27]}
              (sqleval/evaluate rows
                                store
                                {:select ["a" "b"] :where [:or [:in "a" [:e :f :h]] [:in "b" [20 21]]]})))))
