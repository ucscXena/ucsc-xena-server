(ns cavm.query.sources-test
  (:use cavm.query.sources)
  (:use clojure.test))

(deftest add-output-hash-test
  (testing "add output hash"
    (let [out (add-output-hash '[{columns [sym1 sym2]} {columns [sym3]}])]
      (is (= '[{columns [sym1 sym2] data {sym1 nil sym2 nil}}
               {columns [sym3] data {sym3 nil}}] out)))))

(deftest collect-test
  (testing "collect data"
    (let [out (collect '{data {a 5 b 6 c 12} columns [c b a]})]
      (is (= [12 6 5] out)))))

(deftest read-symbols-test
  (testing "read symbols"
    (let [fn1 (fn [reqs] (assoc-in reqs [0 'data 'sym1] 12))
          fn2 (fn [reqs] (assoc-in reqs [1 'data 'sym2] 7))
          out (read-symbols [fn1 fn2] '[{columns [sym1]} {columns [sym2]}])]
      (is (= [12 7] out)))))
