(ns cavm.binner-test
  (:use clojure.test
        cavm.binner))

(deftest basic-test
  (testing "Some bins"
    (is (= 4681 (calc-bin 100 1000)))
    (is (= 4684 (calc-bin 400000 500000)))
    (is (= 1061 (calc-bin 499900000 500000000)))
    (is (= 585 (calc-bin 100000 500000)))))
