(ns cavm.db-test
  (:use cavm.h2)
  (:use cavm.query.sources)
  (:use clojure.test))

(def db (create-db "testing_db"))

(defn db-fixture [f]
  (with-db db
    (f)))

(use-fixtures :once db-fixture)

(def eps 0.000001)

(defn- nearly-equal [e a b]
  (and (= (count a) (count b))
       (reduce (fn [acc pair] (and acc (< (java.lang.Math/abs (apply - pair)) e)))
               true
               (map vector a b))))

(deftest one-probe
  (testing "one probe"
    (let [out (read-symbols
                [genomic-source]
                '[{table fivebyten columns ["3"] samples ["1" "2"]}])]
      (is (nearly-equal eps [-1.2 -0.2] (vec (first out)))))))
