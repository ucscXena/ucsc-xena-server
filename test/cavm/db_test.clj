(ns cavm.query.db-test
  (:use cavm.h2)
  (:use clojure.test))

(def db (create-db "testing_db"))

(defn db-fixture [f]
  (with-db db
    (f)))

(use-fixtures :once db-fixture)

(deftest one-probe
  (testing "one probe"
    (let [out (read-symbols
                genomic-source
                '[{table fivebyten columns [3]}])]
      (is (= [1.1 1.2 1.3 1.4] out)))))
