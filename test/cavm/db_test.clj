(ns cavm.db-test
  (:require [cavm.h2 :as h2])
  (:require [cavm.db :as cdb])
  (:use cavm.query.sources)
  (:require [clojure.test :as ct]))

(def eps 0.000001)

(defn- nearly-equal [e a b]
  (and (= (count a) (count b))
       (reduce (fn [acc pair] (and acc (< (java.lang.Math/abs (apply - pair)) e)))
               true
               (map vector a b))))

(defn matrix1 [db]
  (ct/testing "tsv matrix from memory"
    (cdb/write-matrix
      db
      [{:name "id1" :time (org.joda.time.DateTime. 2014 1 1 0 0 0 0) :hash "1234"}]
      {:name "id1"}
      (fn [] (with-meta 
               [(with-meta [1.1 1.2] {:probe "probe1"})
                (with-meta [2.1 2.2] {:probe "probe2"}) ]
               {:samples ["sample1" "sample2"]}))
      nil
      false)
    (let [exp (cdb/run-query db {:select [:name] :from [:experiments]})
          samples (cdb/run-query db {:select [:*] :from [:exp_samples]})
          probes (cdb/run-query db {:select [:*] :from [:probes]})]
      (ct/is (= exp [{:NAME "id1"}]))
      (ct/is (= samples
                [{:NAME "sample1" :I 0 :EXPERIMENTS_ID 1}
                 {:NAME "sample2" :I 1 :EXPERIMENTS_ID 1}]))
      (ct/is (= probes
                [{:NAME "probe1" :ID 1 :EID 1}
                 {:NAME "probe2" :ID 2 :EID 1}])))))

; clojure.test fixtures don't work with nested tests, so we
; have to invoke fixtures ourselves.
(defn run-tests [fixture]
  (doseq [t [matrix1]]
    (fixture t)))


(ct/deftest test-h2
  (run-tests
    (fn [f]
      (let [db (h2/create-db2 "test" {:subprotocol "h2:mem"})]
        (f db)
        (cdb/close db)))))
