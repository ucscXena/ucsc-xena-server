(ns cavm.db-test
  (:require [clojure.java.io :as io])
  (:require [cavm.h2 :as h2])
  (:require [cavm.db :as cdb])
  (:require [cavm.readers :as cr])
  (:require [cgdata.core :as cgdata])
  (:use cavm.query.sources)
  (:require [clojure.test :as ct]))

(def eps 0.000001)

(defn- nearly-equal [e a b]
  (and (= (count a) (count b))
       (reduce (fn [acc pair] (and acc (< (java.lang.Math/abs (apply - pair)) e)))
               true
               (map vector a b))))

; XXX how to test scores? Could add a call to fetch. Could also
; consider adding a transform that calls fetch, if we ever get
; a sql transform layer implemented.
(defn matrix1 [db]
  (ct/testing "tsv matrix from memory"
    (cdb/write-matrix
      db
      [{:name "id1" :time (org.joda.time.DateTime. 2014 1 1 0 0 0 0) :hash "1234"}]
      {:name "id1"}
      (fn [] {:fields [{:scores [1.1 1.2] :field "probe1"}
                       {:scores [2.1 2.2] :field "probe2"}]
               :samples ["sample1" "sample2"]})
      nil
      false)
    (let [exp (cdb/run-query db {:select [:name] :from [:experiments]})
          samples (cdb/run-query db {:select [:name] :from [:exp_samples] :order-by [:i]})
          probes (cdb/run-query db {:select [:*] :from [:probes]})
          data (comment (h2/genomic-read-req {'table "id1"
                          'columns ["probe1" "probe2"]
                          'samples ["sample1" "sample2"]}))]
      (ct/is (= exp [{:NAME "id1"}]))
      (ct/is (= samples
                [{:NAME "sample1"}
                 {:NAME "sample2"}]))
      (ct/is (= probes
                [{:NAME "probe1" :ID 1 :EID 1}
                 {:NAME "probe2" :ID 2 :EID 1}]))
      (comment (ct/is (= data
                [[1.1 1.2] [2.1 2.2]]))))))

(def detector
  (cr/detector
    cgdata/detect-cgdata
    cgdata/detect-tsv))

(defn detect-matrix [db]
  (ct/testing "detect tsv matrix"
    (let [{file-type :file-type} (detector "test/cavm/test_inputs/matrix")]
      (ct/is (= file-type :cgdata.core/tsv)))))

(defn matrix2 [db]
  (ct/testing "tsv matrix from file"
    (let [filename "test/cavm/test_inputs/matrix"
          {:keys [rfile metadata refs features data-fn]} @(:reader (detector filename))]

      (with-open [in (io/reader filename)]
        (cdb/write-matrix
          db
          nil            ; list of file name, hash, timestamp
          {:name filename}  ; json metadata
          (partial data-fn in)
          nil
          false)))
    (let [exp (cdb/run-query db {:select [:name] :from [:experiments]})
          samples (cdb/run-query db {:select [:name] :from [:exp_samples] :order-by [:i]})
          probes (cdb/run-query db {:select [:*] :from [:probes]})]
      (ct/is (= exp [{:NAME "test/cavm/test_inputs/matrix"}]))
      (ct/is (= samples
                [{:NAME "sample1"}
                 {:NAME "sample2"}
                 {:NAME "sample3"}
                 {:NAME "sample4"}]))
      (ct/is (= probes
                [{:NAME "probe1" :ID 1 :EID 1}
                 {:NAME "probe2" :ID 2 :EID 1}
                 {:NAME "probe3" :ID 3 :EID 1}
                 {:NAME "probe4" :ID 4 :EID 1}
                 {:NAME "probe5" :ID 5 :EID 1} ])))))

(defn detect-cgdata-genomic [db]
  (ct/testing "detect cgdata genomic"
    (let [{file-type :file-type} (detector "test/cavm/test_inputs/cgdata_matrix")]
      (ct/is (= file-type :cgdata.core/genomic)))))

(defn matrix3 [db]
  (ct/testing "cgdata genomic matrix"
    (let [filename "test/cavm/test_inputs/cgdata_matrix"
          {:keys [rfile metadata refs features data-fn]} @(:reader (detector filename))]

      (with-open [in (io/reader filename)]
        (cdb/write-matrix
          db
          nil ; XXX fix this
          {:name filename}
          (partial data-fn in)
          nil
          false)))
    (let [exp (cdb/run-query db {:select [:name] :from [:experiments]})
          samples (cdb/run-query db {:select [:name] :from [:exp_samples] :order-by [:i]})
          probes (cdb/run-query db {:select [:*] :from [:probes]})]
      (ct/is (= exp [{:NAME "test/cavm/test_inputs/cgdata_matrix"}]))
      (ct/is (= samples
                [{:NAME "sample1"}
                 {:NAME "sample2"}
                 {:NAME "sample3"}
                 {:NAME "sample4"}]))
      (ct/is (= probes
                [{:NAME "probe1" :ID 1 :EID 1}
                 {:NAME "probe2" :ID 2 :EID 1}
                 {:NAME "probe3" :ID 3 :EID 1}
                 {:NAME "probe4" :ID 4 :EID 1}
                 {:NAME "probe5" :ID 5 :EID 1}])))))

; XXX test that cgdata defaults to genomicMatrix if not specified
; XXX fix root path: our test depends on $HOME, which is broken
;     drop root from cgdata processing. Should be in a support lib.
; XXX test clinical
; XXX test probemaps

; clojure.test fixtures don't work with nested tests, so we
; have to invoke fixtures ourselves.
(defn run-tests [fixture]
  (doseq [t [matrix1 detect-matrix matrix2 detect-cgdata-genomic matrix3]]
    (fixture t)))

(ct/deftest test-h2
  (run-tests
    (fn [f]
      (let [db (h2/create-db2 "test" {:subprotocol "h2:mem"})]
        (try (f db) ; 'finally' ensures our teardown always runs
          (finally (cdb/close db)))))))

; (ct/run-tests)
