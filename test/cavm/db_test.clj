(ns cavm.db-test
  (:require [clojure.java.io :as io])
  (:require [cavm.h2 :as h2])
  (:require [cavm.db :as cdb])
  (:require [cavm.readers :as cr])
  (:require [cgdata.core :as cgdata])
  (:require [cavm.loader :refer [loader]])
  (:require [cavm.cgdata]) ; register reader methods
  (:use cavm.query.sources)
  (:require [clojure.test :as ct]))

(def eps 0.000001)

(defn- nearly-equal [e a b]
  (and (= (count a) (count b))
       (reduce (fn [acc pair]
                 (or (let [[x y] pair]
                       (and
                         (Double/isNaN x)
                         (Double/isNaN y)))
                     (and acc (< (java.lang.Math/abs (apply - pair)) e))))
               true
               (map vector a b))))

; XXX how to test scores? Could add a call to fetch. Could also
; consider adding a transform that calls fetch, if we ever get
; a sql transform layer implemented.
(defn matrix1 [db]
  (ct/testing "tsv matrix from memory"
    (cdb/write-matrix
      db
      "id1"
      [{:name "id1" :time (org.joda.time.DateTime. 2014 1 1 0 0 0 0) :hash "1234"}]
      {}
      (fn [] {:fields [{:scores [0 1]
                        :field "sampleID"
                        :valueType "category"
                        :feature {:state ["sample1" "sample2"]
                                  :order {"sample1" 0 "sample2" 1}}}
                       {:field "position"
                        :valueType "position"
                        :rows [{:chrom "chr1"
                                :chromStart 1200
                                :chromEnd 1300
                                :strand "-"}
                               {:chrom "chr2"
                                :chromStart 300
                                :chromEnd 1200
                                :strand "+"}]}
                       {:field "genes"
                        :valueType "genes"
                        :rows [["FOXM1" "CUL2"]
                               ["ACK" "BLAH"]]}
                       {:scores [1.1 1.2] :field "probe1"}
                       {:scores [2.1 2.2] :field "probe2"}]
               :samples ["sample1" "sample2"]})
      nil
      false)

    (let [exp (cdb/run-query db {:select [:name] :from [:dataset]})
          samples (cdb/run-query db
                                 {:select [[:value :name]]
                                  :from [:field]
                                  :where [:= :name "sampleID"]
                                  :left-join [:feature [:= :field_id :field.id]
                                              :code [:= :feature.id :feature_id]]
                                  :order-by [:ordering]})
          probes (cdb/run-query db {:select [:*] :from [:field]})
          positions (cdb/run-query db {:select [:*] :from [:field_position]})
          genes (cdb/run-query db {:select [:*] :from [:field_gene]})
          data (cdb/fetch db [{'table "id1"
                               'columns ["probe1" "probe2"]
                               'samples ["sample2" "sample1"]}])
          data2 (cdb/fetch db [{'table "id1"
                                'columns ["probe1" "probe2"]
                                'samples ["sample1" "sample3"]}])]
      (ct/is (= exp [{:NAME "id1"}]))
      (ct/is (= samples
                [{:NAME "sample1"}
                 {:NAME "sample2"}]))
      (ct/is (= probes
                [{:NAME "sampleID" :ID 1 :DATASET_ID 1}
                 {:NAME "position" :ID 2 :DATASET_ID 1}
                 {:NAME "genes"    :ID 3 :DATASET_ID 1}
                 {:NAME "probe1"   :ID 4 :DATASET_ID 1}
                 {:NAME "probe2"   :ID 5 :DATASET_ID 1}]))
      (ct/is (= positions
                [{:FIELD_ID 2
                  :ROW 0
                  :CHROM "chr1"
                  :CHROMSTART 1200
                  :CHROMEND 1300
                  :BIN 4681
                  :STRAND "-"}
                 {:FIELD_ID 2
                  :ROW 1
                  :CHROM "chr2"
                  :CHROMSTART 300
                  :CHROMEND 1200
                  :BIN 4681
                  :STRAND "+"}]))
      (ct/is (= genes
                [{:FIELD_ID 3 :ROW 0 :GENE "FOXM1"}
                 {:FIELD_ID 3 :ROW 0 :GENE "CUL2"}
                 {:FIELD_ID 3 :ROW 1 :GENE "ACK"}
                 {:FIELD_ID 3 :ROW 1 :GENE "BLAH"}]))
      (let [[probe1 probe2]
            (vec (map #(into [] %) (map ((first data) 'data) ["probe1" "probe2"])))]
        (ct/is (nearly-equal 0.0001 probe1 [1.2 1.1]))
        (ct/is (nearly-equal 0.0001 probe2 [2.2 2.1])))
      (let [[probe1 probe2]
            (vec (map #(into [] %) (map ((first data2) 'data) ["probe1" "probe2"])))]
        (ct/is (nearly-equal 0.0001 probe1 [1.1 Double/NaN]))
        (ct/is (nearly-equal 0.0001 probe2 [2.1 Double/NaN]))))))

(def docroot "test/cavm/test_inputs")

(def detector
  (cr/detector
    docroot
    cgdata/detect-cgdata
    cgdata/detect-tsv))

(defn detect-matrix [db]
  (ct/testing "detect tsv matrix"
    (let [{file-type :file-type} (detector "test/cavm/test_inputs/matrix")]
      (ct/is (= file-type :cgdata.core/tsv)))))

(defn matrix2 [db]
  (ct/testing "tsv matrix from file"
    (loader db detector docroot "test/cavm/test_inputs/matrix") ; odd that loader & detector both require docroot
    (let [exp (cdb/run-query db {:select [:name] :from [:dataset]})
          samples (cdb/run-query db
                                 {:select [[:value :name]]
                                  :from [:field]
                                  :where [:= :name "sampleID"]
                                  :left-join [:feature [:= :field_id :field.id]
                                              :code [:= :feature.id :feature_id]]
                                  :order-by [:ordering]})
          probes (cdb/run-query db {:select [:*] :from [:field]})]
      (ct/is (= exp [{:NAME "matrix"}]))
      (ct/is (= samples
                [{:NAME "sample1"}
                 {:NAME "sample2"}
                 {:NAME "sample3"}
                 {:NAME "sample4"}]))
      (ct/is (= probes
                [{:NAME "sampleID" :ID 1 :DATASET_ID 1}
                 {:NAME "probe1" :ID 2 :DATASET_ID 1}
                 {:NAME "probe2" :ID 3 :DATASET_ID 1}
                 {:NAME "probe3" :ID 4 :DATASET_ID 1}
                 {:NAME "probe4" :ID 5 :DATASET_ID 1}
                 {:NAME "probe5" :ID 6 :DATASET_ID 1} ])))))

(defn detect-cgdata-genomic [db]
  (ct/testing "detect cgdata genomic"
    (let [{file-type :file-type} (detector "test/cavm/test_inputs/cgdata_matrix")]
      (ct/is (= file-type :cgdata.core/genomic)))))

(defn matrix3 [db]
  (ct/testing "cgdata genomic matrix"
    (loader db detector docroot "test/cavm/test_inputs/cgdata_matrix")
    (let [exp (cdb/run-query db {:select [:name] :from [:dataset]})
          samples (cdb/run-query db
                                 {:select [[:value :name]]
                                  :from [:field]
                                  :where [:= :name "sampleID"]
                                  :left-join [:feature [:= :field_id :field.id]
                                              :code [:= :feature.id :feature_id]]
                                  :order-by [:ordering]})
          probes (cdb/run-query db {:select [:*] :from [:field]})]
      (ct/is (= exp [{:NAME "cgdata_matrix"}]))
      (ct/is (= samples
                [{:NAME "sample1"}
                 {:NAME "sample2"}
                 {:NAME "sample3"}
                 {:NAME "sample4"}]))
      (ct/is (= probes
                [{:NAME "sampleID" :ID 1 :DATASET_ID 1}
                 {:NAME "probe1" :ID 2 :DATASET_ID 1}
                 {:NAME "probe2" :ID 3 :DATASET_ID 1}
                 {:NAME "probe3" :ID 4 :DATASET_ID 1}
                 {:NAME "probe4" :ID 5 :DATASET_ID 1}
                 {:NAME "probe5" :ID 6 :DATASET_ID 1}])))))

(defn detect-cgdata-probemap [db]
  (ct/testing "detect cgdata probemap"
    (let [{file-type :file-type} (detector "test/cavm/test_inputs/probes")]
      (ct/is (= file-type :cgdata.core/probemap)))))

(defn probemap1 [db]
  (ct/testing "cgdata probemap"
    (loader db detector docroot "test/cavm/test_inputs/probes")
    (let [probemap (cdb/run-query db {:select [:name] :from [:dataset]})
          fields (cdb/run-query db {:select [:*] :from [:field]})
          probes (cdb/run-query db {:select [:value] :from [:code]})
          positions (cdb/run-query db {:select [:*] :from [:field_position]})
          genes (cdb/run-query db {:select [:*] :from [:field_gene]})]

      (ct/is (= probemap [{:NAME "probes"}]))
      (ct/is (= (set probes)
                (set [{:VALUE "probe1"}
                      {:VALUE "probe2"}
                      {:VALUE "probe3"}
                      {:VALUE "probe4"}
                      {:VALUE "probe5"}
                      {:VALUE "probe6"}
                      {:VALUE "probe7"}
                      {:VALUE "probe8"}
                      {:VALUE "probe9"}])))
      (ct/is (= (set genes)
                (set
                  [{:FIELD_ID 3 :ROW 0 :GENE "GENEA"}
                   {:FIELD_ID 3 :ROW 2 :GENE "GENEA"}
                   {:FIELD_ID 3 :ROW 4 :GENE "GENEB"}
                   {:FIELD_ID 3 :ROW 6 :GENE "GENEB"}
                   {:FIELD_ID 3 :ROW 6 :GENE "GENEC"}
                   {:FIELD_ID 3 :ROW 7 :GENE "GENED"}
                   {:FIELD_ID 3 :ROW 8 :GENE "GENEE"}]))))))

(defn detect-cgdata-clinical [db]
  (ct/testing "detect cgdata clinical"
    (let [{file-type :file-type} (detector "test/cavm/test_inputs/clinical_matrix")]
      (ct/is (= file-type :cgdata.core/clinical)))))

(defn clinical1 [db]
  (ct/testing "cgdata clinical matrix"
    (loader db detector docroot "test/cavm/test_inputs/clinical_matrix")
    (let [exp (cdb/run-query db {:select [:name] :from [:dataset]})
          samples (cdb/run-query db
                                 {:select [[:value :name]]
                                  :from [:field]
                                  :where [:= :name "sampleID"]
                                  :left-join [:feature [:= :field_id :field.id]
                                              :code [:= :feature.id :feature_id]]
                                  :order-by [:ordering]})
          probes (cdb/run-query db {:select [:*] :from [:field]})]
      (ct/is (= exp [{:NAME "clinical_matrix"}]))
      (ct/is (= samples
                [{:NAME "sample1"}
                 {:NAME "sample2"}
                 {:NAME "sample3"}
                 {:NAME "sample4"}
                 {:NAME "sample5"}]))
      (ct/is (= probes
                [{:NAME "sampleID" :ID 1 :DATASET_ID 1}
                 {:NAME "probe1" :ID 2 :DATASET_ID 1}
                 {:NAME "probe2" :ID 3 :DATASET_ID 1}
                 {:NAME "probe3" :ID 4 :DATASET_ID 1}
                 {:NAME "probe4" :ID 5 :DATASET_ID 1}])))))

(defn detect-cgdata-mutation [db]
  (ct/testing "detect cgdata mutation"
    (let [{file-type :file-type} (detector "test/cavm/test_inputs/mutation")]
      (ct/is (= file-type :cgdata.core/mutation)))))

(defn get-categorical [db table field samples]
  (let [field-id (->> {:select [:field.id]
                       :from [:dataset]
                       :left-join [:field [:= :dataset.id :dataset_id]]
                       :where [:and
                               [:= :field.name field]
                               [:= :dataset.name table]]}
                      (cdb/run-query db)
                      (first)
                      (:ID))
        codes (->> {:select [:ordering :value]
                    :from [:field]
                    :left-join [:feature [:= :field.id :field_id]
                                :code [:= :feature.id :feature_id]]
                    :where [:= :field_id field-id]}
                   (cdb/run-query db)
                   (#(for [{:keys [ORDERING VALUE]} %] [ORDERING VALUE]))
                   (into {}))]
    (->> [{'table table
           'columns [field]
           'samples samples}]
         (cdb/fetch db)
         (first)
         (#(% 'data))
         (#(% field))
         (into [])
         (map (comp codes int)))))

(defn mutation1 [db]
  (ct/testing "cgdata mutation"
    (loader db detector docroot "test/cavm/test_inputs/mutation")
    (let [effect (get-categorical db "mutation" "effect" ["sample1" "sample2"])
          reference (get-categorical db "mutation" "reference" ["sample1" "sample2"])
          alt (get-categorical db "mutation" "alt" ["sample1" "sample2"])
          amino-acid (get-categorical db "mutation" "amino-acid" ["sample1" "sample2"])]

      (ct/is (= effect ["frameshift_variant"
                        "missense_variant"
                        "missense_variant"]))
      (ct/is (= reference ["G" "G" "C"]))
      (ct/is (= alt ["A" "T" "T"]))
      (ct/is (= amino-acid ["R151W" "F1384L" "R1011K"])))))

; XXX test that cgdata defaults to genomicMatrix if not specified

; clojure.test fixtures don't work with nested tests, so we
; have to invoke fixtures ourselves.
(defn run-tests [fixture]
  (doseq [t [matrix1 detect-matrix matrix2 detect-cgdata-genomic matrix3
             detect-cgdata-probemap probemap1
             detect-cgdata-clinical clinical1
             detect-cgdata-mutation mutation1]]
    (fixture t)))

(ct/deftest test-h2
  (run-tests
    (fn [f]
      (let [db (h2/create-xenadb "test" {:subprotocol "h2:mem"})]
        (try (f db) ; 'finally' ensures our teardown always runs
          (finally (cdb/close db)))))))

