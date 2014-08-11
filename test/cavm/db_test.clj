(ns cavm.db-test
  (:require [clojure.java.io :as io])
  (:require [cavm.h2 :as h2])
  (:require [cavm.db :as cdb])
  (:require [cavm.readers :as cr])
  (:require [cgdata.core :as cgdata])
  (:require [cavm.loader :refer [loader]])
  (:require [cavm.cgdata]) ; register reader methods
  (:require [honeysql.types :as hsqltypes])
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
      (fn [] {:fields [{:rows [0 1]
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
                       {:rows [1.1 1.2] :field "probe1"}
                       {:rows [2.1 2.2] :field "probe2"}]
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
          data (cdb/fetch db [{:table "id1"
                               :columns ["probe1" "probe2"]
                               :samples ["sample2" "sample1"]}])
          data2 (cdb/fetch db [{:table "id1"
                                :columns ["probe1" "probe2"]
                                :samples ["sample1" "sample3"]}])]
      (ct/is (= exp [{:name "id1"}]))
      (ct/is (= samples
                [{:name "sample1"}
                 {:name "sample2"}]))
      (ct/is (= probes
                [{:name "sampleID" :id 1 :dataset_id 1}
                 {:name "position" :id 2 :dataset_id 1}
                 {:name "genes"    :id 3 :dataset_id 1}
                 {:name "probe1"   :id 4 :dataset_id 1}
                 {:name "probe2"   :id 5 :dataset_id 1}]))
      (ct/is (= positions
                [{:field_id 2
                  :row 0
                  :chrom "chr1"
                  :chromstart 1200
                  :chromend 1300
                  :bin 4681
                  :strand "-"}
                 {:field_id 2
                  :row 1
                  :chrom "chr2"
                  :chromstart 300
                  :chromend 1200
                  :bin 4681
                  :strand "+"}]))
      (ct/is (= genes
                [{:field_id 3 :row 0 :gene "FOXM1"}
                 {:field_id 3 :row 0 :gene "CUL2"}
                 {:field_id 3 :row 1 :gene "ACK"}
                 {:field_id 3 :row 1 :gene "BLAH"}]))
      (let [[probe1 probe2]
            (vec (map #(into [] %) (map ((first data) :data) ["probe1" "probe2"])))]
        (ct/is (nearly-equal 0.0001 probe1 [1.2 1.1]))
        (ct/is (nearly-equal 0.0001 probe2 [2.2 2.1])))
      (let [[probe1 probe2]
            (vec (map #(into [] %) (map ((first data2) :data) ["probe1" "probe2"])))]
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
          probes (cdb/run-query db {:select [:*] :from [:field]})
          data (cdb/fetch db [{:table "matrix"
                               :columns ["probe1" "probe2"]
                               :samples ["sample2" "sample1"]}])]
      (ct/is (= exp [{:name "matrix"}]))
      (ct/is (= samples
                [{:name "sample1"}
                 {:name "sample2"}
                 {:name "sample3"}
                 {:name "sample4"}]))
      (ct/is (= probes
                [{:name "sampleID" :id 1 :dataset_id 1}
                 {:name "probe1" :id 2 :dataset_id 1}
                 {:name "probe2" :id 3 :dataset_id 1}
                 {:name "probe3" :id 4 :dataset_id 1}
                 {:name "probe4" :id 5 :dataset_id 1}
                 {:name "probe5" :id 6 :dataset_id 1} ]))
      (let [[probe1 probe2]
            (vec (map #(into [] %) (map ((first data) :data) ["probe1" "probe2"])))]
        (ct/is (nearly-equal 0.0001 probe1 [2.1 1.1]))
        (ct/is (nearly-equal 0.0001 probe2 [2.2 1.2]))))))

(defn matrix2-reload [db]
  (ct/testing "reload tsv matrix from file"
    (loader db detector docroot "test/cavm/test_inputs/matrix")
    (loader db detector docroot "test/cavm/test_inputs/matrix" true)
    (let [exp (cdb/run-query db {:select [:name] :from [:dataset]})
          samples (cdb/run-query db
                                 {:select [[:value :name]]
                                  :from [:field]
                                  :where [:= :name "sampleID"]
                                  :left-join [:feature [:= :field_id :field.id]
                                              :code [:= :feature.id :feature_id]]
                                  :order-by [:ordering]})
          probes (cdb/run-query db {:select [:*] :from [:field]})
          data (cdb/fetch db [{:table "matrix"
                               :columns ["probe1" "probe2"]
                               :samples ["sample2" "sample1"]}])]
      (ct/is (= exp [{:name "matrix"}]))
      (ct/is (= samples
                [{:name "sample1"}
                 {:name "sample2"}
                 {:name "sample3"}
                 {:name "sample4"}]))
      (ct/is (= probes
                [{:name "sampleID" :id 7 :dataset_id 1}
                 {:name "probe1" :id 8 :dataset_id 1}
                 {:name "probe2" :id 9 :dataset_id 1}
                 {:name "probe3" :id 10 :dataset_id 1}
                 {:name "probe4" :id 11 :dataset_id 1}
                 {:name "probe5" :id 12 :dataset_id 1} ]))
      (let [[probe1 probe2]
            (vec (map #(into [] %) (map ((first data) :data) ["probe1" "probe2"])))]
        (ct/is (nearly-equal 0.0001 probe1 [2.1 1.1]))
        (ct/is (nearly-equal 0.0001 probe2 [2.2 1.2]))))))

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
      (ct/is (= exp [{:name "cgdata_matrix"}]))
      (ct/is (= samples
                [{:name "sample1"}
                 {:name "sample2"}
                 {:name "sample3"}
                 {:name "sample4"}]))
      (ct/is (= probes
                [{:name "sampleID" :id 1 :dataset_id 1}
                 {:name "probe1" :id 2 :dataset_id 1}
                 {:name "probe2" :id 3 :dataset_id 1}
                 {:name "probe3" :id 4 :dataset_id 1}
                 {:name "probe4" :id 5 :dataset_id 1}
                 {:name "probe5" :id 6 :dataset_id 1}])))))

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
          genes (cdb/run-query db {:select [:*] :from [:field_gene]})
          probe-field (:id (first (cdb/run-query db {:select [:id] :from [:field] :where [:= :name "name"]})))
          data (map :pid (cdb/run-query db {:select [[(hsqltypes/read-sql-call [:unpackValue probe-field :row]) :pid]]
                                            :from [:field_position]}))]

      (ct/is (= probemap [{:name "probes"}]))
      (ct/is (= (set probes)
                (set [{:value "probe1"}
                      {:value "probe2"}
                      {:value "probe3"}
                      {:value "probe4"}
                      {:value "probe5"}
                      {:value "probe6"}
                      {:value "probe7"}
                      {:value "probe8"}
                      {:value "probe9"}])))
      (ct/is (= (set genes)
                (set
                  [{:field_id 3 :row 0 :gene "GENEA"}
                   {:field_id 3 :row 2 :gene "GENEA"}
                   {:field_id 3 :row 4 :gene "GENEB"}
                   {:field_id 3 :row 6 :gene "GENEB"}
                   {:field_id 3 :row 6 :gene "GENEC"}
                   {:field_id 3 :row 7 :gene "GENED"}
                   {:field_id 3 :row 8 :gene "GENEE"}])))
      (ct/is (= data ["probe1" "probe5" "probe2" "probe6" "probe3" "probe7" "probe4" "probe8" "probe9"])))))

(defn detect-cgdata-clinical [db]
  (ct/testing "detect cgdata clinical"
    (let [{file-type :file-type} (detector "test/cavm/test_inputs/clinical_matrix")]
      (ct/is (= file-type :cgdata.core/clinical)))))

(defn clinical1 [db]
  (ct/testing "cgdata clinical matrix"
    (loader db detector docroot "test/cavm/test_inputs/clinical_matrix")
    (let [exp (cdb/run-query db {:select [:name :type] :from [:dataset]})
          samples (cdb/run-query db
                                 {:select [[:value :name]]
                                  :from [:field]
                                  :where [:= :name "sampleID"]
                                  :left-join [:feature [:= :field_id :field.id]
                                              :code [:= :feature.id :feature_id]]
                                  :order-by [:ordering]})
          probes (cdb/run-query db {:select [:*] :from [:field]})]
      (ct/is (= exp [{:name "clinical_matrix" :type "clinicalMatrix"}]))
      (ct/is (= samples
                [{:name "sample1"}
                 {:name "sample2"}
                 {:name "sample3"}
                 {:name "sample4"}
                 {:name "sample5"}]))
      (ct/is (= probes
                [{:name "sampleID" :id 1 :dataset_id 1}
                 {:name "probe1" :id 2 :dataset_id 1}
                 {:name "probe2" :id 3 :dataset_id 1}
                 {:name "probe3" :id 4 :dataset_id 1}
                 {:name "probe4" :id 5 :dataset_id 1}])))))

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
                      (:id))
        codes (->> {:select [:ordering :value]
                    :from [:field]
                    :left-join [:feature [:= :field.id :field_id]
                                :code [:= :feature.id :feature_id]]
                    :where [:= :field_id field-id]}
                   (cdb/run-query db)
                   (#(for [{:keys [ordering value]} %] [ordering value]))
                   (into {}))]
    (->> [{:table table
           :columns [field]
           :samples samples}]
         (cdb/fetch db)
         (first)
         (#(% :data))
         (#(% field))
         (into [])
         (map (comp codes int)))))

(defn mutation1 [db]
  (ct/testing "cgdata mutation"
    (loader db detector docroot "test/cavm/test_inputs/mutation")
    (let [effect (get-categorical db "mutation" "effect" ["sample1" "sample2"])
          reference (get-categorical db "mutation" "ref" ["sample1" "sample2"])
          alt (get-categorical db "mutation" "alt" ["sample1" "sample2"])
          amino-acid (get-categorical db "mutation" "amino-acid" ["sample1" "sample2"])
          dataset (first (cdb/run-query db {:select [:*] :from [:dataset]}))]

      (ct/is (= effect ["frameshift_variant"
                        "missense_variant"
                        "missense_variant"]))
      (ct/is (= reference ["G" "G" "C"]))
      (ct/is (= alt ["A" "T" "T"]))
      (ct/is (= amino-acid ["R151W" "F1384L" "R1011K"]))
      (ct/is (= (select-keys dataset [:name :datasubtype])
                {:name "mutation" :datasubtype "somatic mutation"})))))

(defn mutation2 [db]
  (ct/testing "cgdata mutation"
    (loader db detector docroot "test/cavm/test_inputs/mutation2")
    (let [effect (get-categorical db "mutation2" "effect" ["TCGA-F2-6879-01"])
          reference (get-categorical db "mutation2" "ref" ["TCGA-F2-6879-01"])
          alt (get-categorical db "mutation2" "alt" ["TCGA-F2-6879-01"])
          amino-acid (get-categorical db "mutation2" "amino-acid" ["TCGA-F2-6879-01"])]

      (ct/is (= effect ["Splice_Site"
                        "Missense_Mutation"
                        "RNA"
                        "Missense_Mutation"
                        "Missense_Mutation"
                        "Missense_Mutation"
                        "RNA"
                        "Silent"
                        "Missense_Mutation"]))
      (ct/is (= reference ["C" "A" "A" "G" "T" "C" "T" "C" "G"]))
      (ct/is (= alt ["T" "T" "G" "A" "C" "A" "C" "T" "A"])))))

(defn gene-pred1 [db]
  (ct/testing "gene predition file"
    (loader db detector docroot "test/cavm/test_inputs/refGene")
    (let [chrom-field (:id (first (cdb/run-query db
                                                 {:select [:id]
                                                  :from [:field]
                                                  :where [:= :name "chrom"]})))

          data (cdb/run-query
                 db
                 {:select [:gene [(hsqltypes/read-sql-call
                                    [:unpackValue chrom-field :row]) :chrom]]
                  :from [:field-gene]})]

      (ct/is (= (set data)
                #{{:gene "MTVR2" :chrom "chr17"}
                  {:gene "LOC100506860" :chrom "chr7"}
                  {:gene "CHMP1B" :chrom "chr18"}
                  {:gene "LOC441204" :chrom "chr7"}
                  {:gene "TCOF1" :chrom "chr5"}
                  {:gene "NSRP1" :chrom "chr17"}
                  {:gene "SPPL3" :chrom "chr12"}
                  {:gene "OPA3" :chrom "chr19"}
                  {:gene "OPA1" :chrom "chr3"}})))))

; XXX test that cgdata defaults to genomicMatrix if not specified

; clojure.test fixtures don't work with nested tests, so we
; have to invoke fixtures ourselves.
(defn run-tests [fixture]
  (doseq [t [matrix1 detect-matrix matrix2 detect-cgdata-genomic matrix3
             matrix2-reload
             detect-cgdata-probemap probemap1
             detect-cgdata-clinical clinical1
             detect-cgdata-mutation mutation1 mutation2
             gene-pred1
             ]]
    (fixture t)))

(ct/deftest test-h2
  (run-tests
    (fn [f]
      (let [db (h2/create-xenadb "test" {:subprotocol "h2:mem"})]
        (try (f db) ; 'finally' ensures our teardown always runs
          (finally (cdb/close db)))))))

