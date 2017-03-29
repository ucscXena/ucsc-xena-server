(ns cavm.benchmark.data
  (:require [clojure.java.io :as io])
  (:require [clojure.string :as s])
  )

(def probemaps [{:id "AffymetrixHumanExon1ST_hg18"
                 :probes ["2315223"
                          "2315102"
                          "2315232"
                          "2315200"
                          "2315116"
                          "2315165"
                          "2315238"
                          "2315162"
                          "2315126"
                          "2315120"
                          "2315244"
                          "2315200"
                          "2315195"
                          "2315183"
                          "2315188"
                          "2315156"
                          "2315236"
                          "2315132"
                          "2315179"
                          "2315150"]}])

(def cnv [{:id "20170119_final_consensus_copynumber_donor"}])

(defn tsv-lines [txt]
  (map #(s/split % #"\t") (s/split txt #"\n")))

(defn gene-line [[chr start end gene]]
  {:chr chr
   :start (Integer/parseInt (s/trim start))
   :end (Integer/parseInt (s/trim end))
   :gene gene})

(def genes
  (map gene-line (tsv-lines (slurp (io/resource "benchmark/600genes")))))
