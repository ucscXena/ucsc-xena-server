(ns cavm.benchmark.data
  (:require [clojure.java.io :as io])
  (:require [clojure.string :as s])
  )

(def probemaps [{:id "AffymetrixHumanExon1ST_hg18"
                 :remote-id "probeMap/AffymetrixHumanExon1ST_hg18"
                 :host "https://ucscpublic.xenahubs.net"
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

(def cnv [^:pp {:id "20170119_final_consensus_copynumber_donor"
                :remote-id "20170119_final_consensus_copynumber_donor"
                :host "\017\021\000\007\033\133\133\126\037\026\017\022\002\112\037\000\032\026\000\024\026\012\101\033\013\021"}])

(defn tsv-lines [txt]
  (map #(s/split % #"\t") (s/split txt #"\n")))

(defn gene-line [[chr start end gene]]
  {:chr chr
   :start (Integer/parseInt (s/trim start))
   :end (Integer/parseInt (s/trim end))
   :gene gene})

(def genes
  (map gene-line (tsv-lines (slurp (io/resource "benchmark/600genes")))))

(def datasets (concat probemaps cnv))
