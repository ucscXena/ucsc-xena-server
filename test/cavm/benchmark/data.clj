(ns cavm.benchmark.data
  (:require [clojure.java.io :as io])
  (:require [clojure.string :as s]))

(def probemaps [{:id "AffymetrixHumanExon1ST_hg18"
                 :key :probemap-large
                 :remote-id "probeMap/AffymetrixHumanExon1ST_hg18"
                 :host "https://ucscpublic.xenahubs.net"}])

(def cnv [^:pp {:id "20170119_final_consensus_copynumber_donor"
                :key :cnv-large
                :remote-id "20170119_final_consensus_copynumber_donor"
                :host "\017\021\000\007\033\133\133\126\037\026\017\022\002\112\037\000\032\026\000\024\026\012\101\033\013\021"}])

(def matrix [{:id "AgilentG4502A_07_3"
              :key :matrix-medium
              :remote-id "TCGA.BRCA.sampleMap/AgilentG4502A_07_3"
              :host "https://tcga.xenahubs.net"}])

(def refgene [{:id "gencode_good_hg19"
               :key :refgene-hg19
               :remote-id "gencode_good_hg19"
               :host "https://reference.xenahubs.net"}])

(def datasets (concat probemaps cnv matrix refgene))
