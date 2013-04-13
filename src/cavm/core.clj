(ns cavm.core
  (:require [clojure.string :as string])
  (:use cavm.h2)
  (:require [clojure.java.io :as io])
  (:use [clj-time.coerce :only (from-long)])
  (:gen-class))

(defn- tabbed [line]
  (string/split line #"\t"))

(defn- parseFloatNA [str]
  (if (= str "NA")
    Double/NaN
    (Float/parseFloat str)))

(defn- file-time [file]
  (from-long (.lastModified (java.io.File. file))))

; return seq of floats from probe line, with probe as metadata
; probeid value value value...
(defn- probe-line [line]
  (let [cols (tabbed line)]
    (with-meta (map parseFloatNA (rest cols))
               {:probe (first cols)})))

;    (with-meta (map #(Float/parseFloat %) (rest cols))

; Return seq of scores, probes X samples, with samples as metadata
(defn- genomic-matrix-data [lines]
  (with-meta (map probe-line (rest lines))
             {:samples (rest (tabbed (first lines)))}))

(defn- filehash [file]
  "blah")

(defn load-tsv-file [file]
  (let [ts (file-time file)
        h (filehash file)]
    (with-open [in (io/reader file)]
      (load-exp file ts h (genomic-matrix-data (line-seq in))))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (when (not (> (count args) 0))
    (println "Usage\nload <filename>")
    (System/exit 0))

  (create)
  (dorun (map load-tsv-file args))
  (shutdown-agents))
