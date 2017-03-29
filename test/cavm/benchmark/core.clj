(ns cavm.benchmark.core
  (:require [clojure.java.io :as io])
  (:require [cavm.benchmark.data :as benchmark-data])
  (:require [cavm.benchmark.install :refer [install]])
  (:require [cavm.h2 :as h2])
  (:require [cavm.db :as cdb]))

; This should be fine for long-running tasks. As tasks become shorter
; hotspot effects become important, and a tool like criterium is required.
(defmacro timeit [expr]
  `(let [start# (. System (nanoTime))]
     ~expr
     (/ (double (- (. System (nanoTime)) start#)) 1000000.0)))

(defn spy [msg x]
  (println msg x)
  x)

; XXX should probably validate the return values to some degree, so
; that we can see nothing is broken. Or do that in unit tests?
(defn probemap-probe [xena dataset probes]
  (timeit (cdb/column-query xena
                            {:select ["name" "position"]
                             :from [dataset]
                             :where [:in "name" probes]})))

(defn cnv-gene [xena dataset genes]
  (timeit (cdb/column-query xena
                            {:select ["position" "sampleID" "value"]
                             :from [dataset]
                             :where [:in "position" (mapv (juxt :chr :start :end) genes)]})))

(defn sort-genes [genes]
  (sort (fn [x y]
          (compare [(:chr x) (:start x)]
                   [(:chr y) (:start y)])) genes))

(def dbfile
  (str (io/file (System/getProperty "user.home") "xena-benchmark/database")))

(defn report [msg t]
  (format "%60s\t%f msec" msg t))

(defn -main [& args]
  (println "benchmark" args)

  (try
    (if (= args ["install"])
      (install (io/file (System/getProperty "user.home") "xena-install"))
      (let [xena (h2/create-xenadb dbfile)
            {:keys [id probes]} (first benchmark-data/probemaps)
            {cnv-id :id} (first benchmark-data/cnv)]
        (probemap-probe xena id (take 1 probes)) ; XXX initial query can be slow. Need to investigate.
        (println (report "1 probe from probemap of 1.4M rows" (probemap-probe xena id (take 1 (drop 5 probes)))))
        (println (report "2 probe from probemap of 1.4M rows" (probemap-probe xena id (take 2 (drop 10 probes)))))
        (println (report "3 sorted gene positions from CNV of 970k rows" (cnv-gene xena cnv-id (sort-genes (take 3 benchmark-data/genes)))))
        (println (report "30 sorted gene positions from CNV of 970k rows" (cnv-gene xena cnv-id (sort-genes (take 30 benchmark-data/genes)))))
        (println (report "300 sorted gene positions from CNV of 970k rows" (cnv-gene xena cnv-id (sort-genes (take 300 benchmark-data/genes)))))
        (println (report "3 gene positions from CNV of 970k rows" (cnv-gene xena cnv-id (take 3 benchmark-data/genes))))
        (println (report "30 gene positions from CNV of 970k rows" (cnv-gene xena cnv-id (take 30 benchmark-data/genes))))
        (println (report "300 gene positions from CNV of 970k rows" (cnv-gene xena cnv-id (take 300 benchmark-data/genes))))
        ))
    (finally
      (shutdown-agents))))
