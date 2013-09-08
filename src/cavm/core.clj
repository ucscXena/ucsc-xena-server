(ns cavm.core
  (:require [clojure.string :as string])
  (:use [cavm.h2 :only [create load-exp load-probemap del-exp datasets with-db create-db]]) ; XXX use model instead?
  (:require [cavm.test-data :as data])
  (:require [clojure.java.io :as io])
  (:require [clj-time.coerce :refer [from-long]])
  (:require [clj-time.core :refer [now]])
  (:require [noir.server :as server])
  (:require [me.raynes.fs :as fs])
  (:require [clojure.tools.cli :refer [cli]]))

(defn- tabbed [line]
  (string/split line #"\t"))

(defn- parseFloatNA [str]
  (if (or (= str "NA") (= str "nan"))
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

; Return seq of scores, probes X samples, with samples as metadata
(defn- genomic-matrix-data [lines]
  (with-meta (map probe-line (rest lines))
             {:samples (rest (tabbed (first lines)))}))

(def data-path (str fs/*cwd*))

(defn- in-data-path [path]
  (boolean (fs/child-of? data-path path))) ; XXX not working??

(defn- filehash [file]
  "fixme")

(defn load-tsv-file [file]
  (let [ts (file-time file)
        h (filehash file)]
    (with-open [in (io/reader file)]
      (load-exp file ts h (genomic-matrix-data (line-seq in))))))

(defn- loadtest [name m n]
  (create)
  (let [mi (Integer. m)
        ni (Integer. n)]
  (load-exp name (now) "FIXME" (genomic-matrix-data (data/matrix mi ni)))))


(defn- split-no-empty [in pat]
  (filter #(not (= "" %)) (string/split in pat)))

;
; cgData probemap
;

(defn- probemap-row [row]
  (let [[name genes chrom start end strand] (string/split row #"\t")]
    {:name name
     :genes (split-no-empty genes #",")
     :chrom chrom
     :chromStart (. Integer parseInt start)
     :chromEnd (. Integer parseInt end)
     :strand strand}))

(defn load-probemap-file [file]
  (let [ts (file-time file)
        h (filehash file)]
    (with-open [in (io/reader file)]
      (load-probemap file ts h (map probemap-row (line-seq in))))))
;
; web services

(def port 7222)

(defn wrap-access-control [handler]
  (fn [request]
    (let [response (handler request)]
      (if (= nil response)
        response              ; If we add headers to nil 404 become 200.
        (-> response
            (assoc-in [:headers "Access-Control-Allow-Origin"] "https://tcga1.kilokluster.ucsc.edu")
            (assoc-in [:headers "Access-Control-Allow-Headers"] "Cancer-Browser-Api"))))))


(server/load-views-ns 'cavm.views)

(server/add-middleware wrap-access-control)

(defn- del-datasets [args]
  (dorun (map del-exp args)))

(defn- print-datasets []
  (dorun (map println (datasets))))

(defn- serv []
  (server/start port {:mode :dev
                      :ns 'cavm}))

(defn- timefn [fn]
  (with-out-str (time (fn))))

(defn- load-tsv-report [load-fn file]
  (try
    (load-fn file)
    (catch java.lang.Throwable e
      (binding [*out* *err*]
        (println "Error loading file" file)
        (.printStackTrace e)))))

(defn- loadfiles [load-fn args]
  (when (not (> (count args) 0))
    (println "Usage\nload <filename>")
    (System/exit 0))

  ; Skip files outside the designated path, which for now is CWD.
  (let [{in-path true, not-in-path false} (group-by in-data-path args)]
    (when not-in-path
      (binding [*out* *err*]
        (println "These files are outside the CAVM data path and will not be served:")
        (println (string/join "\n" (in-path false)))))
    (create)
    (println "Loading " (count in-path) " file(s)")
    (dorun (map #(do (print %2 %1 "") (time (load-tsv-report load-fn %1)))
                in-path
                (range (count in-path) 0 -1)))))

(def ^:private argspec
  [["-s" "Start web server" :flag true :default false]
   ["-p" "Load probemaps" :flag true :default false]
   ["-h" "--help" "Show help" :default false :flag true]
   ["-d" "Database to use" :default "file:///data/TCGA/craft/h2/cavm.h2"]
   ["-t" "Load test data  <name> <samples> <probes>" :flag true]])

(defn -main [& args]
  (let [[opts extra usage] (apply cli (cons args argspec))
        load-fn (if (:p opts) load-probemap-file load-tsv-file)]
    (with-db (create-db (:d opts))
      (cond
        (:help opts) (println usage)
        (:s opts) (serv)
        (:t opts) (if (not (= 3 (count extra)))
                    (println usage)
                    (apply loadtest extra))
        :else (loadfiles load-fn extra))))

  (shutdown-agents))
