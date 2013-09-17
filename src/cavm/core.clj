(ns cavm.core
  (:require [clojure.string :as string])
  (:use [cavm.h2 :only [create
                        load-exp
                        load-probemap
                        del-exp
                        datasets
                        with-db
                        clean-sources
                        create-db]]) ; XXX use model instead?
  (:require [digest])
  (:require [cavm.test-data :as data])
  (:require [clojure.java.io :as io])
  (:require [clj-time.coerce :refer [from-long]])
  (:require [clj-time.core :refer [now]])
  (:require [noir.server :as server])
  (:require [clojure.data.json :as json])
  (:require [me.raynes.fs :as fs])
  (:require [clojure.tools.cli :refer [cli]])
  (:require [cavm.cgdata :as cgdata])
  (:gen-class))

(defn- tabbed [line]
  (string/split line #"\t"))

(defn- parseFloatNA [str]
  (if (or (= str "NA") (= str "nan") (= str ""))
    Double/NaN
    (Float/parseFloat str)))

(defn- file-time [file]
  (from-long (.lastModified (java.io.File. file))))

(def data-path (str fs/*cwd*))

(defn- in-data-path [path]
  (boolean (fs/child-of? data-path path))) ; XXX not working with ".."??

(defn- filehash [file]
  (digest/sha1 (io/as-file file)))

;
; cgData genomicMatrix
;

(defn- infer-value-type [cols]
  (try
    (do
      (dorun (map parseFloatNA (take 10 (rest cols))))
      "float")
    (catch NumberFormatException e "category")))

(defmulti ^:private data-line
  (fn [features cols]
    (or (get (get features (first cols)) "valueType") ; using (get) handles nil
        (infer-value-type cols))))

; return seq of floats from probe line, with probe as metadata
; probeid value value value...
(defmethod ^:private data-line "float"
  [features cols]
  (let [feature (features (first cols))]
    (with-meta (map parseFloatNA (rest cols))
               {:probe (String. (first cols)) :feature feature :valueType "float"}))) ; copy the string, because string/split is evil

; update map with value for NA
(defn- nil-val [order]
  (assoc order "" Double/NaN))

(defn- ad-hoc-order
  "Provide default order from data order in file"
  [feature cols]
  (if (:order feature)
    feature
    (let [state (distinct cols)
          order (into {} (map vector state (range)))] ; XXX handle all values null?
      (assoc feature :state state :order order))))

(defmethod ^:private data-line "category"
  [features cols]
  (let [feature (features (first cols))
        feature (ad-hoc-order feature (rest cols))
        order (nil-val (:order feature))]
    (with-meta (map order (rest cols))
               {:probe (String. (first cols)) :feature feature :valueType "category"}))) ; copy the string, because string/split is evil

;
; matrix files
;

(defmulti ^:private matrix-data
  "Return seq of scores, probes X samples, with samples as metadata"
  (fn [metadata features lines] (metadata "type")))

(defmethod ^:private matrix-data :default
  [metadata features lines]
  (with-meta (map #(data-line features %) (rest lines))
             {:samples (rest (first lines))}))

(defn- transpose [lines]
  (apply mapv vector lines))

(defmethod ^:private matrix-data "clinicalMatrix"
  [metadata features lines]
  (let [lines (transpose lines)]
    (with-meta (map #(data-line features %) (rest lines))
               {:samples (rest (first lines))})))

;
;
;

(defn- cgdata-meta [file]
  (let [mfile (io/as-file (str file ".json"))]
    (if (.exists mfile)
      (json/read-str (slurp mfile))
      {:name file})))

(defn- genomic-matrix-meta [file]
  (-> file
      (cgdata-meta)
      (clojure.set/rename-keys
        {":probeMap" "probeMap" "PLATFORM" "platform"})))

(defn- probemap-meta [file]
  (-> file
      (cgdata-meta)
      (clojure.set/rename-keys
        {":assembly" "assembly"})))

(defn- file-stats [file]
  (let  [ts (file-time file)
         h (filehash file)]
    {:name file :time ts :hash h}))

(defn load-tsv-file [file]
  (let [fstats (file-stats file)
        metadata (genomic-matrix-meta file)
        feature (metadata ":clinicalFeature")
        fdesc (cgdata/feature-file (metadata ":clinicalFeature"))
        files (if fdesc [fstats (file-stats feature)] [fstats])]
    (with-open [in (io/reader file)]
      (load-exp
        files
        metadata
        (fn [] (matrix-data metadata fdesc (map tabbed (line-seq in))))
        fdesc))))

(defn- loadtest [name m n]
  (create)
  (let [mi (Integer. m)
        ni (Integer. n)]
  (load-exp name (now) "FIXME" (matrix-data nil (data/matrix mi ni)))))


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
      (load-probemap
        [{:name file :time ts :hash h}]
        (probemap-meta file)
        (fn [] (map probemap-row (line-seq in)))))))
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
    (catch java.lang.Exception e
      (binding [*out* *err*]
        (println "Error loading file" file)
        (println (str "message " (.getMessage e)))
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
                (range (count in-path) 0 -1)))
    (clean-sources)))

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
