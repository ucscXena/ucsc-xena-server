(ns cavm.core
  (:require [clojure.string :as s])
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
  (s/split line #"\t"))

(defn- file-time [file]
  (from-long (.lastModified (java.io.File. file))))

(def data-path (str fs/*cwd*))

(defn- in-data-path [path]
  (boolean (fs/child-of? data-path path))) ; XXX not working with ".."??

(defn- filehash [file]
  (digest/sha1 (io/as-file file)))

;
;


(defn- file-stats [file]
  (let  [ts (file-time file)
         h (filehash file)]
    {:name file :time ts :hash h}))

(defn- genomic-renames [mdata]
  "Convert cgData genomic attrs to db attrs."
  (clojure.set/rename-keys 
    mdata
    {":probeMap" "probeMap" "PLATFORM" "platform"}))

(defn load-tsv-file [file]
  (with-open [in (io/reader file)]
    (let [{md :meta deps :deps fm :features} (cgdata/matrix-file file)
          md (genomic-renames md)
          files (mapv file-stats (cons file deps))
          data-fn (fn []
                    (cgdata/matrix-data md fm (map tabbed (line-seq in))))]
      (load-exp files md data-fn fm))))

(defn- loadtest [name ^Integer m ^Integer n]
  (create)
  (let [mi (Integer. m)
        ni (Integer. n)]
  (load-exp name (now) "FIXME" (cgdata/matrix-data nil (data/matrix mi ni)))))


;
; cgData probemap
;

(defn- probemap-renames [mdata]
  "Convert cgData probemap attrs to db attrs."
  (clojure.set/rename-keys
    mdata
    {":assembly" "assembly"}))

(defn load-probemap-file [file]
  (with-open [in (io/reader file)]
    (let [{md :meta deps :deps} (cgdata/probemap-file file)
          md (probemap-renames md)
          files (mapv file-stats (cons file deps))
          data-fn (fn []
                    (cgdata/probemap-data (line-seq in)))]
      (load-probemap files md data-fn))))

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
        (println (s/join "\n" (in-path false)))))
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
