(ns cavm.core
  (:require [clojure.string :as string])
  (:use [cavm.h2 :only [create load-exp del-exp datasets with-db create-db]]) ; XXX use model instead?
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

; Return seq of scores, probes X samples, with samples as metadata
(defn- genomic-matrix-data [lines]
  (with-meta (map probe-line (rest lines))
             {:samples (rest (tabbed (first lines)))}))

(def data-path (str fs/*cwd*))

(defn- in-data-path [path]
  (boolean (fs/child-of? data-path path)))

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

(defn- loadfiles [args]
  (when (not (> (count args) 0))
    (println "Usage\nload <filename>")
    (System/exit 0))

  ; Skip files outside the designated path, which for now is CWD.
  (let [in-path (group-by in-data-path args)]
    (when (in-path false)
      (binding [*out* *err*]
        (println "These files are outside the CAVM data path and will not be served:")
        (println (string/join "\n" (in-path false)))))
    (create)
    (dorun (map load-tsv-file (in-path true)))))

(def ^:private argspec
  [["-s" "Start web server" :flag true :default false]
   ["-d" "Database to use" :default "file:///data/TCGA/craft/h2/cavm.h2"]
   ["-t" "Load test data  <name> <samples> <probes>" :flag true]])

(defn -main [& args]
  (let [[opts extra usage] (apply cli (cons args argspec))]
    (with-db (create-db (:d opts))
      (cond
        (:s opts) (serv)
        (:t opts) (if (not (= 3 (count extra)))
                    (println usage)
                    (apply loadtest extra))
        :else (loadfiles extra))))

  (shutdown-agents))
