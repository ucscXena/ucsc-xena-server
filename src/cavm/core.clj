(ns cavm.core
  (:require [clojure.string :as string])
  (:use cavm.h2)
  (:require [clojure.java.io :as io])
  (:use [clj-time.coerce :only (from-long)])
  (:require [noir.server :as server])
  (:require [me.raynes.fs :as fs])
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

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (if (= (first args) "-s")
    (serv)
    (loadfiles args))

  (shutdown-agents))
