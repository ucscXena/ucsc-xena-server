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
  (:require [ring.adapter.jetty :refer [run-jetty]])
  (:require [clojure.data.json :as json])
  (:require [me.raynes.fs :as fs])
  (:require [clojure.tools.cli :refer [cli]])
  (:require [cavm.cgdata :as cgdata])
  (:require [ring.middleware.resource :refer [wrap-resource]])
  (:require [ring.middleware.content-type :refer [wrap-content-type]])
  (:require [ring.middleware.not-modified :refer [wrap-not-modified]])
  (:require [ring.middleware.params :refer [wrap-params]])
  (:require [cavm.views.datasets])
  (:require [ring.middleware.gzip :refer [wrap-gzip]])
  (:require [ring.middleware.stacktrace :refer [wrap-stacktrace]]) ; XXX only in dev
  (:require [liberator.dev :refer [wrap-trace]])                   ; XXX only in dev
  (:gen-class))

(defn- file-time [file]
  (from-long (.lastModified (java.io.File. file))))

(defn- in-data-path [root path]
  (boolean (fs/child-of? root path))) ; XXX not working with ".."??

(defn- filehash [file]
  (digest/sha1 (io/as-file file)))

;
;
;

(defn- file-stats [root file]
  (let  [full-path (str (fs/file root file))
         ts (file-time full-path)
         h (filehash full-path)]
    {:name file :time ts :hash h}))

(defn- genomic-renames [mdata rfile]
  "Convert cgData genomic attrs to db attrs."
  (-> mdata
      (clojure.set/rename-keys
        {":probeMap" "probeMap"
         "PLATFORM" "platform"
         ":dataSubType" "dataSubType"})
      (assoc "name" rfile)))

(defn load-matrix-file [root file]
  (with-open [in (io/reader file)]
    (let [{:keys [rfile meta refs features]} (cgdata/matrix-file file :root root)
          meta (genomic-renames meta rfile)
          files (if features
                  [(refs ":clinicalFeature") rfile]
                  [rfile])
          stats (mapv #(file-stats root %) files)
          data-fn (fn []
                    (cgdata/matrix-data meta features (line-seq in)))]
      (load-exp stats meta data-fn features))))

(defn- loadtest [name m n]
  (create)
  (let [mi (Integer. m)
        ni (Integer. n)]
  (load-exp
    [{:name name :time (now) :hash "FIXME" }]
    {"name" name}
    (fn [] (cgdata/matrix-data {} nil (data/matrix mi ni))) nil)))


;
; cgData probemap
;

(defn- probemap-renames [mdata rfile]
  "Convert cgData probemap attrs to db attrs."
  (-> mdata
      (clojure.set/rename-keys
        {":assembly" "assembly"})
      (assoc "name" rfile)))

(defn load-probemap-file [root file]
  (with-open [in (io/reader file)]
    (let [{:keys [rfile meta]} (cgdata/probemap-file file :root root)
          meta (probemap-renames meta rfile)
          files [rfile]
          stats (mapv #(file-stats root %) files)
          data-fn (fn []
                    (cgdata/probemap-data (line-seq in)))]
      (load-probemap stats meta data-fn))))

;
; web services

(def port 7222)

; XXX change Access-Control-Allow-Origin in production.
(defn wrap-access-control [handler]
  (fn [request]
    (let [response (handler request)]
      (-> response
          (assoc-in [:headers "Access-Control-Allow-Origin"] "https://tcga1.kilokluster.ucsc.edu")
          (assoc-in [:headers "Access-Control-Allow-Headers"] "Cancer-Browser-Api")))))

(defn- db-middleware [app db]
  (fn [req]
    (app (assoc req :db db))))

(defn- del-datasets [args]
  (dorun (map del-exp args)))

(defn- print-datasets []
  (dorun (map println (datasets))))

; XXX should add ring jsonp
(defn- get-app [db]
  (-> cavm.views.datasets/routes
      (db-middleware db)
      (wrap-params)
      (wrap-resource "public")
      (wrap-content-type)
      (wrap-not-modified)
      (wrap-access-control)
      (wrap-gzip)
      (wrap-stacktrace)))

(defn- serv [app]
  (ring.adapter.jetty/run-jetty app {:port port :join? true}))

(defn- load-report [load-fn root file]
  (try
    (load-fn root file)
    (catch java.lang.Exception e
      (binding [*out* *err*]
        (println "Error loading file" file)
        (println (str "message " (.getMessage e)))
        (.printStackTrace e)))))

(defn- loadfiles [load-fn root args]
  (when (not (> (count args) 0))
    (println "Usage\nload <filename>")
    (System/exit 0))

  ; Skip files outside the designated path
  (let [{in-path true, not-in-path false}
        (group-by #(in-data-path root %) args)]
    (when not-in-path
      (binding [*out* *err*]
        (println "These files are outside the CAVM data path and will not be served:")
        (println (s/join "\n" (in-path false)))))
    (create)
    (println "Loading " (count in-path) " file(s)")
    (dorun (map #(do (print %2 %1 "") (time (load-report load-fn root %1)))
                in-path
                (range (count in-path) 0 -1)))
    (clean-sources)))

(def ^:private argspec
  [["-s" "Start web server" :flag true :default false]
   ["-l" "Load files (default)" :flag true :default false]
   ["-p" "Load probemaps" :flag true :default false]
   ["-h" "--help" "Show help" :default false :flag true]
   ["-r" "--root" "Set root data directory" :default (str fs/*cwd*)]
   ["-d" "Database to use"]
   ["-j" "--json" "Fix json" :default false :flag true]
   ["-t" "Load test data  <name> <samples> <probes>" :flag true]])

(def ^:private modes [:s :help :json])
(def ^:private nodb-opts #{:help :json})

(defn -main [& args]
  (let [[opts extra usage] (apply cli (cons args argspec))
        load-fn (if (:p opts) load-probemap-file load-matrix-file)
        opts-modes (map first (filter (fn [[k v]] v) (select-keys opts modes)))
        mode  (or (first opts-modes) :l)
        db (create-db (:d opts))]
    ; Because we don't know if the option is short or long, this error message
    ; can have the wrong number of dashes. Also, we don't have the order
    ; of the options, so we can't pick the first one. The ones we ignore are
    ; random due to the hash.
    (when (> (count opts-modes) 1)
      (binding [*out* *err*]
        (println "Ignoring conflicting modes:"
                 (s/join " " (map #(->> % name (str "-")) (rest opts-modes))))))
    (if (nodb-opts mode)
      (cond
        (:help opts) (println usage)
        (:json opts) (cgdata/fix-json (:root opts)))
      (with-db db
        (cond
          (:s opts) (do
                      (serv (get-app db)))
          (:t opts) (if (not (= 3 (count extra)))
                      (println usage)
                      (apply loadtest extra))
          :else (loadfiles load-fn (:root opts) extra)))))

  (shutdown-agents))

; (def testdb (create-db "test;TRACE_LEVEL_FILE=3"))
; (def app (get-app testdb))
; (defonce server (ring.adapter.jetty/run-jetty #'app {:port port :join? false}))
; (.start server)
; (.stop server)
