(ns cavm.core
  (:require [clojure.string :as s])
  (:require [cavm.h2 :refer [create-xenadb]])
  (:require [clojure.java.io :as io])
  (:require [ring.adapter.jetty :refer [run-jetty]])
  (:require [clojure.data.json :as json])
  (:require [me.raynes.fs :as fs])
  (:require [clojure.tools.cli :refer [cli]])
  (:require [cgdata.core :as cgdata])
  (:require [ring.middleware.resource :refer [wrap-resource]])
  (:require [ring.middleware.content-type :refer [wrap-content-type]])
  (:require [ring.middleware.not-modified :refer [wrap-not-modified]])
  (:require [ring.middleware.params :refer [wrap-params]])
  (:require [cavm.views.datasets])
  (:require [ring.middleware.gzip :refer [wrap-gzip]])
  (:require [ring.middleware.stacktrace :refer [wrap-stacktrace]]) ; XXX only in dev
  (:require [liberator.dev :refer [wrap-trace]])                   ; XXX only in dev
  (:require [filevents.core :refer [watch]])
  (:require [cavm.readers :as cr])
  (:require [cavm.loader :as cl])
  (:require [cavm.fs-utils :refer [normalized-path]])
  (:require [cavm.cgdata])
  (:require [clj-http.client :as client])
  (:gen-class))

(defn- in-data-path [root path]
  (boolean (fs/child-of? (normalized-path root) (normalized-path path))))

;
; web services

; XXX change Access-Control-Allow-Origin in production.
(defn wrap-access-control [handler]
  (fn [request]
    (let [response (handler request)]
      (-> response
          (assoc-in [:headers "Access-Control-Allow-Origin"] "https://tcga1.kilokluster.ucsc.edu")
          (assoc-in [:headers "Access-Control-Allow-Headers"] "Cancer-Browser-Api")))))

(defn- attr-middleware [app k v]
  (fn [req]
    (app (assoc req k v))))

(comment (defn- del-datasets [args]
   (dorun (map del-exp args))))

(comment (defn- print-datasets []
   (dorun (map println (datasets)))))

; XXX add ring jsonp?
(defn- get-app [db loader]
  (-> cavm.views.datasets/routes
      (wrap-trace :header :ui)
      (attr-middleware :db db)
      (attr-middleware :loader loader)
      (wrap-params)
      (wrap-resource "public")
      (wrap-content-type)
      (wrap-not-modified)
      (wrap-access-control)
      (wrap-gzip)
      (wrap-stacktrace)))

(defn- serv [app port]
  (ring.adapter.jetty/run-jetty app {:port port :join? true}))

(comment (defn- load-report [load-fn root file]
   (try
     (load-fn root file)
     (catch java.lang.Exception e
       (binding [*out* *err*]
         (println "Error loading file" file)
         (println (str "message " (.getMessage e)))
         (.printStackTrace e))))))

; XXX call clean-sources somewhere?? Should be automated.
(comment (defn- loadfiles [load-fn root args]
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
     (clean-sources))))

(defn- loadfiles [port files]
  (client/post (str "http://localhost:" port "/update/")
               {:form-params {:file files}}))

(def detectors
  [cgdata/detect-cgdata
   cgdata/detect-tsv])

; Full reload metadata. The loader will skip
; data files with unchanged hashes.
(defn file-changed [loader docroot kind file]
  (doseq [f (rest (file-seq (io/file docroot)))] ; skip docroot (the first element)
    (try (loader f)
      (catch Exception e (println (str "caught exception: " (.getMessage e))))))) ; XXX this is unhelpful. Log it somewhere.

(def xenadir-default (str (io/file (System/getProperty  "user.home") "xena")))
(def docroot-default (str (io/file xenadir-default "files")))
(def db-default (str (io/file xenadir-default "database")))

(def ^:private argspec
  [["-s" "--serve" "Start web server" :flag true :default true] ; note --no-s to disable
   ["-p" "--port" "Server port to listen on" :default 7222]
   ["-l" "--load" "Load files into running server" :flag true :default false]
   ["--auto" "Auto-load files" :flag true :default true]
   ["-h" "--help" "Show help" :flag true :default false]
   ["-r" "--root" "Set document root directory" :default docroot-default]
   ["-d" "Database to use" :default db-default]
   ["-j" "--json" "Fix json" :default false :flag true]])

; XXX create dir for database as well?
(defn -main [& args]
  (let [[opts extra usage] (apply cli (cons args argspec))
        docroot (:root opts)
        port (:port opts)]
    (cond
      (:help opts) (println usage)
      (:json opts) (cgdata/fix-json docroot)
      (:load opts) (loadfiles port extra)
      :else (let [db (create-xenadb (:d opts))
                  detector (apply cr/detector docroot detectors)
                  loader (partial cl/loader db detector docroot)]
              (.mkdirs (io/file docroot))
              (if (not (.exists (io/file docroot)))
                (binding [*out* *err*]
                  (println "Unable to create directory" docroot))
                (do
                  (when (:auto opts)
                    (watch (partial file-changed loader docroot) docroot))
                  (when (:serve opts)
                    (serv (get-app db loader) port)))))))
  (shutdown-agents))

; When logging to the repl from a future, *err* gets lost.
; This will set it to the repl terminal, for reasons I don't understand.
(comment (defn snoop [msg x]
   (.start (Thread. #(binding [*out* *err*]
                       (println msg x)
                       (flush))))
   x))

; (def testdb (create-xenadb "test;TRACE_LEVEL_FILE=3"))
; (def testdb (create-xenadb "/inside/home/craft/xena/database;TRACE_LEVEL_FILE=3"))
; (def testdetector (apply cr/detector "/inside/home/craft/xena/files" detectors))
; (def testloader (partial cl/loader testdb testdetector "/inside/home/craft/xena/files"))
;            (watch (partial file-changed #'testloader docroot-default) docroot-default)
; (def app (get-app testdb testloader))
; (defonce server (ring.adapter.jetty/run-jetty #'app {:port 7222 :join? false}))
; (.start server)
; (.stop server)
