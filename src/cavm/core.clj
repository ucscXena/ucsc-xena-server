(ns
  ^{:author "Brian Craft"
    :doc "xena server main."}
  cavm.core
  (:require [clojure.string :as s])
  (:require [cavm.h2 :as h2])
  (:require [clojure.java.io :as io])
  (:require [ring.adapter.jetty :refer [run-jetty]])
  (:require [clojure.data.json :as json])
  (:require [me.raynes.fs :as fs])
  (:require [clojure.tools.cli :refer [parse-opts]])
  (:require [cgdata.core :as cgdata])
  (:require [ring.middleware.resource :refer [wrap-resource]])
  (:require [ring.middleware.content-type :refer [wrap-content-type]])
  (:require [ring.middleware.not-modified :refer [wrap-not-modified]])
  (:require [ring.middleware.params :refer [wrap-params]])
  (:require [cavm.views.datasets])
  (:require [ring.middleware.gzip :refer [wrap-gzip]])
  (:require [ring.middleware.stacktrace :refer [wrap-stacktrace-web]]) ; XXX only in dev
  (:require [liberator.dev :refer [wrap-trace]])                   ; XXX only in dev
  (:require [filevents.core :refer [watch]])
  (:require [cavm.readers :as cr])
  (:require [cavm.loader :as cl])
  (:require [cavm.fs-utils :refer [normalized-path]])
  (:require [cavm.cgdata])
  (:require [clj-http.client :as client])
  (:require [clojure.tools.logging :as log :refer [info trace warn error]])
  (:require [taoensso.timbre :as timbre])
  (:require [less.awful.ssl :as ssl])
  (:import cavm.XenaImport cavm.XenaServer cavm.Splash)
  (:import org.slf4j.LoggerFactory
           ch.qos.logback.classic.joran.JoranConfigurator
           ch.qos.logback.core.util.StatusPrinter)
  (:gen-class))

(defn- in-data-path [root path]
  (boolean (fs/child-of? (normalized-path root) (normalized-path path))))

(def pom-props
  (doto (java.util.Properties.)
    (.load (io/reader (io/resource "META-INF/maven/cavm/cavm/pom.properties")))))

;
; web services

(def trusted-hosts
  ;#"https://[-_A-Za-z0-9]+.kilokluster.ucsc.edu|https://genome-cancer.ucsc.edu"
  #".*")

(def public-url "https://genome-cancer.ucsc.edu/")
(def public-xena (str public-url "proj/public/xena"))

(defn local-url [port] (str "https://local.xena.ucsc.edu:" (inc port)))
(defn local-xena [port] (str "http://local.xena.ucsc.edu:" port))

(def cohort-query
  '(map :cohort (query
                  {:select [:%distinct.cohort]
                   :from [:dataset]})))

(defn async-post [url post cb]
  (let [x (agent nil)]
    (set-error-handler! x (fn [_ err] (println "Error" err) (cb [])))
    (send-off x (fn [_]
                  (cb (:body (client/post url {:body
                                               (str post)
                                               :as :json
                                               :content-type
                                               "text/plain"})))))))

; need to block in an agent & then invoke callback
(defn retrieve-cohorts [port cb]
  (let [x (atom [])
        update (fn [cohorts]
                 (swap! x (fn [all]
                            (let [ret (conj all cohorts)]
                              (when (= 2 (count ret))
                                (cb (filter identity (apply concat ret))))
                              ret))))]
    (async-post (str public-xena "/data/") cohort-query update)
    (async-post (str (local-xena port) "/data/") cohort-query update)))

(defn- wrap-access-control [handler]
  (fn [request]
    (let [response (handler request)]
      (info "request" request)
      (if-let [origin (re-matches trusted-hosts (get-in request [:headers "origin"] ""))]
        (-> response
            (assoc-in [:headers "Access-Control-Allow-Origin"] origin)
            (assoc-in [:headers "Access-Control-Allow-Headers"] "Cancer-Browser-Api"))
        response))))

(defn- attr-middleware [app k v]
  (fn [req]
    (app (assoc req k v))))

(defn- log-middleware [handler]
  (fn [{:keys [uri query-string request-method] :as request}]
    (info "Start:" (.toUpperCase (name request-method)) uri query-string)
    (try
      (let [resp (handler request)]
        (info "End:" uri (:status resp))
        resp)
      (catch Exception ex
        (error ex "Error handling request" uri)
        (throw ex)))))

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
      (wrap-gzip)
      (log-middleware)
      (wrap-stacktrace-web)
      (wrap-access-control)))

(defn- serv [app host port {:keys [keystore password]}]
  (ring.adapter.jetty/run-jetty app {:host host
                                     :port port
                                     :ssl? true
                                     :ssl-port (+ port 1)
                                     :keystore keystore
                                     :key-password password
                                     :join? true}))

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

(defn- load-files [port always files]
  (client/post (str "http://localhost:" port "/update/")
               {:form-params (merge {:file (map normalized-path files)}
                                    (when always {:always "true"}))}))

(defn- delete-files [port files]
  (client/post (str "http://localhost:" port "/update/")
               {:form-params {:file (map normalized-path files) :delete "true"}}))

(def ^:private detectors
  [cgdata/detect-cgdata
   cgdata/detect-tsv])

(defn- filter-hidden
  [s]
  (filter #(not (.isHidden %)) s))

; Full reload metadata. The loader will skip
; data files with unchanged hashes.
(defn- file-changed [loader docroot kind file]
  (doseq [f (-> docroot
                (io/file)
                (file-seq)
                (rest) ; skip docroot (the first element)
                (filter-hidden))]
    (try (loader f)
      (catch Exception e (warn e "Dispatching load" f)))))

(def ^:private xenadir-default (str (io/file (System/getProperty  "user.home") "xena")))
(def ^:private docroot-default (str (io/file xenadir-default "files")))
(def ^:private db-default (str (io/file xenadir-default "database")))
(def ^:private logfile-default (str (io/file xenadir-default "xena.log")))
(def ^:private tmp-dir-default
  (str (io/file (System/getProperty "java.io.tmpdir") "xena-staging")))

(def ^:private argspec
  [[nil "--no-serve" "Don't start web server" :id :serve :parse-fn not :default true]
   ["-p" "--port PORT" "Server port to listen on" :default 7222 :parse-fn #(Integer/parseInt %)]
   ["-l" "--load" "Load files into running server"]
   ["-x" "--delete" "Delete file from running server"]
   [nil "--force" "Force reload of unchanged files (with -l)"]
   [nil "--auto" "Auto-load files" :id :auto :default false]
   [nil "--no-gui" "Don't start GUI" :id :gui :parse-fn not :default true]
   ["-h" "--help" "Show help"]
   ["-H" "--host HOST" "Set host for listening socket" :default "localhost"]
   ["-r" "--root DIR" "Set document root directory" :default docroot-default]
   ["-d" "--database FILE" "Database to use" :default db-default]
   [nil "--logfile FILE" "Log file to use" :default logfile-default]
   [nil "--keyfile FILE" "Private key file to use (requires --certfile)"]
   [nil "--certfile FILE" "Cert file to use (requires --keyfile)"]
   [nil "--version" "Print version and exit"]
   ["-j" "--json" "Fix json"]
   ["-t" "--tmp DIR" "Set tmp dir" :default tmp-dir-default]])

(defn- mkdir [dir]
  (.mkdirs (io/file dir))
  (when (not (.exists (io/file dir)))
    (str "Unable to create directory: " dir)))

; XXX should move these to h2.clj
(def h2-log-level
  (into {} (map vector [:off :error :info :debug :slf4j] (range))))

(def ^:private default-h2-opts-map
  {:cache_size 65536
   :undo_log 1
   :log 1
   :max_query_timeout 60000
   :multi_threaded "TRUE"
   :trace_level_file (h2-log-level :slf4j)})

; Enable transaction log, rollback log, and MVCC (so we can load w/o blocking readers).
(def ^:private default-h2-opts
  (s/join ";" (for [[k v] default-h2-opts-map]
                (str (.toUpperCase (name k)) "=" v))))

; Might want to allow more piecemeal setting of options, by
; parsing them & allowing cli overrides. For now, if the
; user sets options they must be comprehensive. If the user
; doesn't set options, we use default-h2-opts.

(defn- h2-opts
  "Add default h2 options if none are specified"
  [database]
  (if (= -1 (.indexOf database ";"))
    (str database ";" default-h2-opts)
    database))

(defn- logback-config
  ([filename]
   (logback-config filename "logback_config.xml"))
  ([filename config]
   (System/setProperty "log_file" filename)
   (let [context (LoggerFactory/getILoggerFactory)
         configurator (JoranConfigurator.)]
     (try
       (.setContext configurator context)
       (.reset context)
       (.doConfigure configurator (io/resource config))
       (catch Exception e))
     (StatusPrinter/printInCaseOfErrorsOrWarnings context))))

(defn- log-config [filename]
  (logback-config filename)
  (log/log-capture! 'cavm.core) ; redirect System.out System.error to clojure.tools.logging
  ; Only use timbre for the profiling commands. Might want to extract
  ; them into a standalone lib.
  (timbre/set-level! :trace)
  (timbre/merge-config!
    {:appenders {:standard-out {:enabled? false}
                 :jl {:enabled? true
                      :fn (fn [{:keys [ns level throwable message]}]
                            (log/log ns level throwable message))}}}))

(defn validate-certkey [{{:keys [keyfile certfile]} :options :as p-opts}]
  (if (or (and certfile (not keyfile)) (and (not certfile) keyfile))
    (update-in p-opts [:errors] conj "--certfile and --keyfile must be used together")
    p-opts))

(defn notify-ui-cohorts [cb cohorts]
  (.callback cb (into-array String
                            (sort-by #(.toLowerCase ^String %) cohorts))))

; XXX create dir for database as well?
(defn -main [& args]
  (let [{:keys [options arguments summary errors]} (validate-certkey (parse-opts args argspec))
        docroot (:root options)
        port (:port options)
        host (:host options)
        tmp (:tmp options)
        logfile (:logfile options)
        always (:force options)
        gui (:gui options)
        certfile (:certfile options)
        keyfile (:keyfile options)
        keystore (if keyfile
                   {:keystore (ssl/key-store keyfile certfile)
                    :password (String. ssl/key-store-password)}
                   {:keystore (.toString (io/resource "localhost.keystore"))
                    :password  "localxena"})
        database (h2-opts (:database options))]
    (if errors
      (binding [*out* *err*]
        (println (s/join "\n" errors)))
      (try
        (cond
          (:help options) (println summary)
          (:version options) (println (get pom-props "version"))
          (:json options) (cgdata/fix-json docroot)
          (:load options) (load-files port always arguments)
          (:delete options) (delete-files port arguments)
          :else (if-let [error (some mkdir [tmp docroot])]
                  (binding [*out* *err*] ; XXX move below log-config? What if we can't create the log file?
                    (println error)
                    (System/exit 1)) ; XXX notify gui
                  (do
                    (log-config logfile)
                    (info "Xena server starting"
                          (get pom-props "version")
                          (s/trim
                            (get pom-props "revision")))
                    (h2/set-tmp-dir! tmp)
                    (let [db (h2/create-xenadb database)
                          detector (apply cr/detector docroot detectors)
                          loader (cl/loader-agent db detector docroot)]
                      (when (:auto options)
                        (watch (partial file-changed loader docroot) docroot))
                      (Splash/close)
                      (when gui
                        (try
                          (XenaImport/start
                            (proxy [XenaServer] []
                              (load [file] (loader (str file)) true)
                              (retrieveCohorts [cb] (retrieve-cohorts port #(notify-ui-cohorts cb %)))
                              (publicUrl [] public-url)
                              (localUrl [] (local-url port))))
                          (catch Exception ex
                            (binding [*out* *err*]
                              (println "Failed to start gui. Logging error.")
                              (error "Failed to start gui." ex)))))
                      (when (:serve options)
                        (serv (get-app db loader) host port keystore))))))
        (catch Exception ex
          ; XXX maybe should enable ERROR logging to console, instead of this.
          (binding [*out* *err*]
            (println "Uncaught exception" ex))
          (error "Uncaught exception" ex)
          (System/exit 1)))))
  (shutdown-agents))

; When logging to the repl from a future, *err* gets lost.
; This will set it to the repl terminal, for reasons I don't understand.
(comment (defn snoop [msg x]
   (.start (Thread. #(binding [*out* *err*]
                       (println msg x)
                       (flush))))
   x))

; set logging to terminal:
; (logback-config "log" "logback_repl.xml")

; (def testdb (h2/create-xenadb "test;TRACE_LEVEL_FILE=3"))
; (def testdb (h2/create-xenadb "/inside/home/craft/xena/database;TRACE_LEVEL_FILE=3"))
; (def testdetector (apply cr/detector "/inside/home/craft/xena/files" detectors))
; (def testloader (cl/loader-agent testdb testdetector "/inside/home/craft/xena/files"))
;            (watch (partial file-changed #'testloader docroot-default) docroot-default)
; (def app (get-app testdb testloader))
; (defonce server (ring.adapter.jetty/run-jetty #'app {:port 7222 :join? false}))
; (.start server)
; (.stop server)
