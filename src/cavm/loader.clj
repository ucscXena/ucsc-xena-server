(ns
  ^{:author "Brian Craft"
    :doc "xena dataset loader entry point."}
  cavm.loader
  (:require [me.raynes.fs :as fs])
  (:require [cavm.fs-utils :refer [relativize]])
  (:require [cavm.multireader :refer [multi-reader with-multi]])
  (:require [clj-time.coerce :refer [from-long]])
  (:require [clojure.java.io :as io])
  (:require [digest])
  (:require [clojure.tools.logging :refer [info warn]])
  (:require [cavm.db :as cdb]))

; Combines file readers with db writers.

(defn- file-time [file]
  (from-long (.lastModified (java.io.File. file))))

(defn- file-hash [file]
  (digest/sha1 (io/as-file file)))

(defn- file-stats [docroot file]
  (let [full-path (str (io/file docroot file))
        ts (file-time full-path)
        h (file-hash full-path)]
    {:name file :time ts :hash h}))

(defn- log-error [filename e]
  (warn e "Loading" filename))

(defn- write-matrix [db docroot filename reader always]
  (let [{:keys [metadata refs features data-fn]} reader
        dependencies (if features
                       [(refs ":clinicalFeature") filename]
                       [filename])
        files (mapv #(file-stats docroot %) dependencies)]
    (with-multi [in (multi-reader (io/file docroot filename))]
      (cdb/write-matrix
        db
        filename
        files
        metadata
        (partial data-fn in)
        features
        always))))

(defn- delete-matrix [db docroot filename]
  (let [fname (str (relativize docroot filename))]
    (cdb/delete-matrix db fname)))

(def ^:private loaders
  {:probemap write-matrix
   :matrix write-matrix
   :mutation write-matrix
   :segment write-matrix})

(defn- ignore [& args])

; XXX The reader should really return a lazy seq of data types and readers, since
; one file type may contain mutiple xena data types.
(defn loader
  "Load a data file into the database"
  [db detector docroot filename & [opts]]
  (let [fname  (str (relativize docroot filename))
        reader @(:reader (detector filename))
        loader (get loaders (:datatype reader) ignore)]
    (loader db docroot fname reader opts)))

(defn dispatch [db detector docroot a q filename & [{:keys [delete always]}]]
  (let [filename (fs/with-cwd docroot (fs/file filename))]
    (if delete
      (send-off a (fn [n]
                    (info "Removing dataset" (str filename))
                    (let [t (read-string
                              (with-out-str
                                (time (try
                                        (delete-matrix @db docroot filename)
                                        (catch Exception e (log-error filename e))))))]
                      (info (str "Removed " filename ", " t)))
                    (swap! q #(vec (rest %)))))
      (send-off a (fn [n]
                    (info "Loading dataset" (str filename))
                    (let [t (read-string
                              (with-out-str
                                (time (try
                                        (loader @db detector docroot filename always)
                                        ; XXX Should re-throw Errors, but we need
                                        ; to catch Throwable (instead of Exception)
                                        ; in order to log it.
                                        (catch Throwable e (log-error filename e))))))]
                      (info (str "Loaded " filename ", " t)))
                    (swap! q #(vec (rest %))))))))

(defn loader-agent
  "Create an agent to serialize bulk loads, returning a function
  for loading a file via the agent, and the queue atom. Files are removed from
  queue after loading is complete.

  fn [filename & [{:keys [always delete] :or {always false delete false}}]]

  Detector is a function returning a file type as a keyword, which
  will be used to select a dataset writer method. This has evolved
  to a noop, as we currently have a single dataset writer."

  [db detector docroot]
  (let [a (agent 0)
        queue (atom [])]
    (add-watch queue :dispatch
               (fn [_ q prev curr]
                 (if (< (count prev) (count curr))
                   (apply dispatch db detector docroot a queue (last curr)))))
    [(fn [& args] (swap! queue conj args))
     queue]))
