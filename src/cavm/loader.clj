(ns cavm.loader
  (:require [cavm.fs-utils :refer [relativize]])
  (:require [clj-time.coerce :refer [from-long]])
  (:require [clojure.java.io :as io])
  (:require [digest])
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

(defn write-matrix [db docroot filename reader]
  (let [{:keys [metadata refs features data-fn]} reader
        dependencies (if features
                       [(refs ":clinicalFeature") filename]
                       [filename])
        files (mapv #(file-stats docroot %) dependencies)]
    (with-open [in (io/reader (io/file docroot filename))]
      (cdb/write-matrix
        db
        filename
        files
        metadata
        (partial data-fn in)
        features
        false))))


(defn write-probemap [db docroot filename reader]
  (let [{:keys [metadata refs data-fn]} reader
        files (mapv #(file-stats docroot %) [filename])]
    (with-open [in (io/reader (io/file docroot filename))]
      (cdb/write-probemap
        db
        filename
        files
        metadata
        (partial data-fn in) 
        false))))

(def loaders
  {:probemap write-probemap
   :matrix write-matrix})

(defn- ignore [& args])

; XXX The reader should really return a lazy seq of data types and readers, since
; one file type may contain mutiple xena data types.
(defn loader
  "Load a data file into the database"
  [db detector docroot filename]
  (let [fname  (str (relativize docroot filename))
        reader @(:reader (detector filename))
        loader (get loaders (:datatype reader) ignore)]
    (loader db docroot fname reader)))

(defn- log-error [e]
  (binding [*err* *out*]
    (println (.getMessage e))))

(defn loader-agent
  "Create an agent to serialize bulk loads, returning a function
  for loading a file via the agent."
  [db detector docroot]
  (let [a (agent nil)]
    (fn [filename]
      (send a (fn [n]
                (try
                  (loader db detector docroot filename)
                  (catch Exception e (log-error e)))
                nil)))))
