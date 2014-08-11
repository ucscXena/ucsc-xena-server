(ns
  ^{:author "Brian Craft"
    :doc "xena dataset loader entry point."}
  cavm.loader
  (:require [cavm.fs-utils :refer [relativize]])
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

(defn- write-matrix [db docroot filename reader always]
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
        always))))

(def ^:private loaders
  {:probemap write-matrix
   :matrix write-matrix
   :mutation write-matrix})

(defn- ignore [& args])

; XXX The reader should really return a lazy seq of data types and readers, since
; one file type may contain mutiple xena data types.
(defn loader
  "Load a data file into the database"
  [db detector docroot filename & [always]]
  (let [fname  (str (relativize docroot filename))
        reader @(:reader (detector filename))
        loader (get loaders (:datatype reader) ignore)]
    (loader db docroot fname reader (or always false))))

(defn- log-error [filename e]
  (warn e "Loading" filename))

(defn loader-agent
  "Create an agent to serialize bulk loads, returning a function
  for loading a file via the agent.

  fn [filename & [{always :always :or {always false}}]]

  Detector is a function returning a file type as a keyword, which
  will be used to select a dataset writer method. This has evolved
  to a noop, as we currently have a single dataset writer."

  [db detector docroot]
  (let [a (agent nil)]
    (fn [filename & [{always :always :or {always false}}]]
      (send-off a (fn [n]
                    (info "Loading dataset" (str filename))
                    (let [t (read-string
                              (with-out-str
                                (time (try
                                        (loader db detector docroot filename always)
                                        (catch Exception e (log-error filename e))))))]
                      (info (str "Loaded " filename ", " t)))
                    nil)))))
