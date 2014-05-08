(ns cavm.cgdata
  (:require [clojure.data.json :as json])
  (:require [clojure.string :as s])
  (:require [clojure.java.io :as io])
  (:require [clojure-csv.core :as csv])
  (:require [me.raynes.fs :as fs])
  (:require clojure.pprint)
  (:require [cavm.readers :refer [reader]])
  (:gen-class))

;
; Utility functions
;

(defn- chunked-pmap [f coll]
  (->> coll
       (partition-all 250)
       (pmap (fn [chunk] (doall (map f chunk))))
       (apply concat)))

(defn- normalized-path
  "Like fs/normalized-path, but doesn't add *cwd*."
  [path]
  (fs/with-cwd "/"
    (apply io/file (drop 1 (fs/split (fs/normalized-path path))))))

(defn- tabbed [line]
  (s/split line #"\t"))

;
; cgData metadata
;

(defn- all-json [path]
  (let [files (file-seq (io/file path))
        fnames (map str files)]
    (map #(vector (s/replace % #"\.json$" "") (json/read-str (slurp %)))
         (filter #(.endsWith ^String % ".json") fnames))))

(defn- json-add [acc [file {n "name" t "type" :as metadata}]]
  (if-let [group (and (= t "probeMap") (metadata "group"))]
    (assoc-in acc [t (str group "::" (metadata ":assembly"))] file)
    (assoc-in acc [t n] file)))

(defn- json-table [json-list]
  (reduce json-add {} json-list))

(defn- dirname [file]
   (s/replace file #"[^/]*$" ""))

(defn- basename [file]
  (s/replace file #".*/([^/]*)$" "$1"))

(defn- make-absolute [^String path]
  (if (.startsWith path "/")
    path
    (str "/" path)))

(defn- normalize-path [cut file path]
  (if (= (dirname file) (dirname path))
    (basename path)
    (make-absolute (subs path cut))))

; either pass through [k v] or map it to a file if
; it matches an object in the table.
(defn resolve-reference [normalize table file [k v]]
  (if-let [match (and (.startsWith ^String k ":")
                      (or
                        (get-in table [(subs k 1) v])
                        (get-in table [(subs k 1) (str v "::hg18")])))]
    [k (normalize file match)]
    [k v]))

; rewrite table to use files instead of object names
(defn- resolve-references [normalize table file metadata]
  (into {}
        (map #(resolve-reference normalize table file %) metadata)))

(defn- write-json [file json]
  (with-open [writer (io/writer file)]
    (binding [*out* writer]
      (json/pprint json :escape-slash false))))

(defn fix-json [root]
  (let [files (all-json root)
        table (json-table files)
        normalize (partial normalize-path (count root))]
    (doseq [[file data] files]
      (write-json (str file ".json")
                  (resolve-references normalize table file data)))))

; apply f to values of map
(defn- fmap [f m]
  (into {} (for [[k v] m] [k (f v)])))

;
; cgData clinicalFeature
;

(defmulti ^:private feature-line
  (fn [acc line] () (second line)))

(defmethod ^:private feature-line :default
  [acc [feature attr value]]
  (assoc-in acc [feature attr] value))

(defn- add-state [curr value]
  (conj (vec curr) value))

(defmethod ^:private feature-line "state"
  [acc [feature attr value]]
  (update-in acc [feature attr] add-state value))

(defn- parse-order [order]
  (first (csv/parse-csv order)))

(defmethod ^:private feature-line "stateOrder"
  [acc [feature attr value]]
  (assoc-in acc [feature attr] (parse-order value)))

(defn- feature-map [lines]
  "Read tab-split clincalFeature rows into a map"
  (reduce feature-line {} lines))

(defn- calc-order
  "Take feature order from 'stateOrder', or file order of 'state' rows."
  [feature]
  (if-let [state (or (feature "stateOrder") (feature "state"))]
    (-> feature
        (assoc :order (into {} (map #(vector %1 %2) state (range))))
        (assoc :state state))
    feature))

(defn feature-file [file]
  (when (and file (.exists (io/as-file file)))
    (->> file
         (slurp)
         (#(s/split % #"\n"))     ; split lines
         (map #(s/split % #"\t")) ; split tabs
         (feature-map)
         (fmap calc-order))))

;
; cgData genomicMatrix
;

(defn- parseFloatNA [str]
  (if (or (= str "NA") (= str "nan") (= str ""))
    Double/NaN
    (Float/parseFloat str)))

; If we have no feature description, we try "float". If that
; fails, we try "category" by passing in a hint. We do not
; allow the hint to override a feature description, since that
; would mask curation errors. multimethod may not be the best
; mechanism for this policy.

(defmulti ^:private data-line
  "(id val val val val) -> seq of parsed values)

  Return from a split probe line a probe name,
  feature definition, data type, and seq of floats"
  (fn [features cols & [hint]]
    (or (get (get features (first cols)) "valueType")
        hint)))

(defmethod ^:private data-line :default
  [features cols & [hint]]
  (try ; the body must be eager so we stay in the try/except scope
    (let [feature (get features (first cols))]
      {:field (String. ^String (first cols)) ; copy, because split is evil.
       :feature feature
       :valueType "float"
       :scores (mapv parseFloatNA (rest cols))}) ; eager
    (catch NumberFormatException e
      (data-line features cols "category"))))

; update map with value for NA
(defn- nil-val [order]
  (assoc order "" Double/NaN))

(defn- ad-hoc-order
  "Provide default order from data order in file"
  [feature cols]
  (if (:order feature)
    feature
    (let [state (distinct cols) ; XXX drop ""? This adds "" as a state.
          order (into {} (map vector state (range)))] ; XXX handle all values null?
      (assoc feature :state state :order order))))

(defn- throw-on-nil [x msg & args]
  (when-not x
    (throw (IllegalArgumentException.
             (apply format msg args))))
  x)

(defmethod ^:private data-line "category"
  [features cols & [hint]]
  (let [name (first cols)
        feature (get features name)  ; use 'get' to handle nil
        feature (ad-hoc-order feature (rest cols))
        order (nil-val (:order feature))
        msg "Invalid state %s for feature %s"
        vals (map #(throw-on-nil (order %) msg % name)
                  (rest cols))]

    {:field (String. ^String name) ; copy the string. string/split is evil.
     :feature feature
     :valueType "category"
     :scores vals}))

(defmulti matrix-data
  "Return samples and seq of scores, probes X samples"
  (fn [metadata features lines] (metadata "type")))

(defmethod matrix-data :default
  [metadata features lines]
  {:fields (chunked-pmap #(data-line features (tabbed %)) (rest lines))
   :samples (rest (tabbed (first lines)))})

(defn- transpose [lines]
  (apply mapv vector lines))

(defmethod matrix-data "clinicalMatrix"
  [metadata features lines]
  (let [lines (transpose (map tabbed lines))]
    {:fields (chunked-pmap #(data-line features %) (rest lines))
     :samples (rest (first lines))}))

(defn- cgdata-meta [file]
  (let [mfile (io/as-file (str file ".json"))]
    (when (.exists mfile)
      (json/read-str (slurp mfile)))))

(defn- path-from-root
  "Return file path relative to root"
  [root file]
  (let [root (fs/normalized-path root)
        file (fs/normalized-path file)]
    (when-not (fs/child-of? root file)
      (throw (IllegalArgumentException. (str file " not in root path: " root))))
    (apply io/file (drop (count (fs/split root)) (fs/split file)))))

(defn- path-from-ref
  "Construct a file path relative to the root of the referring file. 'referer'
  must be relative to root."
  [referrer file]
  (let [sref (fs/split referrer)
        sfile (fs/split file)]
    (if (= (first sfile) fs/unix-root)
      (apply io/file (drop 1 sfile))
      (fs/with-cwd fs/unix-root
        (normalized-path (apply io/file (concat (drop-last 1 sref) sfile)))))))

(defn- references [referrer md]
  "Return map of any references in md to their paths relative to the root. 'referer'
  must be relative to root."
  (let [refs (->> md
                  (keys)
                  (filter #(.startsWith % ":")))]
    (into {} (map vector refs (map #(str (path-from-ref referrer (md %))) refs)))))

(defn matrix-file
  "Return a map describing a cgData matrix file. This will read
  any assoicated json or clinicalFeature file."
  [file & {root :root :or {root fs/unix-root}}]
  (let [rfile (str (path-from-root root file))
        meta-data (or (cgdata-meta file) {"name" file})
        refs (references rfile meta-data)
        cf (refs ":clinicalFeature")
        feature (when cf (feature-file (fs/file root cf)))]

    {:rfile rfile        ; file relative to root
     :meta meta-data     ; json metadata
     :refs refs          ; map of json metadata references to paths relative to root
     :features feature   ; slurped clinicalFeatures
     :data-fn (fn [in] (matrix-data meta-data feature (line-seq in)))}))

;
; cgData probemaps
;

(defn- split-no-empty [in pat] ; XXX Does this need to handle quotes?
  (filter #(not (= "" %)) (s/split in pat)))

(defn- probemap-row [row]
  (let [[name genes chrom start end strand] (s/split row #"\t")]
    {:name name
     :genes (split-no-empty genes #",")
     :chrom chrom
     :chromStart (. Integer parseInt start)
     :chromEnd (. Integer parseInt end)
     :strand strand}))

(defn probemap-data
  "Return seq of probes entries as maps"
  [rows]
  (map probemap-row rows))

(defn probemap-file
  "Return a map describing a cgData probemap file. This will read
  any assoicated json."
  [file & {root :root :or {root fs/unix-root}}]
  (let [rfile (str (path-from-root root file))
        meta-data (cgdata-meta file)
        refs (references rfile meta-data)]
    {:rfile rfile
     :meta meta-data
     :refs refs}))

;
; cgdata file detector
;

(def types
  {"clincialMatrix" ::matrix
   "genomicMatrix" ::matrix
   "probeMap" ::probeMap})

(defn detect-cgdata
  "Detect cgdata files by presence of json metadata. If no type
  is give, assume genomicMatrix"
  [file]
  (when-let [cgmeta (cgdata-meta file)]
    (or (types (cgmeta "type")) ::genomic)))

;
; cgdata file reader
;

(defmethod reader ::probeMap
  [filetype url]
  (probemap-file url))

(defmethod reader ::matrix
  [filetype url]
  (matrix-file url))

;
;
;

(defn tabs [line]
  (count (filter #(= % \tab) line)))

(defn is-tsv?
  "Detect tsv. Requires two non-blank lines, each having
  at least one tab."
  [lines]
  (let [head (take 2 (filter #(re-matches #".*\S.*" %) lines))] ; two non-blank lines
    (and
      (> (count head) 1)
      (every? #(> (tabs %) 0) head))))

; XXX Note that this will pick up all kinds of unrelated files,
; including probeMaps without assocated json metadata.
(defn detect-tsv
  "Return ::tsv if the file is tsv, or nil"
  [file]
  (when (with-open [in (io/reader file)]
        (is-tsv? (line-seq in)))
    ::tsv))

(defmethod reader ::tsv
  [filetype url]
  (matrix-file url))
