(ns cavm.cgdata
  (:require [clojure.data.json :as json])
  (:require [clojure.string :as s])
  (:require [clojure.java.io :as io])
  (:require [clojure-csv.core :as csv])
  (:gen-class))

;
; cgData metadata
;

(defn- all-json [path]
  (let [files (file-seq (io/file path))
        fnames (map str files)]
    (map #(vector % (json/read-str (slurp %)))
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

(defn- fix-json [root]
  (let [files (all-json root)
        table (json-table files)
        normalize (partial normalize-path (count root))]
    (doseq [[file data] files]
      (write-json file (resolve-references normalize table file data)))))

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

(defn- infer-value-type [cols]
  (try
    (do
      (dorun (map parseFloatNA (take 10 (rest cols))))
      "float")
    (catch NumberFormatException e "category")))

(defn snoop [x] (println x) x)

(defmulti ^:private data-line
  "(id val val val val) -> seq of parsed values)

  Return a seq of floats from a split probe line, with probe name,
  feature definition, and data type as metadata."
  (fn [features cols]
    (or (get (get features (first cols)) "valueType") ; using (get) handles nil
        (infer-value-type cols))))

(defmethod ^:private data-line "float"
  [features cols]
  (let [feature (get features (first cols))]
    (with-meta
      (map parseFloatNA (rest cols))
      {:probe (String. ^String (first cols))
       :feature feature
       :valueType "float"}))) ; copy the string. string/split is evil.

; update map with value for NA
(defn- nil-val [order]
  (assoc order "" Double/NaN))

(defn- ad-hoc-order
  "Provide default order from data order in file"
  [feature cols]
  (if (:order feature)
    feature
    (let [state (distinct cols)
          order (into {} (map vector state (range)))] ; XXX handle all values null?
      (assoc feature :state state :order order))))

(defmethod ^:private data-line "category"
  [features cols]
  (let [feature (features (first cols))
        feature (ad-hoc-order feature (rest cols))
        order (nil-val (:order feature))]
    (with-meta
      (map order (rest cols))
      {:probe (String. ^String (first cols)) ; copy the string. string/split is evil.
       :feature feature
       :valueType "category"})))

(defmulti matrix-data
  "Return seq of scores, probes X samples, with samples as metadata"
  (fn [metadata features lines] (metadata "type")))

(defmethod matrix-data :default
  [metadata features lines]
  (with-meta (map #(data-line features %) (rest lines))
             {:samples (rest (first lines))}))

(defn- transpose [lines]
  (apply mapv vector lines))

(defmethod matrix-data "clinicalMatrix"
  [metadata features lines]
  (let [lines (transpose lines)]
    (with-meta (map #(data-line features %) (rest lines))
               {:samples (rest (first lines))})))

(defn- cgdata-meta [file]
  (let [mfile (io/as-file (str file ".json"))]
    (if (.exists mfile)
      (json/read-str (slurp mfile))
      {:name file})))

(defn matrix-file
  "Return a map describing a cgData matrix file. This will read
  any assoicated json or clinicalFeature file."
  [file]
  (let [{cf-file ":clinicalFeature" :as meta-data} (cgdata-meta file)
        feature-meta (feature-file cf-file)]
    {:meta meta-data
     :deps (if feature-meta [cf-file] [])
     :features feature-meta}))

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
  [file]
  (let [meta-data (cgdata-meta file)]
    {:meta meta-data
     :deps '()}))
