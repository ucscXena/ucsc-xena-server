(ns cavm.h2
  (:require [korma.db :as kdb])
  (:require [korma.config :as kconf])
  (:require [org.clojars.smee.binary.core :as binary])
  (:require [clojure.java.io :as io])
  (:require [honeysql.core :as hsql])
  (:require [honeysql.format :as hsqlfmt])
  (:require [honeysql.types :as hsqltypes])
  (:require [clojure.data.json :as json])
  (:use [cavm.binner :only (calc-bin)])
  (:use [clj-time.format :only (formatter unparse)])
  (:use [cavm.hashable :only (ahashable)])
  (:use [korma.core :rename {insert kcinsert}])
  (:require [cavm.query.sources :as sources])
  (:require [me.raynes.fs :as fs])
  (:require [cavm.db :refer [XenaDb]])
  (:require [taoensso.timbre :as timbre])
  (:gen-class))

;
; Note that "bin" in this file is like a "segement" in the column store
; literature: a column is split into a series of segments (bins) before
; writing to disk (saving as blob in h2).
;

(timbre/refer-timbre) ; XXX magic requires for timbre

(def ^:dynamic *tmp-dir* (System/getProperty "java.io.tmpdir"))

(defn set-tmp-dir! [dir]
  (alter-var-root #'*tmp-dir* (constantly dir)))

; Coerce this sql fn name to a keyword so we can reference it.
(def KEY-ID (keyword "SCOPE_IDENTITY()"))

; provide an insert macro that directly returns an id, instead of a hash
; on SCOPE_IDENTITY().
(defmacro insert [& args] `(KEY-ID (kcinsert ~@args)))

(def float-size 4)
(def bin-size 1000)
(def score-size (* float-size bin-size))
; Add 30 for gzip header
;( def score-size (+ 30 (* float-size bin-size)))

;
; Utility functions
;

(defn- chunked-pmap [f coll]
  (->> coll
       (partition-all 250)
       (pmap (fn [chunk] (doall (map f chunk))))
       (apply concat)))

(defn memoize-key
  "Memoize with the given function of the arguments"
  [kf f]
  (let [mem (atom {})]
    (fn [& args]
      (let [k (kf args)]
        (if-let [e (find @mem k)]
          (val e)
          (let [ret (apply f args)]
            (swap! mem assoc k ret)
            ret))))))

;
; Table models
;

(declare dataset_source feature field scores dataset sample)

(defentity cohorts)
(defentity source)

(defentity dataset
  (many-to-many source :dataset_source)
  (has-many sample)
  (has-many field))

(defentity sample
  (belongs-to dataset))

(declare score-decode)

(defn- cvt-scores [{scores :SCORES :as v}]
  (if scores
    (assoc v :SCORES (float-array (score-decode scores)))
    v))

(defentity field
  (has-one feature)
  (many-to-many scores :field_score)
  (belongs-to dataset)
  (transform cvt-scores))

(defentity scores
  (many-to-many field :field_score))

(defentity field_score)

;
; Table definitions.
;

; Note H2 has one int type: signed, 4 byte. Max is approximately 2 billion.
(def field-table
  ["CREATE SEQUENCE IF NOT EXISTS FIELD_IDS CACHE 2000"
   "CREATE TABLE IF NOT EXISTS `field` (
   `id` INT NOT NULL DEFAULT NEXT VALUE FOR FIELD_IDS PRIMARY KEY,
   `dataset_id` INT NOT NULL,
   `name` VARCHAR(255),
   UNIQUE(`dataset_id`, `name`),
   FOREIGN KEY (`dataset_id`) REFERENCES `dataset` (`id`))"])

(def scores-table
  ["CREATE SEQUENCE IF NOT EXISTS SCORES_IDS CACHE 2000"
   (format "CREATE TABLE IF NOT EXISTS `scores` (
           `id` INT NOT NULL DEFAULT NEXT VALUE FOR SCORES_IDS PRIMARY KEY,
           `scores` VARBINARY(%d) NOT NULL)" score-size)])

(def field-score-table
  ["CREATE TABLE IF NOT EXISTS `field_score` (
   `field_id` INT,
   `i` INT,
   `score_id` INT,
   UNIQUE (`field_id`, `i`))"])
; XXX ugh. score_id vs scores.id

(def cohorts-table
  ["CREATE TABLE IF NOT EXISTS `cohorts` (
   `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
   `name` VARCHAR(2000) NOT NULL UNIQUE)"])

; XXX What should max file name length be?
; XXX Unique columns? Indexes?
(def source-table
  ["CREATE TABLE IF NOT EXISTS `source` (
   `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
   `name` VARCHAR(2000) NOT NULL,
   `time` TIMESTAMP NOT NULL,
   `hash` VARCHAR(40) NOT NULL)"])

(def dataset-table
  ["CREATE TABLE IF NOT EXISTS `dataset` (
   `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
   `name` varchar(1000) NOT NULL UNIQUE,
   `probeMap` varchar(255),
   `shortTitle` varchar(255),
   `longTitle` varchar(255),
   `groupTitle` varchar(255),
   `platform` varchar(255),
   `cohort` varchar(1000),
   `security` varchar(255),
   `gain` double DEFAULT NULL,
   `text` varchar (65535),
   `dataSubType` varchar (255))"])

;
; experiment in making a macro for metadata tables
;

;(defn- create-table [name cols]
;  (format "CREATE TABLE IF NOT EXISTS `%s` (%s)"
;          name
;          cols))
;
;(defn- format-cols [cols]
;  (->> cols
;       (map (fn [[k v]] (format "`%s` %s" (name k) v)))
;       (clojure.string/join ", ")))
;
;(defmacro defmeta [table & columns]
;;  (let [cols (array-map (into {} (map vec (partition 2 columns))))]
;  (let [kv (map vec (partition 2 columns))
;        create-cmd (create-table
;                     (str table "-table")
;                     (format-cols kv))]
;    nil))
;
;
;(defmeta dataset
;  :id "INT NOT NULL AUTO_INCREMENT PRIMARY KEY"
;  :name "varchar(255) NOT NULL UNIQUE"
;  :probeMap "varchar(255)"
;  :shortTitle "varchar(255)"
;  :longTitle "varchar(255)"
;  :groupTitle "varchar(255)"
;  :platform "varchar(255)"
;  :security "varchar(255)"
;  :gain "double DEFAULT NULL"
;  :url "varchar(255) DEFAULT NULL"
;  :description "longtext"
;  :notes "longtext"
;  :wrangling_procedure "longtext")

;
;
;


(def ^:private dataset-columns
  #{:name
    :probeMap
    :shortTitle
    :longTitle
    :groupTitle
    :platform
    :cohort
    :security
    :dataSubType
    :text
    :gain})

(def ^:private dataset-defaults
  (into {} (map #(vector % nil) dataset-columns)))

; XXX should abstract this to set up the many-to-many
; on the defentity. Can we run many-to-many on the existing
; sources entity?? Or maybe do defentity sources *after* all
; the other tables exist?
(def dataset-source-table
  ["CREATE TABLE IF NOT EXISTS `dataset_source` (
   `dataset_id` INT NOT NULL,
   `source_id` INT NOT NULL,
   FOREIGN KEY (dataset_id) REFERENCES `dataset` (`id`),
   FOREIGN KEY (source_id) REFERENCES `source` (`id`))"])

(defentity dataset_source)

(def ^:private dataset-meta
  {:table dataset
   :defaults dataset-defaults
   :join dataset_source
   :columns dataset-columns})

(def sample-table
  ["CREATE TABLE IF NOT EXISTS `sample` (
   `dataset_id` INT NOT NULL,
   FOREIGN KEY (dataset_id) REFERENCES `dataset` (`id`),
   `i` INT NOT NULL,
   `name` VARCHAR (1000) NOT NULL,
   PRIMARY KEY(`dataset_id`, `i`))"]) ; XXX what should this be?

;
; Probemap declarations
;

(declare probe probe_position probe_gene)
(defentity probemap_source)

(defentity probemap
  (has-many probe)
  (many-to-many source :probemap_source))

(defentity probe
  (belongs-to probemap)
  (has-many probe_position)
  (has-many probe_gene))

(defentity probe_position
  (belongs-to probe))

(defentity probe_gene
  (belongs-to probe))


; XXX What should max file name length be?
(def probemap-table
  ["CREATE TABLE IF NOT EXISTS `probemap` (
   `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
   `name` VARCHAR(1000),
   `assembly` VARCHAR(255),
   `version` VARCHAR(255),
   `text` VARCHAR(65535))"])

(def probemap-source-table
  ["CREATE TABLE IF NOT EXISTS `probemap_source` (
   `probemap_id` INT NOT NULL,
   `source_id` INT NOT NULL,
   FOREIGN KEY (probemap_id) REFERENCES `probemap` (`id`),
   FOREIGN KEY (source_id) REFERENCES `source` (`id`))"])

(def ^:private probemap-columns
  #{:name
    :assembly
    :version
    :text})

(def ^:private probemap-defaults
  (into {} (map #(vector % nil) probemap-columns)))

(def ^:private probemap-meta
  {:table probemap
   :defaults probemap-defaults
   :join probemap_source
   :columns probemap-columns})

(def probe-table
  ["CREATE TABLE IF NOT EXISTS `probe` (
   `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
   `probemap_id` INT NOT NULL,
   `name` VARCHAR(1000) NOT NULL,
   FOREIGN KEY (probemap_id) REFERENCES `probemap` (`id`) ON DELETE CASCADE)"])

; XXX CASCADE might perform badly. Might need to do this incrementally in application code.
(def probe-position-table
  ["CREATE TABLE IF NOT EXISTS `probe_position` (
   `probemap_id` INT NOT NULL,
   `probe_id` INT NOT NULL,
   `bin` INT,
   `chrom` VARCHAR(255) NOT NULL,
   `chromStart` INT NOT NULL,
   `chromEnd` INT NOT NULL,
   `strand` CHAR(1),
   FOREIGN KEY (`probemap_id`) REFERENCES `probemap` (`id`) ON DELETE CASCADE,
   FOREIGN KEY (`probe_id`) REFERENCES `probe` (`id`) ON DELETE CASCADE)"
   "CREATE INDEX IF NOT EXISTS chrom_bin ON probe_position (`probemap_id`, `chrom`, `bin`)"])

(def probe-gene-table
  ["CREATE TABLE IF NOT EXISTS `probe_gene` (
   `probemap_id` INT NOT NULL,
   `probe_id` INT NOT NULL,
   `gene` VARCHAR(255),
    FOREIGN KEY (`probemap_id`) REFERENCES `probemap` (`id`) ON DELETE CASCADE,
    FOREIGN KEY (`probe_id`) REFERENCES `probe` (`id`) ON DELETE CASCADE)"
   "CREATE INDEX IF NOT EXISTS probe_gene ON `probe_gene` (`probemap_id`, `gene`)"])

;
; feature tables
;

(def feature-table
  ["CREATE TABLE IF NOT EXISTS `feature` (
  `id` int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `field_id` int(11) NOT NULL,
  `shortTitle` varchar(255),
  `longTitle` varchar(255),
  `priority` double DEFAULT NULL,
  `valueType` varchar(255) NOT NULL,
  `visibility` varchar(255),
  FOREIGN KEY (`field_id`) REFERENCES `field` (`id`) ON DELETE CASCADE)"])

(declare code)
(defentity feature
  (belongs-to field)
  (has-many code))

(def ^:private feature-columns
  #{:shortTitle
    :longTitle
    :priority
    :valueType
    :visibility})

(def ^:private feature-defaults
  (into {} (map #(vector % nil) feature-columns)))


(def ^:private feature-meta
  {:table feature
   :defaults feature-defaults
   :columns feature-columns})

; Order is important to avoid creating duplicate indexes. A foreign
; key constraint creates an index if one doesn't exist, so create our
; index before adding the constraint.
(def code-table
  ["CREATE TABLE IF NOT EXISTS `code` (
   `id` int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
   `feature_id` int(11) NOT NULL,
   `ordering` int(10) unsigned NOT NULL,
   `value` varchar(255) NOT NULL,
   UNIQUE (`feature_id`, `ordering`),
   FOREIGN KEY (`feature_id`) REFERENCES `feature` (`id`) ON DELETE CASCADE)"])

(defentity code
  (belongs-to feature))

(defn- normalize-meta [m-ent metadata]
  (-> metadata
      (clojure.walk/keywordize-keys)
      (#(merge (:defaults m-ent) %))
      (select-keys (:columns m-ent))))

(defn- json-text [md]
  (assoc md :text (json/write-str md :escape-slash false)))

(defn- load-probe-meta [feature-list]
  (doseq [[field-id feat] feature-list]
    (let [fmeta (normalize-meta feature-meta feat)
          fmeta (merge fmeta {:field_id field-id})
          feature-id (insert feature (values fmeta))
          order (:order feat)]
      (doseq [a-code (:state feat)]
        (insert code (values {:feature_id feature-id
                               :ordering (order a-code)
                               :value a-code}))))))

;
;
;

(defn- fields-in-exp [exp]
  (subselect field (fields :id) (where {:dataset_id exp})))

(defn- scores-with-fields [fs]
  (subselect field_score (fields :score_id) (where {:field_id [in fs]})))

(defn- clear-by-exp [exp]
  (let [f (fields-in-exp exp)]
    (delete sample (where {:dataset_id exp}))
    (delete scores (where {:id [in (scores-with-fields f)]}))
    (delete field_score (where {:field_id [in f]}))
    (delete field (where {:id [in f]}))))

; Merge cohort, returning id
(comment (defn- merge-cohort [cohort]
  (exec-raw ["MERGE INTO cohorts(name) KEY(name) VALUES (?)" [cohort]])
  (let [[{cid :ID}] (select cohorts (where {:name cohort}))]
    cid)))

; Update meta entity record.
(defn- merge-m-ent [m-ent ename metadata]
  (let [normmeta (normalize-meta m-ent (json-text (assoc metadata "name" ename)))
        table (:table m-ent)
        [{id :ID}] (select table (fields :id) (where {:name ename}))]
    (if id
      (do
        (update table (set-fields normmeta) (where {:id id}))
        id)
      (insert table (values normmeta)))))

;
; Hex
;

(def ^:private hex-chars (char-array "0123456789ABCDEF"))

(defn- bytes->hex [^bytes ba]
  (let [c (count ba)
        out ^chars (char-array (* 2 c))]
    (loop [i 0]
      (when (< i c)
        (aset out (* 2 i)
              (aget ^chars hex-chars
                    (bit-shift-right
                      (bit-and 0xF0 (aget ba i))
                      4)))
        (aset out (+ 1 (* 2 i))
              (aget ^chars hex-chars
                    (bit-and 0x0F (aget ba i))))
        (recur (inc i))))
    (String. out)))

;
; Binary buffer manipulation.
;

(defn- sort-float-bytes
  "Sorts a byte array by float byte order. This makes the bytes very slightly
  more compressable."
  [^bytes in]
  (let [c (count in)
        fc (/ c 4)
        out (byte-array c)]
    (loop [i 0
           b1 0
           b2 fc
           b3 (* fc 2)
           b4 (* fc 3)]
      (when (< i c)
        (aset out b1 (aget in i))
        (aset out b2 (aget in (+ i 1)))
        (aset out b3 (aget in (+ i 2)))
        (aset out b4 (aget in (+ i 3)))
        (recur (+ i 4) (inc b1) (inc b2) (inc b3) (inc b4))))
    out))

; Pick elements of float array, by index.
(defn apick-float [^floats a idxs]
 (let [ia (float-array idxs)]
  (amap ia i ret (aget a i))))

(defn ashuffle-float [mapping ^floats out ^floats in]
  (dorun (map #(aset out (% 1) (aget in (% 0))) mapping))
  out)

(defn bytes-to-floats [^bytes barr]
  (let [bb (java.nio.ByteBuffer/allocate (alength barr))
        fb (.asFloatBuffer bb)
        out (float-array (quot (alength barr) 4))]
    (.put bb barr)
    (.get fb out)
    out))

(defn floats-to-bytes [^floats farr]
  (let [bb (java.nio.ByteBuffer/allocate (* (alength farr) 4))
        fb (.asFloatBuffer bb)]
    (.put fb farr)
    (.array bb)))

;
; Score encoders
;

(def codec-length (memoize (fn [len]
                             (binary/repeated :float-le :length len))))

(defn- codec [blob]
  (codec-length (/ (count blob) float-size)))

;(defn- score-decode [blob]
;  (binary/decode (codec blob) (io/input-stream blob)))

(defn- score-decode [blob]
  (bytes-to-floats blob))

(defn- score-encode-orig [slist]
  (floats-to-bytes (float-array slist)))

(defn- score-encodez
  "Experimental compressing score encoder."
  [slist]
  (let [baos (java.io.ByteArrayOutputStream.)
        gzos (java.util.zip.GZIPOutputStream. baos)]
    (binary/encode (codec-length (count slist)) gzos slist)
    (.finish gzos)
    (.toByteArray baos)))

(defn- score-sort-encodez
  "Experimental compressing score encoder that sorts by byte order before compressing."
  [slist]
  (let [fbytes (score-encode-orig slist)
        ^bytes sorted (sort-float-bytes fbytes)
        baos (java.io.ByteArrayOutputStream.)
        gzos (java.util.zip.GZIPOutputStream. baos)]
    (.write gzos sorted)
    (.finish gzos)
    (.toByteArray baos)))

(defn- score-decodez [blob]
  (let [bais (java.io.ByteArrayInputStream. blob)
        gzis (java.util.zip.GZIPInputStream. bais)]
    (binary/decode (binary/repeated :float-le :length 10) gzis)))

(def score-encode score-encode-orig)

;
; Table writer that updates tables as rows are read.
;

; Insert probe & return id
(defn- insert-field [exp name]
  (let [id (insert field (values {:dataset_id exp :name name}))]
    id))

(defn- insert-scores [slist]
  (let [score-id (insert scores (values {:scores slist}))]
    score-id))

(defn- insert-field-score [field-id i score-id]
  (insert field_score (values {:field_id field-id :i i :score_id score-id})))

(defn- table-writer-default [dir f]
  (f {:insert-field insert-field
      :insert-score insert-scores
      :insert-field-score insert-field-score
      :encode score-encode
      :key ahashable}))

;
; Table writer that updates one table at a time by writing to temporary files.
;

(defn- insert-field-out [^java.io.BufferedWriter out seqfn dataset-id name]
  (let [field-id (seqfn)]
    (.write out (str field-id "\t" dataset-id "\t" name "\n"))
    field-id))

; This is a stateful iterator. It may not be idiomatic in clojure, but it's a
; quick way to get sequential ids deep in a call stack. Maybe we should instead
; return a thunk that generates a seq, then zip with the data?

(defn- seq-iterator
  "Create iterator for a seq"
  [s]
  (let [a (atom s)]
    (fn []
      (let [s @a]
        (reset! a (next s))
        (first s)))))

; Generates primary keys using range, w/o accessing the db for each next value.
; This works in h2 when doing a csvread when the sequence has been created
; automatically by h2 with the column: h2 will update the sequence while reading
; the rows. It doesn't work if the sequence is create separately from the column.
; In that case h2 will not update the sequence after a csvread. We have to
; create the sequence separately in order to set the cache parameter.

(comment (defn- sequence-seq [seqname]
   (let [[{val :I}] (exec-raw (format "SELECT CURRVAL('%s') AS I" seqname) :results)]
     (range (+ val 1) Double/POSITIVE_INFINITY 1))))

(defn- sequence-seq
  "Retrieve primary keys in batches by querying the db"
  [seqname]
  (let [cmd (format "SELECT NEXT VALUE FOR %s AS i FROM SYSTEM_RANGE(1, 100)" seqname)]
    (apply concat
           (repeatedly
             (fn [] (-> cmd str
                        (exec-raw :results)
                        (#(map :I %))))))))


(defn- insert-scores-out [^java.io.BufferedWriter out seqfn slist]
  (let [score-id (seqfn)]
    (.write out (str score-id "\t" (:hex slist) "\n"))
    score-id))

(defn- insert-field-score-out [^java.io.BufferedWriter out field-id i score-id]
  (.write out (str field-id "\t" i "\t" score-id "\n")))

(defn- sequence-for-column [table column]
  (-> (exec-raw
        ["SELECT SEQUENCE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=? AND COLUMN_NAME=?"
         [table column]]
        :results)
      first
      :SEQUENCE_NAME))

(defn- read-from-tsv [table file & cols]
  (-> "INSERT INTO %s DIRECT SORTED SELECT * FROM CSVREAD('%s', '%s', 'fieldSeparator=\t')"
      (format table file (clojure.string/join "\t" cols))
      (exec-raw)))

(defn- table-writer [dir f]
  (let [field-seq (-> "FIELD_IDS" sequence-seq seq-iterator)
        scores-seq (-> "SCORES_IDS" sequence-seq seq-iterator)
        ffname (fs/file dir "field.tmp") ; XXX add pid to these, or otherwise make them unique
        sfname (fs/file dir "scores.tmp")
        fsfname (fs/file dir "field_score.tmp")]
    (try
      (let [finish
            (with-open
              [field-file (io/writer ffname)
               scores-file (io/writer sfname)
               field-score-file (io/writer fsfname)]
              (f {:insert-field (partial insert-field-out field-file field-seq)
                  :insert-score (partial insert-scores-out scores-file scores-seq)
                  :insert-field-score (partial insert-field-score-out field-score-file)
                  :encode (fn [scores] (let [s (score-encode scores)] {:scores s :hex (bytes->hex s)}))
                  :key #(ahashable (:scores (first %)))}))]
        (do (read-from-tsv "field" ffname "ID" "DATASET_ID" "NAME")
          (read-from-tsv "scores" sfname "ID" "SCORES")
          (read-from-tsv "field_score" fsfname "FIELD_ID" "I" "SCORES_ID"))
        (finish))
      (finally
        (fs/delete ffname)
        (fs/delete sfname)
        (fs/delete fsfname)))))

;
; Main loader routines
;

(defn- insert-scores-block [writer block]
  ((:insert-score writer) block))

(defn- insert-unique-scores-fn [writer]
  (memoize-key (:key writer) (partial insert-scores-block writer)))

(defn- load-field [writer insert-scores-fn exp row]
  (let [field-id ((:insert-field writer) exp (:field row))
        blocks (:data row)]
    (doseq [[block i] (mapv vector blocks (range))]
      (let [score-id (insert-scores-fn block)]
        ((:insert-field-score writer) field-id i score-id)))
    field-id))

(defn- inferred-type
  "Replace the given type with the inferred type"
  [fmeta row]
  (assoc fmeta "valueType" (:valueType row)))

(defn- load-field-feature [writer insert-scores-fn exp acc row]
  (let [field-id (load-field writer insert-scores-fn exp row)]
    (if-let [feature (:feature row)]
      (cons [field-id (inferred-type feature row)] acc)
      acc)))

(defn- insert-exp-sample [dataset-id samp i]
  (insert sample (values {:dataset_id dataset-id :name samp :i i})))

(defn- load-samples [exp samples]
  (dorun (map #(apply (partial insert-exp-sample exp) %)
              (map vector samples (range)))))

(defn- encode-scores [writer row]
  (mapv (:encode writer) (partition-all bin-size row)))

; insert matrix, updating scores, fields, and field-score tables
(defn- load-exp-matrix [exp matrix-fn writer]
  (let [{:keys [samples fields]} (matrix-fn)]
    (load-samples exp samples)
    (let [scores-fn (insert-unique-scores-fn writer)
          loadp (partial load-field-feature writer scores-fn exp)
          fields (chunked-pmap #(assoc % :data (encode-scores writer (:scores %))) fields)
          field-meta (reduce loadp '() fields)]
      #(load-probe-meta field-meta))))


; XXX Update to return all samples in cohort by merging
; sample.
;(defn- cohort-sample-list [cid sample-list]
;  (select samples (fields :id :sample)
;          (where {:cid cid :sample [in sample-list]})))
;
;; hash seq of maps by given key
;(defn- hash-by-key [k s]
;  (zipmap (map #(% k) s) s))
;
;; return values in order of the given keys
;(defn- in-order [order hm]
;  (map hm order))
;
;; return value of the given key for all objects in seq
;(defn- select-val [k hms]
;  (map #(% k) hms))
;
;(defn- sample-ids [cid sample-list]
;  (->> sample-list
;       (cohort-sample-list cid)
;       (hash-by-key :SAMPLE)
;       (in-order sample-list)
;       (select-val :ID)))

(let [fmtr (formatter "yyyy-MM-dd hh:mm:ss")]
  (defn- format-timestamp [timestamp]
    (unparse fmtr timestamp)))

(defn- fmt-time [file-meta]
  (assoc file-meta :time
         (. java.sql.Timestamp valueOf (format-timestamp (:time file-meta)))))

(defn clean-sources []
  (delete source
          (where (not (in :id (subselect
                                (union (queries
                                         (subselect
                                           dataset_source
                                           (fields [:source_id]))
                                         (subselect
                                           probemap_source
                                           (fields [:source_id]))))))))))

(defn- load-related-sources [table id-key id files]
  (delete table (where {id-key id}))
  (let [sources (map #(insert source (values %)) files)]
    (doseq [source-id sources]
      (insert table
              (values {id-key id :source_id source-id})))))

; work around h2 uppercase craziness
; XXX review passing naming option to korma
(defn- keys-lower [m]
  (into {} (map #(vector (keyword (subs (clojure.string/lower-case (% 0)) 1)) (% 1)) m)))

(defn- related-sources [table id]
  (map keys-lower
    (select table (join source) (where {:id id}) (fields :source.name :source.hash :source.time))))

; kdb/transaction will create a closure of our parameters,
; so we can't pass in the matrix seq w/o blowing the heap. We
; pass in a function to return the seq, instead.

; files: the files this matrix was loaded from, e.g. clinical & genomic matrix, plus their timestamps & file hashes
; metadata: the metadata for the matrix (should cgdata mix in the clinical metadata?)
; matrix-fn: function to return seq of data rows
; features: field metadata. Need a better name for this.

(defn load-exp
  "Load matrix file and metadata. Skips the data load if file hashes are unchanged,
  and 'force' is false. Metadata is always updated."
  ([mname files metadata matrix-fn features]
   (load-exp mname files metadata matrix-fn features false))

  ([mname files metadata matrix-fn features force]
   (profile
     :trace
     :dataset-load
     (kdb/transaction
       (let [exp (merge-m-ent dataset-meta mname metadata)
             files (map fmt-time files)]
         (when (or force
                   (not (= (set files) (set (related-sources dataset exp)))))
           (p :dataset-clear
              (clear-by-exp exp))
           (p :dataset-sources
              (load-related-sources
                dataset_source :dataset_id exp files))
           (p :dataset-table
              (table-writer *tmp-dir*
                            (partial load-exp-matrix exp matrix-fn)))))))))

; XXX factor out common parts with merge, above?
(defn del-exp [file]
  (kdb/transaction
    (let [[{exp :ID}] (select dataset (where {:file file}))]
      (clear-by-exp exp)
      (delete dataset (where {:id exp})))))

;
; probemap routines
;

(defnp add-probemap-probe [pmid a-probe]
  (let [pmp
        (p :probemap-insert-probe
           (insert probe (values {:probemap_id pmid
                                  :name (a-probe :name)})))]
    (p :probemap-insert-position
       (insert probe_position (values {:probemap_id pmid
                                       :probe_id pmp
                                       :bin (calc-bin
                                              (a-probe :chromStart)
                                              (a-probe :chromEnd))
                                       :chrom (a-probe :chrom)
                                       :chromStart (a-probe :chromStart)
                                       :chromEnd (a-probe :chromEnd)
                                       :strand (a-probe :strand)})))
    (p :probemap-insert-genes
       (dorun (map #(insert probe_gene (values {:probemap_id pmid ; XXX doseq?
                                                :probe_id pmp
                                                :gene %}))
                   (a-probe :genes))))))

; kdb/transaction will create a closure of our parameters,
; so we can't pass in the matrix seq w/o blowing the heap. We
; pass in a function to return the seq, instead.
(defn load-probemap
  "Load probemap file and metadata. Skips the data load if the file hashes are unchanged,
  and 'force' is false."
  ([pname files metadata probes-fn]
   (load-probemap pname files metadata probes-fn false))

  ([pname files metadata probes-fn force]
   (profile
     :trace
     :probemap-load
     (kdb/transaction
       (let [probes (probes-fn)
             pmid (merge-m-ent probemap-meta pname metadata)
             add-probe (partial add-probemap-probe pmid)
             files (map fmt-time files)]
         (when (or force
                   (not (= (set files) (set (related-sources probemap pmid)))))
           (p :probemap-sources
              (load-related-sources
                probemap_source :probemap_id pmid files))
           (p :probemap-probes
              (dorun (map add-probe probes))))))))) ; XXX change to doseq?

(defn create-db [file & [{:keys [classname subprotocol delimiters make-pool?]
                          :or {classname "org.h2.Driver"
                               subprotocol "h2"
                               delimiters "`"
                               make-pool? true}}]]
  (kdb/create-db  {:classname classname
                   :subprotocol subprotocol
                   :subname file
                   :delimiters delimiters
                   :make-pool? make-pool?}))

(defmacro with-db [db & body]
  `(kdb/with-db ~db ~@body))

; execute a sequence of sql statements
(defn- exec-statements [stmts]
  (dorun (map exec-raw stmts))) ; XXX doseq

(defn run-query [q]
  ; XXX should sanitize the query
  (let [[qstr & args] (hsql/format q)]
    (exec-raw [qstr args] :results)))

;
; Code queries
;

(defn codes-for-field-query [dataset-id field]
   {:select [:ordering :value]
    :from [:field]
    :left-join [:feature [:= :field_id :field.id]
                :code [:= :feature_id :feature.id]]
    :where [:and [:= :name field] [:= :dataset_id dataset-id]]})

(defn codes-for-values-query [dataset-id field values]
  (merge
    {:join [{:table  [[[:value2 :varchar values ]] :T]} [:= :value :value2]]}
    (codes-for-field-query dataset-id field)))

(defn codes-for-values [dataset-id field values]
  (->> (codes-for-values-query dataset-id field values)
       (run-query)
       (map #(mapv % [:VALUE :ORDERING]))
       (into {})))

;
; All rows queries.
;

; All bins for the given fields.
(defn all-rows-query [dataset-id & fields]
   {:select [:name :i :scores]
    :from [{:select [:*]
            :from [[{:select [:name :id]
                     :from [:field]
                     :join  [{:table  [[[:name2 :varchar fields]] :T]}  [:= :name2 :field.name]]
                     :where [:= :dataset_id dataset-id]} :P]]
            :left-join [:field_score [:= :P.id :field_score.field_id]]}]
    :left-join [:scores [:= :score_id :scores.id]]})

; {"foxm1" [{:NAME "foxm1" :SCORES <bytes>} ... ]
(let [concat-field-bins
      (fn [bins]
        (let [sorted (map :SCORES (sort-by :I bins))
              float-bins (map score-decode sorted)
              row-count (apply + (map count float-bins))
              out (float-array row-count)]
          (loop [bins float-bins offset 0]
            (let [b (first bins)
                  c (count b)]
              (when b
                (System/arraycopy (first bins) 0 out offset c) ; XXX mutate "out" in place
                (recur (rest bins) (+ offset c)))))
          out))]

  ; return float arrays of all rows for the given fields
  (defn all-rows [dataset-id fields]
    (let [field-bins (->> fields
                          (apply all-rows-query dataset-id )
                          (run-query)
                          (group-by :NAME))]
      (into {} (for [[k v] field-bins] [k (concat-field-bins v)])))))

;
;
;

(defn dataset-by-name [dname]
  (let [[{id :ID}]
        (select dataset (fields "id") (where {:name (str dname)}))] ; XXX shoul str be here, or higher in the call stack?
    id))

; merge bin number and bin offset for a sample.
(defn- merge-bin-off [{i :i :as row}]
  (merge row
         {:bin (quot i bin-size)
          :off (rem i bin-size)}))

; Returns mapping from input bin offset to output buffer for a
; given row.
(defn- bin-mapping [{off :off row :order}]
  [off row])


; Gets called once for each bin in the request.
; order is row -> order in output buffer.
; bin is the bin for the given set of samples.
; rows is ({:I 12 :bin 1 :off 1}, ... ), where all :bin are the same
(defn- pick-samples-fn [[bin rows]]
  [bin (partial ashuffle-float
                (map bin-mapping rows))])

; Take an ordered list of requested rows, and generate fns to copy
; values from a score bin to an output array. Returns one function
; for each score bin in the request.
;
; rows ({:I 12 :bin 1 :off 1}, ... )
(defn- pick-samples-fns [rows]
  (let [by-bin (group-by :bin rows)]
    (apply hash-map (mapcat pick-samples-fn by-bin))))

; Take map of bin copy fns, list of scores rows, and a map of output arrays,
; copying the scores to the output arrays via the bin copy fns.
(defn- build-score-arrays [rows bfns out]
  (dorun (map (fn [{i :I scores :SCORES field :NAME}]
                ((bfns i) (out field) scores))
              rows))
  out)

(defn- col-arrays [columns n]
  (zipmap columns (repeatedly (partial float-array n Double/NaN))))

; Doing an inner join on a table literal, instead of a big IN clause, so it will hit
; the index.

; XXX performance of the :in clause?
; XXX change "gene" to "field"
(def field-query
  "SELECT  name, i, scores FROM
     (SELECT * FROM (SELECT  `field`.`name`, `field`.`id`  FROM `field`
       INNER JOIN TABLE(name varchar=?) T ON T.`name`=`field`.`name`
       WHERE (`field`.`dataset_id` = ?)) P
   LEFT JOIN `field_score` ON P.id = `field_score`.`field_id` WHERE `field_score`.`i` IN (%s))
   LEFT JOIN `scores` ON `score_id` = `scores`.`id`")

(defn select-scores-full [dataset-id columns bins]
  (let [q (format field-query
                  (clojure.string/join ","
                                       (repeat (count bins) "?")))
        c (to-array (map str columns))
        i (to-array bins)]
    (exec-raw [q (concat [c dataset-id] i)] :results)))

; Returns rows from (dataset-id column-name) matching
; values. Returns a hashmap for each matching  row.
; { :i     The implicit index in the column as stored on disk.
;   :bin   The bin number on disk.
;   :off   The offset in the bin number.
;   ;order The order of the row the result set.
;   :value The value of the row.
; }
(defn rows-matching [dataset-id column-name values]
  (let [val-set (set values)]
    (->> column-name
         (vector)
         (all-rows dataset-id)                 ; Retrieve the full column
         (vals)
         (first)
                                               ; calculate row index in the column
         (map #(-> {:i %1 :value %2}) (range)) ; [{:i 0 :value 1.2} ...]
         (filter (comp val-set :value))        ; filter rows not matching the request.
         (group-by :value)
         (#(map (fn [g] (get % g [nil])) values)) ; arrange by output order.
         (apply concat)                           ; flatten groups.
         (mapv #(assoc %2 :order %1) (range)))))  ; assoc row output order. 

(defn float-nil [x]
  (when x (float x)))

; Get codes for samples in request
; Get the entire sample column
; Find the rows matching the samples, ordered by sample list
;   Have to scan the sample column, group by sample, and map over the ordering
; Build shuffle functions, per bin, to copy bins to output buffer
; Run scores query
; Copy to output buffer

(defn genomic-read-req [req]
  (let [{samples 'samples table 'table columns 'columns} req
        dataset-id (dataset-by-name table)
        samp->code (codes-for-values dataset-id "sampleID" samples)
        order (map (comp float-nil samp->code) samples)     ; codes in request order
        rows (rows-matching dataset-id "sampleID" order)
        rows-to-copy (->> rows
                          (filter :value)
                          (mapv merge-bin-off))

        bins (map :bin rows-to-copy)            ; list of bins to pull.
        bfns (pick-samples-fns rows-to-copy)]   ; make fns that map from input bin to
                                                ; output buffer, one fn per input bin.

    (-> (select-scores-full dataset-id columns (distinct bins))
        (#(map cvt-scores %))
        (build-score-arrays bfns (col-arrays columns (count rows))))))

; Each req is a map of
;  'table "tablename"
;  'columns '("column1" "column2")
;  'samples '("sample1" "sample2")
; We merge into 'data the columns that we can resolve, like
;  'data { 'column1 [1.1 1.2] 'column2 [2.1 2.2] }
(defn genomic-source [reqs]
  (map #(update-in % ['data] merge (genomic-read-req %)) reqs))

(defn create[]
  (kdb/transaction
    (exec-statements cohorts-table)
    (exec-statements source-table)
    (exec-statements dataset-table)
    (exec-statements dataset-source-table)
    (exec-statements sample-table)
    (exec-statements scores-table)
    (exec-statements field-score-table)
    (exec-statements field-table)
    (exec-statements feature-table)
    (exec-statements code-table)
    (exec-statements probemap-table)
    (exec-statements probemap-source-table)
    (exec-statements probe-table)
    (exec-statements probe-position-table)
    (exec-statements probe-gene-table)))

;
; add TABLE handling for honeysql
;

(defmethod hsqlfmt/format-clause :table [[_ [fields alias]] _]
  (str "TABLE("
       (clojure.string/join ", "
       (map (fn [[name type values]]
              (str (hsqlfmt/to-sql name) " " (hsqlfmt/to-sql type) "="
                   (hsqlfmt/paren-wrap (clojure.string/join ", " (map hsqlfmt/to-sql values))))) fields))
       ") " (hsqlfmt/to-sql alias)))

; Fix GROUP_CONCAT in honeysql
(defn- format-concat
  ([value {:keys [order separator]}]
   (let [oclause (if (nil? order)
                   ""
                   (str " order by " (hsqlfmt/to-sql order)))
         sclause (if (nil? separator)
                   ""
                   (str " separator " (hsqlfmt/to-sql separator)))]
     (str "GROUP_CONCAT(" (hsqlfmt/to-sql value) oclause sclause ")"))))

(defmethod hsqlfmt/fn-handler "group_concat" [_ value & args]
  (format-concat value args))

; XXX monkey-patch korma to work around h2 bug.
; h2 will fail to select an index if joins are grouped, e.g.
; FROM (a JOIN b) JOIN c
; We work around it by replacing the function that adds the
; grouping.
(in-ns 'korma.sql.utils)
(defn left-assoc [v]
  (clojure.string/join " " v))
(in-ns 'cavm.h2)

(defrecord H2Db [db])

(extend-protocol XenaDb H2Db
  (write-matrix [this mname files metadata data-fn features always]
    (with-db (:db this)
      (load-exp mname files metadata data-fn features always)))
  (write-probemap [this pname files metadata data-fn always]
    (with-db (:db this)
      (load-probemap pname files metadata data-fn always)))
  (run-query [this query]
    (with-db (:db this)
      (run-query query)))
  (fetch [this reqs]
    (with-db (:db this)
      (doall (genomic-source reqs))))
  (close [this]
    (.close (:datasource @(:pool (:db this))) false)))

(defn create-xenadb [& args]
  (let [db (apply create-db args)]
    (with-db db (create))
    (H2Db. db)))
