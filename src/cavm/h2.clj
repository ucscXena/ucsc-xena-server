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
  (:gen-class))


; Coerce this sql fn name to a keyword so we can reference it.
(def KEY-ID (keyword "SCOPE_IDENTITY()"))

; provide an insert macro that directly returns an id, instead of a hash
; on SCOPE_IDENTITY().
(defmacro insert [& args] `(KEY-ID (kcinsert ~@args)))

(def float-size 4)
(def bin-size 100)
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

(declare experiment_sources features probes scores experiments exp_samples)

(defentity cohorts)
(defentity sources)

(defentity experiments
  (many-to-many sources :experiment_sources)
  (has-many exp_samples)
  (has-many probes))

(defentity exp_samples
  (belongs-to experiments))

(declare score-decode)

(defn- cvt-scores [{scores :EXPSCORES :as v}]
  (if scores
    (assoc v :EXPSCORES (float-array (score-decode scores)))
    v))

(defentity probes
  (has-one features)
  (many-to-many scores :joins  {:lfk 'pid :rfk 'sid})
  (belongs-to experiments {:fk :eid})
  (transform cvt-scores))

(defentity scores
  (many-to-many probes :joins  {:lfk 'sid :rfk 'pid}))

(defentity joins)

;
; Table definitions.
;

; Note H2 has one int type: signed, 4 byte. Max is approximately 2 billion.
(def probes-table
  ["CREATE TABLE IF NOT EXISTS `probes` (
   `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
   `eid` INT NOT NULL,
   `name` VARCHAR(255),
   UNIQUE(`eid`, `name`),
   FOREIGN KEY (`eid`) REFERENCES `experiments` (`id`))"])

(def scores-table
  [(format "CREATE TABLE IF NOT EXISTS `scores` (
           `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
           `expScores` VARBINARY(%d) NOT NULL)" score-size)])

(def join-table
  ["CREATE TABLE IF NOT EXISTS `joins` (
   `pid` INT,
   `i` INT,
   `sid` INT,
   UNIQUE (`pid`, `i`))"])

(def cohorts-table
  ["CREATE TABLE IF NOT EXISTS `cohorts` (
   `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
   `name` VARCHAR(2000) NOT NULL UNIQUE)"])

; XXX What should max file name length be?
; XXX Unique columns? Indexes?
(def sources-table
  ["CREATE TABLE IF NOT EXISTS `sources` (
   `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
   `name` VARCHAR(2000) NOT NULL,
   `time` TIMESTAMP NOT NULL,
   `hash` VARCHAR(40) NOT NULL)"])

; XXX FIX NAME
; XXX Include original json
(def experiments-table
  ["CREATE TABLE IF NOT EXISTS `experiments` (
   `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
   `name` varchar(255) NOT NULL UNIQUE,
   `probeMap` varchar(255),
   `shortTitle` varchar(255),
   `longTitle` varchar(255),
   `groupTitle` varchar(255),
   `platform` varchar(255),
   `security` varchar(255),
   `gain` double DEFAULT NULL,
   `text` varchar (65535),
   `dataSubType` varchar (255))"])

;
; experiments in making a macro for metadata tables
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
;(defmeta experiments
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


(def ^:private experiments-columns
  #{:name
    :probeMap
    :shortTitle
    :longTitle
    :groupTitle
    :platform
    :security
    :dataSubType
    :text
    :gain})

(def ^:private experiments-defaults
  (into {} (map #(vector % nil) experiments-columns)))

; XXX should abstract this to set up the many-to-many
; on the defentity. Can we run many-to-many on the existing
; sources entity?? Or maybe do defentity sources *after* all
; the other tables exist?
(def experiment-sources-table
  ["CREATE TABLE IF NOT EXISTS `experiment_sources` (
   `experiments_id` INT NOT NULL,
   `sources_id` INT NOT NULL,
   FOREIGN KEY (experiments_id) REFERENCES `experiments` (`id`),
   FOREIGN KEY (sources_id) REFERENCES `sources` (`id`))"])

(defentity experiment_sources)

(def ^:private experiments-meta
  {:table experiments
   :defaults experiments-defaults
   :join experiment_sources
   :columns experiments-columns})

(def exp-samples-table
  ["CREATE TABLE IF NOT EXISTS `exp_samples` (
   `experiments_id` INT NOT NULL,
   FOREIGN KEY (experiments_id) REFERENCES `experiments` (`id`),
   `i` INT NOT NULL,
   `name` VARCHAR (1000) NOT NULL,
   PRIMARY KEY(`experiments_id`, `i`))"]) ; XXX what should this be?

;
; Probemap declarations
;

(declare probmaps probemap_probes probemap_positions probemap_genes)
(defentity probemap_sources)

(defentity probemaps
  (has-many probemap_probes)
  (many-to-many sources :probemap_sources))

(defentity probemap_probes
  (belongs-to probemaps)
  (has-many probemap_positions)
  (has-many probemap_genes))

(defentity probemap_positions
  (belongs-to probemap_probes))

(defentity probemap_genes
  (belongs-to probemap_probes))


; XXX What should max file name length be?
(def probemaps-table
  ["CREATE TABLE IF NOT EXISTS `probemaps` (
   `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
   `name` VARCHAR(1000),
   `assembly` VARCHAR(255),
   `version` VARCHAR(255),
   `text` VARCHAR(65535))"])

(def probemap-sources-table
  ["CREATE TABLE IF NOT EXISTS `probemap_sources` (
   `probemaps_id` INT NOT NULL,
   `sources_id` INT NOT NULL,
   FOREIGN KEY (probemaps_id) REFERENCES `probemaps` (`id`),
   FOREIGN KEY (sources_id) REFERENCES `sources` (`id`))"])

(def ^:private probemaps-columns
  #{:name
    :assembly
    :version
    :text})

(def ^:private probemaps-defaults
  (into {} (map #(vector % nil) probemaps-columns)))

(def ^:private probemaps-meta
  {:table probemaps
   :defaults probemaps-defaults
   :join probemap_sources
   :columns probemaps-columns})

; if probemap is a file name, we probably want a separate table.
; Should we reuse the experiments table?? Or duplicate all the fields??
(def probemap-probes-table
  ["CREATE TABLE IF NOT EXISTS `probemap_probes` (
   `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
   `probemaps_id` INT NOT NULL,
   `probe` VARCHAR(1000) NOT NULL,
   FOREIGN KEY (probemaps_id) REFERENCES `probemaps` (`id`) ON DELETE CASCADE)"])

; XXX CASCADE might perform badly. Might need to do this incrementally in application code.
(def probemap-positions-table
  ["CREATE TABLE IF NOT EXISTS `probemap_positions` (
   `probemaps_id` INT NOT NULL,
   `probemap_probes_id` INT NOT NULL,
   `bin` INT,
   `chrom` VARCHAR(255) NOT NULL,
   `chromStart` INT NOT NULL,
   `chromEnd` INT NOT NULL,
   `strand` CHAR(1),
   FOREIGN KEY (`probemaps_id`) REFERENCES `probemaps` (`id`) ON DELETE CASCADE,
   FOREIGN KEY (`probemap_probes_id`) REFERENCES `probemap_probes` (`id`) ON DELETE CASCADE)"
   "CREATE INDEX IF NOT EXISTS chrom_bin ON probemap_positions (`probemaps_id`, `chrom`, `bin`)"
   ])

(def probemap-genes-table
  ["CREATE TABLE IF NOT EXISTS `probemap_genes` (
   `probemaps_id` INT NOT NULL,
   `probemap_probes_id` INT NOT NULL,
   `gene` VARCHAR(255),
    FOREIGN KEY (`probemaps_id`) REFERENCES `probemaps` (`id`) ON DELETE CASCADE,
    FOREIGN KEY (`probemap_probes_id`) REFERENCES `probemap_probes` (`id`) ON DELETE CASCADE)"
   "CREATE INDEX IF NOT EXISTS probemap_gene ON `probemap_genes` (`probemaps_id`, `gene`)"
   ])

;
; feature tables
;

(def features-table
  ["CREATE TABLE IF NOT EXISTS `features` (
  `id` int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `probes_id` int(11) NOT NULL,
  `shortTitle` varchar(255),
  `longTitle` varchar(255),
  `priority` double DEFAULT NULL,
  `valueType` varchar(255) NOT NULL,
  `visibility` varchar(255),
  FOREIGN KEY (`probes_id`) REFERENCES `probes` (`id`) ON DELETE CASCADE)"])

(declare codes)
(defentity features
  (belongs-to probes)
  (has-many codes))

(def ^:private features-columns
  #{:shortTitle
    :longTitle
    :priority
    :valueType
    :visibility})

(def ^:private features-defaults
  (into {} (map #(vector % nil) features-columns)))


(def ^:private features-meta
  {:table features
   :defaults features-defaults
   :columns features-columns})

; Order is important to avoid creating duplicate indexes. A foreign
; key constraint creates an index if one doesn't exist, so create our
; index before adding the constraint.
(def codes-table
  ["CREATE TABLE IF NOT EXISTS `codes` (
   `id` int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
   `features_id` int(11) NOT NULL,
   `ordering` int(10) unsigned NOT NULL,
   `value` varchar(255) NOT NULL,
   UNIQUE (`features_id`, `ordering`),
   FOREIGN KEY (`features_id`) REFERENCES `features` (`id`) ON DELETE CASCADE)"])

(defentity codes
  (belongs-to features))

(defn- normalize-meta [m-ent metadata]
  (-> metadata
      (clojure.walk/keywordize-keys)
      (#(merge (:defaults m-ent) %))
      (select-keys (:columns m-ent))))

(defn- json-text [md]
  (assoc md :text (json/write-str md :escape-slash false)))

(defn- load-probe-meta [feature-list]
  (doseq [[pid feature] feature-list]
    (let [fmeta (normalize-meta features-meta feature)
          fmeta (merge fmeta {:probes_id pid})
          fid (insert features (values fmeta))
          order (:order feature)]
      (doseq [code (:state feature)]
        (insert codes (values {:features_id fid
                               :ordering (order code)
                               :value code}))))))

;
;
;

(defn- probes-in-exp [exp]
  (subselect probes (fields :id) (where {:eid exp})))

(defn- scores-with-probes [probes]
  (subselect joins (fields :sid) (where {:pid [in probes]})))

(defn- clear-by-exp [exp]
  (let [p (probes-in-exp exp)]
    (delete exp_samples (where {:experiments_id exp}))
    (delete scores (where {:id [in (scores-with-probes p)]}))
    (delete joins (where {:pid [in p]}))
    (delete probes (where {:id [in p]}))))

; Merge cohort, returning id
(defn- merge-cohort [cohort]
  (exec-raw ["MERGE INTO cohorts(name) KEY(name) VALUES (?)" [cohort]])
  (let [[{cid :ID}] (select cohorts (where {:name cohort}))]
    cid))

; Update meta entity record.
(defn- merge-m-ent [m-ent {ename "name" :as metadata}]
  (let [normmeta (json-text (normalize-meta m-ent metadata))
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
(defn- insert-probe [exp name]
  (let [pid (insert probes (values {:eid exp :name name}))]
    pid))

(defn- insert-scores [slist]
  (let [sid (insert scores (values {:expScores slist}))]
    sid))

(defn- insert-join [pid i sid]
  (insert joins (values {:pid pid :i i :sid sid})))

(defn- table-writer-default [dir f]
  (f {:insert-probe insert-probe
         :insert-score insert-scores
         :insert-join insert-join
         :encode score-encode
         :key ahashable}))

;
; Table writer that updates one table at a time by writing to temporary files.
;

(defn- insert-probe-out [^java.io.BufferedWriter out seqfn eid name]
  (let [pid (seqfn)]
    (.write out (str pid "\t" eid "\t" name "\n"))
    pid))

; Iterators are not typical of clojure, but it's a quick way to get sequential ids
; deep in a call stack. A more conventional way might be to return the data up
; the stack, then zip with the sequence ids.
(defn- seq-iterator [s]
    (let [a (atom s)]
    (fn []
       (let [s @a]
          (reset! a (next s))
          (first s)))))

(defn- sequence-seq [seqname]
  (let [cmd (format "SELECT NEXT VALUE FOR %s AS i FROM SYSTEM_RANGE(1, 100)" seqname)]
    (apply concat
           (repeatedly
             (fn [] (-> cmd str
                        (exec-raw :results)
                        (#(map :I %))))))))

(defn- insert-scores-out [^java.io.BufferedWriter out seqfn slist]
  (let [sid (seqfn)]
    (.write out (str sid "\t" (:hex slist) "\n"))
    sid))

(defn- insert-joins-out [^java.io.BufferedWriter out pid i sid]
  (.write out (str pid "\t" i "\t" sid "\n")))

(defn- sequence-for-column [table column]
  (-> (exec-raw
        ["SELECT SEQUENCE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=? AND COLUMN_NAME=?"
         [table column]]
        :results)
      first
      :SEQUENCE_NAME))

(defn- read-from-tsv [table file & cols]
  (-> "INSERT INTO %s SELECT * FROM CSVREAD('%s', '%s', 'fieldSeparator=\t')"
      (format table file (clojure.string/join "\t" cols))
      (exec-raw)))

(defn- table-writer [dir f]
  (let [probes-seq (-> (sequence-for-column "PROBES" "ID") sequence-seq seq-iterator)
        scores-seq (-> (sequence-for-column "SCORES" "ID") sequence-seq seq-iterator)
        pfname (fs/file dir "probes.tmp")
        sfname (fs/file dir "scores.tmp")
        jfname (fs/file dir "joins.tmp")]
    (try
      (let [finish
            (with-open
              [probes-file (io/writer pfname)
               scores-file (io/writer sfname)
               joins-file (io/writer jfname)]
              (f {:insert-probe (partial insert-probe-out probes-file probes-seq)
                  :insert-score (partial insert-scores-out scores-file scores-seq)
                  :insert-join (partial insert-joins-out joins-file)
                  :encode (fn [scores] (let [s (score-encode scores)] {:scores s :hex (bytes->hex s)}))
                  :key #(ahashable (:scores (first %)))}))]
        (do (read-from-tsv "probes" pfname "PID" "EID" "NAME")
          (read-from-tsv "scores" sfname "ID" "SCORES")
          (read-from-tsv "joins" jfname "PID" "I" "SID"))
        (finish))
      (finally
        (fs/delete pfname)
        (fs/delete sfname)
        (fs/delete jfname)))))

;
; Main loader routines
;

(defn- insert-scores-block [writer block]
  ((:insert-score writer) block))

(defn- insert-unique-scores-fn [writer]
  (memoize-key (:key writer) (partial insert-scores-block writer)))

(defn- load-probe [writer insert-scores-fn exp row]
  (let [pid ((:insert-probe writer) exp (:field row))
        blocks (:data row)]
    (doseq [[block i] (mapv vector blocks (range))]
      (let [sid (insert-scores-fn block)]
        ((:insert-join writer) pid i sid)))
    pid))

(defn- inferred-type
  "Replace the given type with the inferred type"
  [fmeta row]
  (assoc fmeta "valueType" (:valueType row)))

(defn- load-probe-feature [writer insert-scores-fn exp acc row]
  (let [pid (load-probe writer insert-scores-fn exp row)]
    (if-let [feature (:feature row)]
      (cons [pid (inferred-type feature row)] acc)
      acc)))

(defn- insert-exp-sample [eid sample i]
  (insert exp_samples (values {:experiments_id eid :name sample :i i})))

(defn- load-exp-samples [exp samples]
  (dorun (map #(apply (partial insert-exp-sample exp) %)
              (map vector samples (range)))))

(defn- encode-scores [writer row]
  (mapv (:encode writer) (partition-all bin-size row)))

; insert matrix, updating scores, probes, and joins tables
(defn- load-exp-matrix [exp matrix-fn writer]
  (let [{:keys [samples fields]} (matrix-fn)]
    (load-exp-samples exp samples)
    (let [scores-fn (insert-unique-scores-fn writer)
          loadp (partial load-probe-feature writer scores-fn exp)
          fields (chunked-pmap #(assoc % :data (encode-scores writer (:scores %))) fields)
          probe-meta (reduce loadp '() fields)]
      #(load-probe-meta probe-meta))))


; XXX Update to return all samples in cohort by merging
; exp_samples.
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
  (delete sources
          (where (not (in :id (subselect
                                (union (queries
                                         (subselect
                                           experiment_sources
                                           (fields [:sources_id]))
                                         (subselect
                                           probemap_sources
                                           (fields [:sources_id]))))))))))

(defn- load-related-sources [table id-key id files]
  (delete table (where {id-key id}))
  (let [fids (map #(insert sources (values %)) files)]
    (doseq [fid fids]
      (insert table
              (values {id-key id :sources_id fid})))))

; work around h2 uppercase craziness
; XXX review passing naming option to korma
(defn- keys-lower [m]
  (into {} (map #(vector (keyword (subs (clojure.string/lower-case (% 0)) 1)) (% 1)) m)))

(defn- related-sources [table id]
  (map keys-lower
    (select table (join sources) (where {:id id}) (fields :sources.name :sources.hash :sources.time))))

; kdb/transaction will create a closure of our parameters,
; so we can't pass in the matrix seq w/o blowing the heap. We
; pass in a function to return the seq, instead.

(defn load-exp
  "Load matrix file and metadata. Skips the data load if file hashes are unchanged,
  and 'force' is false."
  ([files metadata matrix-fn features]
   (load-exp files metadata matrix-fn features false))

  ([files metadata matrix-fn features force]
   (kdb/transaction
     (let [exp (merge-m-ent experiments-meta metadata)
           files (map fmt-time files)]
       (when (or force
                 (not (= (set files) (set (related-sources experiments exp)))))
         (clear-by-exp exp)
         (load-related-sources
           experiment_sources :experiments_id exp files)
         (table-writer "/dev/shm" ; XXX use db dir? root dir? tmp dir?
                       (partial load-exp-matrix exp matrix-fn)))))))

; XXX factor out common parts with merge, above?
(defn del-exp [file]
  (kdb/transaction
    (let [[{exp :ID}] (select experiments (where {:file file}))]
      (clear-by-exp exp)
      (delete experiments (where {:id exp})))))

;
; probemap routines
;

(defn- add-probemap-probe [pmid probe]
  (let [pmp
        (insert probemap_probes (values {:probemaps_id pmid
                                         :probe (probe :name)}))]
    (insert probemap_positions (values {:probemaps_id pmid
                                        :probemap_probes_id pmp
                                        :bin (calc-bin
                                               (probe :chromStart)
                                               (probe :chromEnd))
                                        :chrom (probe :chrom)
                                        :chromStart (probe :chromStart)
                                        :chromEnd (probe :chromEnd)
                                        :strand (probe :strand)}))
    (dorun (map #(insert probemap_genes (values {:probemaps_id pmid
                                                 :probemap_probes_id pmp
                                                 :gene %}))
                (probe :genes)))))

; kdb/transaction will create a closure of our parameters,
; so we can't pass in the matrix seq w/o blowing the heap. We
; pass in a function to return the seq, instead.
(defn load-probemap
  "Load probemap file and metadata. Skips the data load if the file hashes are unchanged,
  and 'force' is false."
  ([files metadata probes-fn]
   (load-probemap files metadata probes-fn false))

  ([files metadata probes-fn force]
   (kdb/transaction
     (let [probes (probes-fn)
           pmid (merge-m-ent probemaps-meta metadata)
           add-probe (partial add-probemap-probe pmid)
           files (map fmt-time files)]
       (when (or force
                 (not (= (set files) (set (related-sources probemaps pmid)))))
         (load-related-sources
           probemap_sources :probemaps_id pmid files)
         (dorun (map add-probe probes)))))))

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
  (dorun (map exec-raw stmts)))

; TODO drop N
(defn- dataset-transform [ds]
  (-> ds
      (clojure.set/rename-keys {:FILE :name :COHORT :cohort})
      (#(assoc % :longlabel (% :name) :shortlabel (% :name) :N 100))))

; XXX Should this be in a transaction to ensure data consistency?
(defn datasets []
  (->> (select experiments (with cohorts) (fields :FILE [:cohorts.name :cohort]))
       (map dataset-transform)))

; Intersect experiment samples with the given set of samples.
(defn exp-samples-in-list [exp samps]
  (select exp_samples (fields :i :name)
          (where  {:experiments_id exp}) (where (in :name samps))))

(defn sample-bins [samps]
  (set (map #(quot (% :I) bin-size) samps)))

(defn select-scores []
  (-> (select* probes)
      (fields :name [:scores.expScores :expScores] [:joins.i :i])
      (join scores)))

(defn with-bins [q bins]
  (where q (in :i (distinct bins))))

(defn for-experiment [q exp]
  (where q {:probes.eid exp}))

(defn for-experiment-named [q exp]
  (for-experiment q (subselect experiments (fields "id") (where {:file exp}))))

(defn exp-by-name [exp]
  (let [[{id :ID}]
        (select experiments (fields "id") (where {:name (str exp)}))] ; XXX shoul str be here, or higher in the call stack?
    id))

; XXX expand model for probes/genes??
(defn with-genes [q genes]
  (fields (where q {:probes.name [in genes]}) [:probes.name :gene]))

(defn do-select [q]
  (exec q))

; merge bin number and bin offset for a sample.
(defn- merge-bin-off [sample]
  (let [{i :I} sample]
    (assoc sample :bin (quot i bin-size) :off (rem i bin-size))))

(defn- bin-mapping [order {off :off sample :NAME}]
  [off (order sample)])

(defn- pick-samples-fn [order [bin samples]]
  [bin (partial ashuffle-float
                (map (partial bin-mapping order) samples))])

; Take an ordered list of requested samples, a list of samples (with indexes) in the
; experiment, and generate fns to copy values from a score bin to an output array.
; Returns one function for each score bin in the request.

(defn- pick-samples-fns [s-req s-exp]
  (let [order (zipmap s-req (range))
        by-bin (group-by :bin s-exp)]
    (apply hash-map (mapcat (partial pick-samples-fn order) by-bin))))

; Take map of bin copy fns, list of scores rows, and a map of output arrays,
; copying the scores to the output arrays via the bin copy fns.
(defn- build-score-arrays [rows bfns out]
  (dorun (map (fn [{i :I scores :EXPSCORES gene :GENE}]
                ((bfns i) (out gene) scores))
              rows))
  out)

(defn- col-arrays [columns n]
  (zipmap columns (repeatedly (partial float-array n))))

; Replaced this with the query below, which applies the bin id filter
; earlier.
(comment (def probe-query
  "SELECT  gene, i, expscores from
     (SELECT  `probes`.`name` as `gene`, `probes`.`id`  FROM `probes`
       INNER JOIN TABLE(name varchar=?) T ON T.`name`=`probes`.`name`
       WHERE (`probes`.`eid` = ?)) P
   LEFT JOIN `joins` on P.id = `joins`.`pid`
   LEFT JOIN `scores` ON `sid` = `scores`.`id`
   WHERE `joins`.`i` in (%s)"))

(def probe-query
  "SELECT  gene, i, expscores from
     (SELECT * FROM (SELECT  `probes`.`name` as `gene`, `probes`.`id`  FROM `probes`
       INNER JOIN TABLE(name varchar=?) T ON T.`name`=`probes`.`name`
       WHERE (`probes`.`eid` = ?)) P
   LEFT JOIN `joins` on P.id = `joins`.`pid` WHERE `joins`.`i` in (%s))
   LEFT JOIN `scores` ON `sid` = `scores`.`id`")

(defn select-scores-full [eid columns bins]
  (let [q (format probe-query
                  (clojure.string/join ","
                                       (repeat (count bins) "?")))
        c (to-array (map str columns))
        i (to-array bins)]
    (exec-raw [q (concat [c eid] i)] :results)))

; Doing an inner join on a table literal, instead of a big IN clause, so it will hit
; the index.
(defn genomic-read-req [req]
  (let [{samples 'samples table 'table columns 'columns} req
        eid (exp-by-name table)
        s-in-exp (map merge-bin-off (exp-samples-in-list eid samples))
        bins (map :bin s-in-exp)
        bfns (pick-samples-fns samples s-in-exp)]

    (-> (select-scores-full eid columns (distinct bins))
        (#(map cvt-scores %))
        (build-score-arrays bfns (col-arrays columns (count samples))))))

; XXX Need to fill in NAN for unknown samples.
; XXX call (double-array) to convert to double here, or in genomic-source
; XXX This query is slow
(comment (defn genomic-read-req [req]
  (let [{samples 'samples table 'table columns 'columns} req
        eid (exp-by-name table)
        s-in-exp (map merge-bin-off (exp-samples-in-list eid samples))
        bins (map :bin s-in-exp)
        bfns (pick-samples-fns samples s-in-exp)]

    (-> (select-scores)
        (for-experiment eid)
        (with-genes columns)
        (with-bins bins)
        (do-select)
        (build-score-arrays bfns (col-arrays columns (count samples)))))))

; doall is required so the seq is evaluated in db context.
; Otherwise lazy map may not be evaluted until the context is lost.
(defn genomic-source [reqs]
  (doall (map #(update-in % ['data] merge (genomic-read-req %)) reqs)))

(sources/register ::genomic genomic-source)

(defn create[]
  (kdb/transaction
    (exec-statements cohorts-table)
    (exec-statements sources-table)
    (exec-statements experiments-table)
    (exec-statements experiment-sources-table)
    (exec-statements exp-samples-table)
    (exec-statements scores-table)
    (exec-statements join-table)
    (exec-statements probes-table)
    (exec-statements features-table)
    (exec-statements codes-table)
    (exec-statements probemaps-table)
    (exec-statements probemap-sources-table)
    (exec-statements probemap-probes-table)
    (exec-statements probemap-positions-table)
    (exec-statements probemap-genes-table)))

(defn run-query [q]
  ; XXX should sanitize the query
  (let [[qstr & args] (hsql/format q)]
    (exec-raw [qstr args] :results)))

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
  (write-matrix [this files metadata data-fn features always]
    (with-db (:db this)
      (load-exp files metadata data-fn features always)))
  (write-probemap [this files metadata data-fn features always]
    (with-db (:db this)
      (load-probemap files metadata data-fn always)))
  (run-query [this query]
    (with-db (:db this)
      (run-query query)))
  (close [this]
    (.close (:datasource @(:pool (:db this))) false)))

; XXX rename
(defn create-db2 [& args]
  (let [db (apply create-db args)]
    (with-db db (create))
    (H2Db. db)))
