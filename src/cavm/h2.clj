(ns cavm.h2
  (:require [korma.db :as kdb])
  (:require [korma.config :as kconf])
  (:require [org.clojars.smee.binary.core :as binary])
  (:require [clojure.java.io :as io])
  (:use [cavm.binner :only (calc-bin)])
  (:use [clj-time.format :only (formatter unparse)])
  (:use [cavm.hashable :only (ahashable get-array)])
  (:use [korma.core :rename {insert kcinsert}])
  (:gen-class))

;(def db {:classname "org.h2.Driver"
;         :subprotocol "h2"
;         :subname "file:///data/TCGA/craft/h2/cavm.h2"})

; Coerce this sql fn name to a keyword so we can reference it.
(def KEY-ID (keyword "SCOPE_IDENTITY()"))

; provide an insert macro that directly returns an id, instead of a hash
; on SCOPE_IDENTITY().
(defmacro insert [& args] `(KEY-ID (kcinsert ~@args)))

(def float-size 4)
(def bin-size 100)
(def score-size (* float-size bin-size))

;
; Table models
;

(declare experiment_sources probes scores experiments exp_samples)

(defentity cohorts)
(defentity sources
  (many-to-many experiment_sources experiments))

(defentity experiments
  (many-to-many experiment_sources sources)
  (has-many exp_samples)
  (has-many probes))

(defentity exp_samples
  (belongs-to experiments))

(declare score-decode)

(defentity probes
  (many-to-many scores :joins  {:lfk 'pid :rfk 'sid})
  (belongs-to experiments {:fk :eid})
  (transform (fn [{scores :EXPSCORES :as v}]
               (if scores
                 (assoc v :EXPSCORES (float-array (score-decode scores)))
                 v))))

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
   FOREIGN KEY (eid) REFERENCES `experiments` (`id`),
   `name` VARCHAR(255))"
   "ALTER TABLE `probes` ADD CONSTRAINT IF NOT EXISTS `eid_name` UNIQUE(`eid`, `name`)"
   "CREATE INDEX IF NOT EXISTS probe_name ON probes (eid, name)"])

(def scores-table
  [(format "CREATE TABLE IF NOT EXISTS `scores` (
           `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
           `expScores` VARBINARY(%d) NOT NULL)" score-size)])

(def join-table
  ["CREATE TABLE IF NOT EXISTS `joins` (
   `pid` INT,
   `i` INT,
   `sid` INT)"
   "CREATE INDEX IF NOT EXISTS index_pid ON joins (`pid`, `i`)"])

(def cohorts-table
  ["CREATE TABLE IF NOT EXISTS `cohorts` (
   `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
   `name` VARCHAR(2000) NOT NULL UNIQUE)"
   "CREATE INDEX IF NOT EXISTS index_name ON cohorts (`name`)"])

; XXX What should max file name length be?
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
   `url` varchar(255) DEFAULT NULL,
   `description` longtext,
   `notes` longtext,
   `wrangling_procedure` longtext)"])

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
    :gain
    :url
    :description
    :notes
    :wrangling_procedure})

(def ^:private experiments-defaults
  (into {} (map #(vector % nil) experiments-columns)))

; XXX should abstract this to set up the many-to-many
; on the defentity. Can we run many-to-many on the existing
; sources entity?? Or maybe do defentity sources *after* all
; the other tables exist?
(def experiment-sources-table
  ["CREATE TABLE IF NOT EXISTS `experiment_sources` (
   `experiments_id` INT NOT NULL,
   FOREIGN KEY (experiments_id) REFERENCES `experiments` (`id`),
   `sources_id` INT NOT NULL,
   FOREIGN KEY (sources_id) REFERENCES `sources` (`id`))"
   "CREATE INDEX IF NOT EXISTS experiment ON `experiment_sources` (`experiments_id`)"
   "CREATE INDEX IF NOT EXISTS source ON `experiment_sources` (`sources_id`)"])

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

(declare probemap_probes probemap_positions probemap_genes)
(defentity probemap_sources)

(defentity probemaps
  (has-many probemap_probes))

(defentity probemap_probes
  (belongs-to probemaps)
  (has-many probemap_positions)
  (has-many probemap_genes))

(defentity probemap_positions
  (belongs-to probemap_probes))

(defentity probemap_genes
  (belongs-to probemap_probes))


; XXX FIX NAME
; XXX Include original json
; XXX What should max file name length be?
(def probemaps-table
  ["CREATE TABLE IF NOT EXISTS `probemaps` (
   `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
   `name` VARCHAR(1000),
   `assembly` VARCHAR(255),
   `version` VARCHAR(255))"])

(def probemap-sources-table
  ["CREATE TABLE IF NOT EXISTS `probemap_sources` (
   `probemaps_id` INT NOT NULL,
   FOREIGN KEY (probemaps_id) REFERENCES `probemaps` (`id`),
   `sources_id` INT NOT NULL,
   FOREIGN KEY (sources_id) REFERENCES `sources` (`id`))"
   "CREATE INDEX IF NOT EXISTS probemap ON `probemap_sources` (`probemaps_id`)"
   "CREATE INDEX IF NOT EXISTS source ON `probemap_sources` (`sources_id`)"])

(def ^:private probemaps-columns
  #{:name
    :assembly
    :version})

(def ^:private probemaps-defaults
  (into {} (map #(vector % nil) probemaps-columns)))

(declare probemaps)
(defentity probemap_sources)

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
   FOREIGN KEY (probemaps_id) REFERENCES `probemaps` (`id`) ON DELETE CASCADE,
   `probe` VARCHAR(1000) NOT NULL)"])

; XXX CASCADE might perform badly, since h2 keeps the entire delete transaction
; in memory. Might need to do this incrementally in application code.
(def probemap-positions-table
  ["CREATE TABLE IF NOT EXISTS `probemap_positions` (
   `probemaps_id` INT NOT NULL,
   FOREIGN KEY (`probemaps_id`) REFERENCES `probemaps` (`id`) ON DELETE CASCADE,
   `probemap_probes_id` INT NOT NULL,
   FOREIGN KEY (`probemap_probes_id`) REFERENCES `probemap_probes` (`id`) ON DELETE CASCADE,
   `bin` INT,
   `chrom` VARCHAR(255) NOT NULL,
   `chromStart` INT NOT NULL,
   `chromEnd` INT NOT NULL,
   `strand` CHAR(1))"
   "CREATE INDEX IF NOT EXISTS chrom_bin ON probemap_positions
   (`probemaps_id`, `chrom`, `bin`)"])

(def probemap-genes-table
  ["CREATE TABLE IF NOT EXISTS `probemap_genes` (
   `probemaps_id` INT NOT NULL,
   FOREIGN KEY (`probemaps_id`) REFERENCES `probemaps` (`id`) ON DELETE CASCADE,
   `probemap_probes_id` INT NOT NULL,
   FOREIGN KEY (`probemap_probes_id`) REFERENCES `probemap_probes` (`id`) ON DELETE CASCADE,
   `gene` VARCHAR(255))"
   "CREATE INDEX IF NOT EXISTS gene ON probemap_genes
   (`probemaps_id`, `gene`)"])

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

(defn snoop [x]
  (println x)
  x)

(defn- normalize-meta [m-ent metadata]
  (select-keys
    (merge (:defaults m-ent) (clojure.walk/keywordize-keys metadata))
    (:columns m-ent)))

; Update meta entity record.
(defn- merge-m-ent [m-ent {ename "name" :as metadata}]
  (let [normmeta (normalize-meta m-ent metadata)
        table (:table m-ent)
        [{id :ID}] (select table (fields :id) (where {:name ename}))]
    (if id
      (do
        (update table (set-fields normmeta) (where {:id id}))
        id)
      (insert table (values normmeta)))))

; Insert probe & return id
(defn- insert-probe [exp name]
  (let [pid (insert probes (values {:eid exp :name name}))]
    pid))

(defn- insert-scores [slist]
  (let [sid (insert scores (values {:expScores slist}))]
    sid))

(defn- insert-join [pid i sid]
  (insert joins (values {:pid pid :i i :sid sid})))

;
; Binary buffer manipulation.
;

(def codec-length (memoize (fn [len]
                             (binary/repeated :float-le :length len))))

(defn- codec [blob]
  (codec-length (/ (count blob) float-size)))

(defn- score-decode [blob]
  (binary/decode (codec blob) (io/input-stream blob)))

(defn- score-encode [slist]
  (let [baos (java.io.ByteArrayOutputStream.)]
    (binary/encode (codec-length (count slist)) baos slist)
    (.toByteArray baos)))

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
;
;

(defn- insert-scores-block [block]
  (insert-scores (get-array block)))

(defn- insert-unique-scores-fn []
  (memoize insert-scores-block))

(defn- load-probe [insert-scores-fn exp prow]
  (let [pid (insert-probe exp (:probe (meta prow)))
        blocks (partition-all bin-size prow)]
    (doseq [[block i] (map vector blocks (range))]
      (let [sid (insert-scores-fn (ahashable (score-encode block)))]
        (insert-join pid i sid)))))

; insert matrix, updating scores, probes, and joins tables
(defn- load-exp-matrix [exp matrix]
  (let [loadp (partial load-probe (insert-unique-scores-fn) exp)]
    (dorun (map loadp matrix))))

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

(defn- insert-exp-sample [eid sample i]
  (insert exp_samples (values {:experiments_id eid :name sample :i i})))

(defn- load-exp-samples [exp samples]
  (dorun (map #(apply (partial insert-exp-sample exp) %)
              (map vector samples (range)))))

(let [fmtr (formatter "yyyy-MM-dd hh:mm:ss")]
  (defn- format-timestamp [timestamp]
    (unparse fmtr timestamp)))

(defn- fmt-time [file-meta]
  (assoc file-meta :time (format-timestamp (:time file-meta))))

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

; kdb/transaction will create a closure of our parameters,
; so we can't pass in the matrix seq w/o blowing the heap. We
; pass in a function to return the seq, instead.

(defn load-exp [files metadata matrix-fn]
  (kdb/transaction
    (let [matrix (matrix-fn)
          exp (merge-m-ent experiments-meta metadata)]
      (clear-by-exp exp)
      (load-related-sources
        experiment_sources :experiments_id exp (map fmt-time files))
      (load-exp-samples exp (:samples (meta matrix)))
      (load-exp-matrix exp matrix))))

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
(defn load-probemap [files metadata probes-fn]
  (kdb/transaction
    (let [probes (probes-fn)
          pmid (merge-m-ent probemaps-meta metadata)
          add-probe (partial add-probemap-probe pmid)]
      (load-related-sources
        probemap_sources :probemaps_id pmid (map fmt-time files))
      (dorun (map add-probe probes)))))

(defn create-db [file]
  (kdb/create-db  {:classname "org.h2.Driver"
                   :subprotocol "h2"
                   :subname file}))

(defmacro with-db [db & body]
  `(kdb/with-db ~db ~@body))

(kconf/set-delimiters "`") ; for h2

; execute a sequence of sql statements
(defn- exec-statements [stmts]
  (dorun (map (partial exec-raw) stmts)))

; TODO look up N
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
  (where q (in :i bins)))

(defn for-experiment [q exp]
  (where q {:probes.eid exp}))

(defn for-experiment-named [q exp]
  (for-experiment q (subselect experiments (fields "id") (where {:file exp}))))

(defn exp-by-name [exp]
  (let [[{id :ID}]
        (select experiments (fields "id") (where {:file exp}))]
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

; XXX Need to fill in NAN for unknown samples.
(defn genomic-read-req [req]
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
        (build-score-arrays bfns (col-arrays columns (count samples))))))

; doall is required so the seq is evaluated in db context.
; Otherwise lazy map may not be evaluted until the context is lost.
(defn genomic-source [reqs]
  (doall (map #(update-in % ['data] merge (genomic-read-req %)) reqs)))

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
    (exec-statements probemaps-table)
    (exec-statements probemap-sources-table)
    (exec-statements probemap-probes-table)
    (exec-statements probemap-positions-table)
    (exec-statements probemap-genes-table)))

; XXX monkey-patch korma to work around h2 bug.
; h2 will fail to select an index if joins are grouped, e.g.
; FROM (a JOIN b) JOIN c
; We work around it by replacing the function that adds the
; grouping.
(in-ns 'korma.sql.utils)
(defn left-assoc [v]
  (clojure.string/join " " v))
(in-ns 'cavm.h2)
