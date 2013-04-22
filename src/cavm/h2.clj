(ns cavm.h2
  (:require [korma.db :as kdb])
  (:require [korma.config :as kconf])
  (:require [org.clojars.smee.binary.core :as binary])
  (:require [clojure.java.io :as io])
  (:use [clj-time.format :only (formatter unparse)])
  (:use korma.core)
  (:gen-class))

(def db {:classname "org.h2.Driver"
         :subprotocol "h2"
         :subname "file:///data/TCGA/craft/h2/cavm.h2"})

(def probes-table ["CREATE TABLE IF NOT EXISTS `probes` (
                  `id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                  `exp` INT(10) UNSIGNED NOT NULL,
                  `bin` INT(11),
                  `chrom` VARCHAR(255),
                  `chromStart` INT(10) UNSIGNED,
                  `chromEnd` INT(10) UNSIGNED,
                  `name` VARCHAR(255))"
                   "CREATE INDEX IF NOT EXISTS probe_name ON probes (exp, name)"
                   "CREATE INDEX IF NOT EXISTS probe_chrom ON probes (exp, chrom, bin)"])

(def float-size 4)
(def bin-size 100)
(def score-size (* float-size bin-size))

(def scores-table [(format "CREATE TABLE IF NOT EXISTS `scores` (
                  `id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                  `expScores` VARBINARY(%d) NOT NULL)" score-size)])

(def join-table ["CREATE TABLE IF NOT EXISTS `joins` (
                 `pid` INT(10) UNSIGNED,
                 `i`   INT(10) UNSIGNED,
                 `sid` INT(10) UNSIGNED)"
                 "CREATE INDEX IF NOT EXISTS index_pid ON joins (`pid`, `i`)"])

; change this to use foreign key constraints from the probe table?
; and drop id?
(def experiment-table ["CREATE TABLE IF NOT EXISTS `experiments` (
                `id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
                `file` VARCHAR(2000) NOT NULL PRIMARY KEY,
                `time` TIMESTAMP NOT NULL,
                `hash` VARCHAR(40) NOT NULL)"])

; XXX might need index on sample?
(def samples-table ["CREATE TABLE IF NOT EXISTS `samples` (
                   `exp` INT(10) UNSIGNED NOT NULL,
                   `sample` VARCHAR(1000) NOT NULL)"
                   "CREATE INDEX IF NOT EXISTS `sample_exp` on `samples` (`exp`)"])

(declare score-decode)

(declare probes scores)

(defentity experiments
  (has-many probes))

(defentity samples
  (belongs-to experiments {:fk :exp}))

(defentity probes
  (many-to-many scores :joins  {:lfk 'pid :rfk 'sid})
  (belongs-to experiments {:fk :exp})
  (transform (fn [{scores :EXPSCORES :as v}]
               (if scores
                 (assoc v :EXPSCORES (score-decode scores))
                 v))))

(defentity scores
  (many-to-many probes :joins  {:lfk 'sid :rfk 'pid}))

(defentity joins)

(defn- probes-in-exp [exp]
  (subselect probes (fields :id) (where {:exp [= exp]})))

(defn- scores-with-probes [probes]
  (subselect joins (fields :sid) (where {:pid [in probes]})))

(defn- clear-by-exp [exp]
  (let [p (probes-in-exp exp)]
    (delete scores (where {:id [in (scores-with-probes p)]}))
    (delete joins (where {:pid [in p]}))
    (delete probes (where {:id [in p]}))))

; Delete experiment data & update experiment record.
(defn- merge-exp [file timestamp filehash]
  (exec-raw ["MERGE INTO experiments (file, time, hash) VALUES (?, ?, ?)" [file timestamp filehash]])
  (let [[{exp :ID}] (select experiments (where {:file file}))]
    (clear-by-exp exp)
    exp))

; Not sure if this is korma or h2, but this is pretty unwieldy.
(def key-id (keyword "SCOPE_IDENTITY()"))

; Insert probe & return id
; expand this for chrom info later.
(defn- insert-probe [exp name]
  (let [{pid key-id} (insert probes (values {:exp exp :name name}))]
    pid))

(defn- insert-scores [slist]
  (let [{sid key-id} (insert scores (values {:expScores slist}))]
    sid))

(defn- insert-join [pid i sid]
  (insert joins (values {:pid pid :i i :sid sid})))

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

(defn- load-probe [exp prow]
  (let [pid (insert-probe exp (:probe (meta prow)))
        blocks (partition-all bin-size prow)
        indx (range (count blocks))]
    (dorun (map (fn [block i]
           (let [sid (insert-scores (score-encode block))]
             (insert-join pid i sid))) blocks indx))))

; insert matrix, updating scres, probes, and joins tables
(defn- load-exp-matrix [exp matrix]
  (let [loadp (partial load-probe exp)]
    (dorun (map loadp matrix))))

(defn- load-exp-samples [exp sample-list]
  (dorun (map #(insert samples (values {:exp exp :sample %})) sample-list)))

(let [fmtr (formatter "yyyy-MM-dd hh:mm:ss")]
  (defn- format-timestamp [timestamp]
    (unparse fmtr timestamp)))

(defn load-exp [file timestamp filehash matrix]
  (kdb/transaction
    (let [exp (merge-exp file (format-timestamp timestamp) filehash)]
      (load-exp-samples exp (:samples (meta matrix)))
      (load-exp-matrix exp matrix))))

(kdb/defdb cavmdb db)
(kconf/set-delimiters "`") ; for h2

; execute a sequence of sql statements
(defn- exec-statements [stmts]
  (dorun (map (partial exec-raw cavmdb) stmts)))

(defn datasets []
  (select experiments))

(defn select-probes []
  (select* probes))

; The case is required because group_concat converts to hex.
(defn select-scores []
  (-> (select* probes)
      (fields (raw "CAST(GROUP_CONCAT(`expScores` ORDER BY `i` SEPARATOR '') AS BINARY) AS `expScores`"))
      (join scores)
      (group :joins.pid)))

(defn for-experiment [q exp]
  (where q {:probes.exp exp}))

(defn for-experiment-named [q exp]
  (for-experiment q (subselect experiments (fields "id") (where {:file exp}))))

; The case is required because group_concat converts to hex.
(defn with-genes [q genes]
  (fields (where q {:probes.name [in genes]}) [:probes.name :gene]))

(defn do-select [q]
  (exec q))

(defn create[]
  (kdb/transaction
    (exec-statements samples-table)
    (exec-statements experiment-table)
    (exec-statements scores-table)
    (exec-statements join-table)
    (exec-statements probes-table)))

; XXX monkey-patch korma to work around h2 bug.
; h2 will fail to select an index if joins are grouped, e.g.
; FROM (a JOIN b) JOIN c
; We work around it by replacing the function that adds the
; grouping.
(in-ns 'korma.sql.utils)
(defn left-assoc [v]
  (clojure.string/join " " v))
(in-ns 'cavm.h2)
