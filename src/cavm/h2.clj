(ns cavm.h2
  (:require [korma.db :as kdb])
  (:require [korma.config :as kconf])
  (:require [clojure.java.jdbc.deprecated :as jdbc])
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
  (:require [clojure.core.cache :as cache])
  (:require [cavm.h2-unpack-rows :as unpack])
  (:require [cavm.statement :refer [sql-stmt cached-statement]])
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

(declare dataset_source feature field scores dataset)

(defentity source)

(defentity dataset
  (many-to-many source :dataset_source)
  (has-many field))

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
   `scores_id` INT,
   UNIQUE (`field_id`, `i`))"])

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

; XXX CASCADE might perform badly. Might need to do this incrementally in application code.
(def field-position-table
  ["CREATE TABLE IF NOT EXISTS `field_position` (
   `field_id` INT NOT NULL,
   `row` INT NOT NULL,
   `bin` INT,
   `chrom` VARCHAR(255) NOT NULL,
   `chromStart` INT NOT NULL,
   `chromEnd` INT NOT NULL,
   `strand` CHAR(1),
   FOREIGN KEY (`field_id`) REFERENCES `field` (`id`) ON DELETE CASCADE)"
   "CREATE INDEX IF NOT EXISTS chrom_bin ON field_position (`field_id`, `chrom`, `bin`)"]) ; XXX index on row?

(def field-gene-table
  ["CREATE TABLE IF NOT EXISTS `field_gene` (
   `field_id` INT NOT NULL,
   `row` INT NOT NULL,
   `gene` VARCHAR(255),
    FOREIGN KEY (`field_id`) REFERENCES `field` (`id`) ON DELETE CASCADE)"
   "CREATE INDEX IF NOT EXISTS field_gene ON `field_gene` (`field_id`, `gene`)"]) ; XXX index on row?

;
; feature tables
;

(def feature-table
  ["CREATE SEQUENCE IF NOT EXISTS FEATURE_IDS CACHE 2000"
   "CREATE TABLE IF NOT EXISTS `feature` (
   `id` INT NOT NULL DEFAULT NEXT VALUE FOR FEATURE_IDS PRIMARY KEY,
   `field_id` int(11) NOT NULL,
   `shortTitle` varchar(255),
   `longTitle` varchar(255),
   `priority` double DEFAULT NULL,
   `valueType` varchar(255) NOT NULL,
   `visibility` varchar(255),
   FOREIGN KEY (`field_id`) REFERENCES `field` (`id`) ON DELETE CASCADE)"])

;
; blob unpacking
;

(def unpack-aliases
  ["CREATE ALIAS IF NOT EXISTS unpack FOR \"unpack_rows.unpack\""
   "CREATE ALIAS IF NOT EXISTS unpackCode FOR \"unpack_rows.unpackCode\""
   "CREATE ALIAS IF NOT EXISTS unpackValue FOR \"unpack_rows.unpackValue\""])

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

(defn load-probe-meta
  [feature-seq field-id {:keys [order state] :as feat}]
  (let [feature-id (feature-seq)
        fmeta (merge (normalize-meta feature-meta feat)
                     {:field_id field-id :id feature-id})]
    (cons [:insert-feature (assoc fmeta :id feature-id)]
          (for [a-code state]
            [:insert-code {:feature_id feature-id
                           :ordering (order a-code)
                           :value a-code}]))))
;
;
;

(defn- fields-in-exp [exp]
  (subselect field (fields :id) (where {:dataset_id exp})))

(defn- scores-with-fields [fs]
  (subselect field_score (fields :scores_id) (where {:field_id [in fs]})))

(defn- clear-by-exp [exp]
  (let [f (fields-in-exp exp)]
    (delete scores (where {:id [in (scores-with-fields f)]}))
    (delete field_score (where {:field_id [in f]}))
    (delete field (where {:id [in f]}))))

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

(defn ashuffle-float [mappings ^floats out ^floats in]
   (doseq [[i-in i-out] mappings]
     (aset out i-out (aget in i-in)))
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

(defn- encode-row [encode row]
  (mapv encode (partition-all bin-size row)))

(defn- encode-fields [encoder matrix-fn]
  (chunked-pmap #(assoc % :data (encode-row encoder (:scores %)))
                (:fields (matrix-fn))))

;
; sequence utilities


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

;
; Main loader routines. Here we build a sequence of vectors describing rows to be
; inserted, e.g.
; [:insert-score {:id 1 :scores #<byte[] [B@12345>}]
;

(defmulti load-field
  (fn [dataset-id field-id column score-seq]
    (-> column :valueType)))

(defmethod load-field :default [dataset-id field-id {:keys [field data]} scores-seq]
  (conj (mapcat (fn [[block i]]
                  (let [scores-id (scores-seq)]
                    [[:insert-score {:id scores-id :scores block}]
                     [:insert-field-score {:field_id field-id
                                           :i i
                                           :scores_id scores-id}]]))
                (mapv vector data (range)))
        [:insert-field {:id field-id :dataset_id dataset-id :name field}]))

(defmethod load-field "position" [dataset-id field-id column scores-seq]
  (conj (for [[position row] (mapv vector (:rows column) (range))]
          [:insert-position (assoc position
                                   :field_id field-id
                                   :row row
                                   :bin (calc-bin
                                          (:chromStart position)
                                          (:chromEnd position)))])
        [:insert-field {:id field-id :dataset_id dataset-id :name (:field column)}]))

(defmethod load-field "genes" [dataset-id field-id {:keys [field rows]} scores-seq]
  (conj (mapcat (fn [[genes row]]
                  (for [gene genes]
                    [:insert-gene {:field_id field-id :row row :gene gene}]))
                (mapv vector rows (range)))
        [:insert-field {:id field-id :dataset_id dataset-id :name field}]))

(defn- inferred-type
  "Replace the given type with the inferred type"
  [fmeta row]
  (assoc fmeta "valueType" (:valueType row)))

(defn- load-field-feature [feature-seq field-seq scores-seq dataset-id field]
  (let [field-id (field-seq)]
    (concat (load-field dataset-id field-id field scores-seq)
            (when-let [feature (:feature field)]
              (load-probe-meta feature-seq field-id (inferred-type feature field))))))
;
;
;
; Table writer that updates tables as rows are read.
;
;

(defn quote-ent [x]
  (str \` (name x) \`))

(defn insert-stmt ^cavm.statement.PStatement [table fields]
   (let [field-str (clojure.string/join ", " (map quote-ent fields))
         val-str (clojure.string/join ", " (repeat (count fields), "?"))
         stmt-str (format "INSERT INTO %s(%s) VALUES (%s)"
                      (quote-ent table) field-str val-str)]
     (sql-stmt (jdbc/find-connection) stmt-str fields)))

(def batch-size 1000)

(defn- table-writer-default [dir dataset-id matrix-fn]
  (with-open [code-stmt (insert-stmt :code [:value :id :ordering :feature_id])
              position-stmt (insert-stmt :field_position
                                         [:field_id :row :bin :chrom :chromStart
                                          :chromEnd :strand])
              gene-stmt (insert-stmt :field_gene [:field_id :row :gene])
              feature-stmt (insert-stmt :feature [:id :field_id :shortTitle
                                                  :longTitle :priority
                                                  :valueType :visibility])
              score-stmt (insert-stmt :scores [:id :scores])
              field-stmt (insert-stmt :field [:id :dataset_id :name])
              field-score-stmt (insert-stmt :field_score [:field_id :scores_id :i])]
    (let [feature-seq (-> "FEATURE_IDS" sequence-seq seq-iterator)
          field-seq (-> "FIELD_IDS" sequence-seq seq-iterator)
          scores-seq (-> "SCORES_IDS" sequence-seq seq-iterator)
          ; XXX try memoization again?
          ;        score-id-fn (memoize-key (comp ahashable first) scores-seq)

          writer {:insert-field field-stmt
                  :insert-score score-stmt
                  :insert-field-score field-score-stmt
                  :insert-position position-stmt
                  :insert-feature feature-stmt
                  :insert-code code-stmt
                  :insert-gene gene-stmt}
          fields (encode-fields score-encode matrix-fn)
          inserts (mapcat #(load-field-feature
                             feature-seq
                             field-seq
                             scores-seq
                             dataset-id
                             %)
                          fields)]

      (doseq [insert-batch (partition-all batch-size inserts)]
        (jdbc/transaction
          (doseq [[insert-type values] insert-batch]
            ((writer insert-type) values)))))))
;
;
; Table writer that updates one table at a time by writing to temporary files.
;
; Currently not functioning. Needs update for new schema.

(defn- insert-field-out [^java.io.BufferedWriter out seqfn dataset-id name]
  (let [field-id (seqfn)]
    (.write out (str field-id "\t" dataset-id "\t" name "\n"))
    field-id))

(defn- insert-scores-out [^java.io.BufferedWriter out seqfn slist]
  (let [score-id (seqfn)]
    (.write out (str score-id "\t" (:hex slist) "\n"))
    score-id))

(defn- insert-field-score-out [^java.io.BufferedWriter out field-id i score-id]
  (.write out (str field-id "\t" i "\t" score-id "\n")))

(defn- read-from-tsv [table file & cols]
  (-> "INSERT INTO %s DIRECT SORTED SELECT * FROM CSVREAD('%s', '%s', 'fieldSeparator=\t')"
      (format table file (clojure.string/join "\t" cols))
      (exec-raw)))

(defn- tsv-encoder [scores]
  (let [s (score-encode scores)]
    {:scores s :hex (bytes->hex s)}))

(defn- table-writer-csv [dir dataset-id matrix-fn]
  (let [field-seq (-> "FIELD_IDS" sequence-seq seq-iterator)
        scores-seq (-> "SCORES_IDS" sequence-seq seq-iterator)
        ffname (fs/file dir "field.tmp") ; XXX add pid to these, or otherwise make them unique
        sfname (fs/file dir "scores.tmp")
        fsfname (fs/file dir "field_score.tmp")

        fields (encode-fields tsv-encoder matrix-fn)]
    (try
      (let [field-meta (with-open
                         [field-file (io/writer ffname)
                          scores-file (io/writer sfname)
                          field-score-file (io/writer fsfname)]
                         (let [writer
                               {:insert-field (partial
                                                insert-field-out
                                                field-file field-seq)
                                :insert-score (memoize-key
                                                #(ahashable (:scores (first %)))
                                                #(insert-scores-out scores-file scores-seq %))
                                :insert-field-score (partial
                                                      insert-field-score-out
                                                      field-score-file)}]
                           (reduce #(load-field-feature writer dataset-id %1 %2) '() fields)))]
        (read-from-tsv "field" ffname "ID" "DATASET_ID" "NAME")
        (read-from-tsv "scores" sfname "ID" "SCORES")
        (read-from-tsv "field_score" fsfname "FIELD_ID" "I" "SCORES_ID")
        (kdb/transaction
          (load-probe-meta field-meta)))
      (finally
        (fs/delete ffname)
        (fs/delete sfname)
        (fs/delete fsfname)))))

(def table-writer #'table-writer-default)

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

(defn load-dataset
  "Load matrix file and metadata. Skips the data load if file hashes are unchanged,
  and 'force' is false. Metadata is always updated."
  ([mname files metadata matrix-fn features]
   (load-dataset mname files metadata matrix-fn features false))

  ([mname files metadata matrix-fn features force]
   (profile
     :trace
     :dataset-load
     (let [dataset-id (merge-m-ent dataset-meta mname metadata)
             files (map fmt-time files)]
         (when (or force
                   (not (= (set files) (set (related-sources dataset dataset-id)))))
           (kdb/transaction ; XXX add flag for when dataset is fully loaded. Maybe just updating timestamp?
             (p :dataset-clear
                (clear-by-exp dataset-id))
             (p :dataset-sources
                (load-related-sources
                  dataset_source :dataset_id dataset-id files)))
           (p :dataset-table
              (jdbc/with-connection (korma.db/get-connection @korma.db/_default)
                (table-writer *tmp-dir* dataset-id matrix-fn))))))))

; XXX factor out common parts with merge, above?
(defn del-exp [file]
  (kdb/transaction
    (let [[{exp :ID}] (select dataset (where {:file file}))]
      (clear-by-exp exp)
      (delete dataset (where {:id exp})))))

; XXX probemap meta??? What do we do with it now?

; XXX Horrible work-around to call .setConnectionCustomizerClassName. Need
; to drop korma.
(defn kcreate-db
  [spec]
  {:pool (if (:make-pool? spec)
           (delay (let [cp (kdb/connection-pool spec)]
                    (.setConnectionCustomizerClassName (:datasource cp)
                                                       "conn_customizer")
                    cp))
           spec)
   :options (kconf/extract-options spec)})

(defn create-db [file & [{:keys [classname subprotocol delimiters make-pool?]
                          :or {classname "org.h2.Driver"
                               subprotocol "h2"
                               delimiters "`"
                               make-pool? true}}]]
  (kcreate-db {:classname classname
               :subprotocol subprotocol
               :subname file
               :delimiters delimiters
               :make-pool? make-pool?}))

(defmacro with-db [db & body]
  `(kdb/with-db ~db ~@body))

; execute a sequence of sql statements
(defn- exec-statements [stmts]
  (doseq [s stmts]
    (exec-raw s)))

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
    :left-join [:scores [:= :scores_id :scores.id]]})

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

; merge bin number and bin offset for a row
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
; bin is the bin for the given set of rows
; rows is ({:I 12 :bin 1 :off 1}, ... ), where all :bin are the same
(defn- pick-rows-fn [[bin rows]]
  [bin (partial ashuffle-float
                (map bin-mapping rows))])

; Take an ordered list of requested rows, and generate fns to copy
; values from a score bin to an output array. Returns one function
; for each score bin in the request.
;
; rows ({:I 12 :bin 1 :off 1}, ... )
(defn- pick-rows-fns [rows]
  (let [by-bin (group-by :bin rows)]
    (apply hash-map (mapcat pick-rows-fn by-bin))))

; Take map of bin copy fns, list of scores rows, and a map of output arrays,
; copying the scores to the output arrays via the bin copy fns.
(defn- build-score-arrays [rows bfns out]
  (doseq [{i :I scores :SCORES field :NAME} rows]
    ((bfns i) (out field) scores))
  out)

(defn- col-arrays [columns n]
  (zipmap columns (repeatedly (partial float-array n Double/NaN))))

; Doing an inner join on a table literal, instead of a big IN clause, so it will hit
; the index.

; XXX performance of the :in clause?
; XXX change "gene" to "field"
(def dataset-fields-query
  "SELECT  name, i, scores
   FROM (SELECT *
         FROM (SELECT  `field`.`name`, `field`.`id`
               FROM `field`
               INNER JOIN TABLE(name varchar=?) T ON T.`name`=`field`.`name`
               WHERE (`field`.`dataset_id` = ?)) P
         LEFT JOIN `field_score` ON P.id = `field_score`.`field_id`
         WHERE `field_score`.`i` IN (%s))
   LEFT JOIN `scores` ON `scores_id` = `scores`.`id`")

(defn select-scores-full [dataset-id columns bins]
  (let [q (format dataset-fields-query
                  (clojure.string/join ","
                                       (repeat (count bins) "?")))
        c (to-array (map str columns))
        i (to-array bins)]
    (exec-raw [q (concat [c dataset-id] i)] :results)))

; Returns rows from (dataset-id column-name) matching
; values. Returns a hashmap for each matching  row.
; { :i     The implicit index in the column as stored on disk.
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
        order (mapv (comp float-nil samp->code) samples)     ; codes in request order
        rows (rows-matching dataset-id "sampleID" order)
        rows-to-copy (->> rows
                          (filter :value)
                          (mapv merge-bin-off))

        bins (mapv :bin rows-to-copy)            ; list of bins to pull.
        bfns (pick-rows-fns rows-to-copy)]   ; make fns that map from input bin to
                                                ; output buffer, one fn per input bin.

    (-> (select-scores-full dataset-id columns (distinct bins))
        (#(mapv cvt-scores %))
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
    (exec-statements scores-table)
    (exec-statements field-score-table)
    (exec-statements field-table)
    (exec-statements feature-table)
    (exec-statements code-table)
    (exec-statements field-position-table)
    (exec-statements field-gene-table)
    (exec-statements unpack-aliases)))

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
      (load-dataset mname files metadata data-fn features always)))
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

;
; Support for pulling a row from a blob column. Implements
; an LRU cache of retrieved blobs.
;

(def field-bin-query
  (cached-statement
    "SELECT `scores` FROM `field_score`
     JOIN `scores` ON `scores_id` = `scores`.`id`
     WHERE `field_id` = ? AND `i` = ?"
    true))

(def feature-value-query
  (cached-statement
    "SELECT `value` FROM `feature`
     JOIN `code` ON `feature`.`id` = `feature_id`
     WHERE `field_id` = ? AND `ordering` = ?"
    true))

(defn fetch-bin [conn field-id bin]
  (when-let [scores (-> (field-bin-query conn field-id bin)
                        first
                        :scores)]
    (bytes-to-floats scores)))

(def cache-size (int (/ (* 128 1024) bin-size))) ; 128k bin cache

(def bin-cache-state (atom (cache/lru-cache-factory {} :threshold cache-size)))

(defn update-bin-cache [c conn args]
  (if (cache/has? c args)
    (cache/hit c args)
    (cache/miss c args (apply fetch-bin conn args))))

(defn bin-cache [conn & args]
  (cache/lookup (swap! bin-cache-state update-bin-cache conn args) args))

(defn lookup-row [conn field-id row]
  (let [^floats bin (bin-cache conn field-id (quot row bin-size))
        i (rem row bin-size)]
    (when bin (aget bin i))))

(defn lookup-value [conn field-id row]
  (let [ordering (lookup-row conn field-id row)]
    (when ordering
      (when-let [value (feature-value-query conn field-id (int ordering))]
        (-> value first :value)))))


(unpack/set-lookup-row! lookup-row)
(unpack/set-lookup-value! lookup-value)
