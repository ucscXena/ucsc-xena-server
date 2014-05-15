(ns cavm.cgdata
  (:require [cgdata.core :as cgdata])
  (:require [cavm.readers :refer [reader]])
  (:gen-class))

; normalize names for xena
; normalize path by removing root XXX where do we get root?

;
; cgdata file readers
;

;
; We normalize some cgdata metadata for xena, including
; casing and trimming leading colons.
;
; Note that dropping colons means we no longer know which attributes
; are references, so we might want to rethink this.
; XXX add "name" to metadata?

(defn- normalize-meta-key [k]
  (-> k
      (clojure.string/lower-case)
      (clojure.string/replace #"^:" "")))

(defn- normalize-meta-keys [mdata]
  (into {} (map (fn [[k v]] [(normalize-meta-key k) v]) mdata)))

(defmethod reader ::probeMap
  [filetype url]
  (cgdata/probemap-file url))

; xena cgdata adaptor should normalize references relative to doc root
; xena cgdata adaptor should normalize file name relative to doc root
; XXX Need to pass doc root to matrix-file

(defn normalized-matrix-reader [filetype url]
  (let [{:keys [metadata] :as mf} (cgdata/matrix-file url)]
    (assoc mf "metadata" (normalize-meta-keys metadata))))

(defmethod reader :cgdata.core/genomic
  [filetype url]
  (normalized-matrix-reader filetype url))

(defmethod reader :cgdata.core/clinical
  [filetype url]
  (normalized-matrix-reader filetype url))

;
; Use cgdata's tsv reader for plain tsv.
;

(defmethod reader :cgdata.core/tsv
  [filetype url]
  (normalized-matrix-reader filetype url))

