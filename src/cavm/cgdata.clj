(ns cavm.cgdata
  (:require [cgdata.core :as cgdata])
  (:require [cavm.readers :refer [reader]])
  (:gen-class))

;
; cgdata file readers
;

;
; We normalize some cgdata metadata for xena, including
; casing and trimming leading colons.
;
; Note that dropping colons means we no longer know which attributes
; are references, so we might want to rethink this.

(defn- normalize-meta-key [k]
  (-> k
      (clojure.string/lower-case)
      (clojure.string/replace #"^:" "")))

(defn- normalize-meta-keys [mdata]
  (into {} (map (fn [[k v]] [(normalize-meta-key k) v]) mdata)))

(defmethod reader :cgdata.core/probemap
  [filetype docroot url]
  (-> (cgdata/probemap-file url :docroot docroot)
      (assoc :datatype :probemap)))

(defn normalized-matrix-reader [filetype docroot url]
  (-> (cgdata/matrix-file url :docroot docroot)
      (assoc :datatype :matrix)
      (update-in [:metadata] normalize-meta-keys)))

(defmethod reader :cgdata.core/genomic
  [filetype docroot url]
  (normalized-matrix-reader filetype docroot url))

(defmethod reader :cgdata.core/clinical
  [filetype docroot url]
  (normalized-matrix-reader filetype docroot url))

;
; Use cgdata's tsv reader for plain tsv.
;

(defmethod reader :cgdata.core/tsv
  [filetype docroot url]
  (normalized-matrix-reader filetype docroot url))

