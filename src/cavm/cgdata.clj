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

;
; Canonical forms for xena metadata keys
(def metakeys
  ["probeMap"
   "shortTitle"
   "longTitle"
   "groupTitle"
   "platform"
   "security"
   "dataSubType"
   "text"
   "gain"])

; Map of lower-case forms to canonical forms of xena metadata.
; We are permissive in what we accept from cgdata, because humans
; are editing it.
(def keymap (into {} (map vector (map clojure.string/lower-case metakeys) metakeys)))

(defn- normalize-meta-key [k]
  (-> k
      (clojure.string/lower-case)
      (clojure.string/replace #"^:" "")
      (keymap)))

(defn- normalize-meta-keys [mdata]
  (into {} (map (fn [[k v]] [(normalize-meta-key k) v]) mdata)))

(defn- replace-references
  "Overwrite file references in the metadata with the resolved paths
  returned by the cgdata reader."
  [matrix]
  (update-in matrix [:metadata] #(merge % (:refs matrix))))

(defmethod reader :cgdata.core/probemap
  [filetype docroot url]
  (-> (cgdata/probemap-file url :docroot docroot)
      (assoc :datatype :probemap)))

(defn normalized-matrix-reader [filetype docroot url]
  (-> (cgdata/matrix-file url :docroot docroot)
      (assoc :datatype :matrix)
      (replace-references)
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

