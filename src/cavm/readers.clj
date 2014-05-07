(ns cavm.readers)

;
; File format readers.
;
; Readers are split into two parts: a file type detector, and a reader multimethod.
; This split is maintained because there is a not a one-to-one relationship between
; dectecting a file type and having a reader, e.g. we identify all cgdata file types
; with a single method that reads file type from the json metadata.
;
; XXX Note that this strategy opens each file repeatedly to determine its type.
;

(defmulti reader
  (fn [filetype url] filetype))

(defn detector [& detectors]
  (fn [file]
    (let [file-type (some #(% file) detectors)]
      {:file-type file-type
       :reader (delay (reader file-type file))})))
