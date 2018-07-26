(ns cavm.json
  (:import (java.io PrintWriter))
  (:require [clojure.data.json :as json]))

;
; Overrides for json.
;
; We really only need this for views, but the dispatch
; is via protocol, and protocol handlers are global. So,
; we need to call this from the loader path, too, to avoid
; a null pointer on *formatter*.

(def ^:dynamic *formatter* nil)

(defn write-array
  "Write a core.matrix array to json"
  [arr out]
  (json/-write (seq arr) out)) ; XXX is the seq here expensive? Using vec fails.

(defn- write-floating-point [x ^PrintWriter out]
  (.print out
          (if (Double/isNaN x)
            "\"NaN\""
            (.format ^java.text.NumberFormat *formatter* x))))

(extend mikera.arrayz.INDArray json/JSONWriter {:-write write-array})
(extend java.lang.Float json/JSONWriter {:-write write-floating-point})
(extend java.lang.Double json/JSONWriter {:-write write-floating-point})

(defn- write-floating-point [x ^PrintWriter out]
  (.print out
          (if (Double/isNaN x)
            "\"NaN\""
            (.format ^java.text.NumberFormat *formatter* x))))

(defn write-str [data & args]
  (let [formatter (java.text.NumberFormat/getInstance java.util.Locale/US)] ; json must be US locale, regardless of system setting
    (.setMaximumFractionDigits formatter 6)
    (.setGroupingUsed formatter false)
    (binding [*formatter* formatter]
      (apply json/write-str data args))))

(defn read-str [s]
  (json/read-str s))
