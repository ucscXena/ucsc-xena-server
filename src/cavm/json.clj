(ns cavm.json
  (:import (java.io PrintWriter))
  (:import info.adams.ryu.RyuFloat)
  (:import info.adams.ryu.RoundingMode)
  (:import [java.nio ByteBuffer])
  (:require [cavm.pfc :as pfc])
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
  (let [lx (long x)]
    (cond
      (Double/isNaN x) (.print out "\"NaN\"")
      (== x lx) (json/-write lx out)
      :else (.print out (RyuFloat/floatToString x RoundingMode/ROUND_EVEN 4)))))

(extend mikera.arrayz.INDArray json/JSONWriter {:-write write-array})
(extend java.lang.Float json/JSONWriter {:-write write-floating-point})
(extend java.lang.Double json/JSONWriter {:-write write-floating-point})

;
; Assume that jdbc blobs are htfc & allow printing.
;
; Normally we wouldn't extend a 3rd-party class like this, but
; since we aren't delivering a library it doesn't really matter.
; The alternative is to find a way to automatically wrap the blobs
; as they come from h2, e.g. with a tree walk, or evaluating the
; sql on the way in to determine which return values will be
; htfc.
(defn- write-pfc [x ^PrintWriter out]
  (json/-write (seq (pfc/to-htfc x)) out))

(extend org.h2.jdbc.JdbcBlob json/JSONWriter {:-write write-pfc})

(defn write-str [s & args]
  (apply json/write-str s args))

(defn read-str [s]
  (json/read-str s))
