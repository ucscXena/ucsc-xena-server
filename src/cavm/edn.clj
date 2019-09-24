(ns cavm.edn
  (:require [cavm.pfc :as pfc])
  (:import [cavm HTFC]))

;
; Assume that jdbc blobs are htfc & allow printing.
;
; Normally we wouldn't extend a 3rd-party class like this, but
; since we aren't delivering a library it doesn't really matter.
; The alternative is to find a way to automatically wrap the blobs
; as they come from h2, e.g. with a tree walk, or evaluating the
; sql on the way in to determine which return values will be
; htfc.

(defmethod clojure.core/print-method HTFC [this writer]
  (clojure.core/print-method (seq this) writer))

