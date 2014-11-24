(ns
  ^{:author "Brian Craft"
    :doc "A set of core functions for use with cavm.query.expression."}
  cavm.query.functions
  (:use clojure.core.matrix)
  (:use clojure.core.matrix.operators)
  (:refer-clojure :exclude  [* - + == /])
  (:gen-class))

(set-current-implementation :vectorz)

(defn- meannan1d [m]
  (let [NaN Double/NaN
        [sum n] (ereduce
                  (fn [[acc cnt] x] (if (Double/isNaN x) [acc cnt] [(+ acc x) (inc cnt)]))
                  [0 0]
                  m)]
    (if (= 0 n) NaN (/ sum n))))

; XXX this handling of dim is wrong for dim > 1
; XXX do we need to fill nan values, like we do in python?
(defn meannan-impl [m dim]
    (let [new-shape (assoc (vec (shape m)) (long dim) 1)]
      (reshape (matrix (map meannan1d (slices m (- 1 dim)))) new-shape)))

; Methods that coerce params to core.matrix. Should be extended
; to all the math functions in 'functions', below.
(defprotocol Matrix
  (meannan [m dim]))

(extend-protocol Matrix
  mikera.matrixx.Matrix
  (meannan [m dim] (meannan-impl m dim))
  clojure.lang.Seqable
  (meannan [m dim] (meannan-impl (matrix (map double-array m)) dim)))

(def functions
  {'+ +
   '/ /
   '* emul
   '> >
   '< <
   '>= >=
   '<= <=
   '- -
   '= ==
   'map map
   'get get
   'assoc assoc
   'cons cons
   'car first
   'cdr rest
   'group-by group-by
   'mean meannan
   'count count
   'apply apply}) ; XXX is this a security hole? Can the user pass in clojure symbols?
