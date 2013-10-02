(ns cavm.query.functions
  (:use clojure.core.matrix)
  (:use clojure.core.matrix.operators)
  (:refer-clojure :exclude  [* - + == /])
  (:gen-class))

(set-current-implementation :vectorz)

(defn meannan1d [m]
  (let [NaN Double/NaN
        [sum n] (ereduce
                  (fn [[acc cnt] x] (if (Double/isNaN x) [acc cnt] [(+ acc x) (inc cnt)]))
                  [0 0]
                  m)]
    (if (= 0 n) NaN (/ sum n))))

; XXX this handling of dim is wrong for dim > 1
; XXX do we need to fill nan values, like we do in python?
(defn meannan [m dim]
    (let [new-shape (assoc (vec (shape m)) (long dim) 1)]
      (reshape (matrix (map meannan1d (slices m (- 1 dim)))) new-shape)))

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
   'mean meannan
   'apply apply}) ; XXX is this a security hole? Can the user pass in clojure symbols?
