(ns
  ^{:author "Brian Craft"
    :doc "Utility for calculating bins for spatial ranges, useful as
         b-tree indexed columns in a database.

         This is a port of the algorithm in RangeFinder.py by Mark Diekhans,
         which is a port of binRange.{h,c} and hdb.c by by Jim Kent.

         For simplicitly, it only implements the extended (not basic) range bins.
         This means it is not directly usable by kent/src libraries."}
  cavm.binner)

(def ^:private bin-offsets
  [(+ 4096 512 64 8 1)
   (+ 512 64 8 1)
   (+ 64 8 1)
   (+ 8 1)
   0])

(def ^:private first-shift 17)
(def ^:private next-shift 3)

(defn- find-bin [start-bin end-bin offsets]
  (when (seq offsets)
    (if (== start-bin end-bin)
      (+ (first offsets) start-bin)
      (recur (bit-shift-right start-bin next-shift)
             (bit-shift-right end-bin next-shift)
             (rest offsets)))))

(defn- calc-bin-for-offsets [start end offsets]
  (let [start-bin (bit-shift-right start first-shift)
        end-bin (bit-shift-right (- end 1) first-shift)]
    (find-bin start-bin end-bin offsets)))

(defn calc-bin [start end]
  (calc-bin-for-offsets start end bin-offsets))

; XXX check that we're not off by one when matching.
(defn- overlapping-bins-for-offsets [start end offsets]
  (loop [start-bin (bit-shift-right start first-shift)
         end-bin (bit-shift-right (- end 1) first-shift)
         [offset & offsets] offsets
         bins []]
    (if offset
      (recur (bit-shift-right start-bin next-shift)
             (bit-shift-right end-bin next-shift)
             offsets
             (conj bins [(+ start-bin offset) (+ end-bin offset)]))
      bins)))

(defn overlapping-bins [start end]
  (overlapping-bins-for-offsets start end bin-offsets))
