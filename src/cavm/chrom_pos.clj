(ns
  ^{:author "Brian Craft"
    :doc "Specialized collections for chrom positions, with low memory footprint.
         Measured to be about 1/7 the size of a vector of hash-maps, and about
         1/5 the size of a vector of records.

         Chrom names are interned. The interned collection never shrinks."}
  cavm.chrom-pos)

(defrecord Chrom [^String chrom ^long chromStart ^long chromEnd ^char strand]
  clojure.lang.IFn
  (invoke [self k] (k self)))
(def chrom-pos ->Chrom)

(declare ->ChromPosVec chrom-pos-vec)
(deftype ChromPosVec [chroms starts ends strands chrom-cache]
  clojure.lang.IPersistentVector
  (seq [self] (when (> (count chroms) 0)
                (map ->Chrom chroms starts ends strands)))
  (nth [self i] (apply ->Chrom (map #(nth % i) [chroms starts ends strands])))
  (nth [self i notfound] (or (.valAt self i) notfound))
  (empty [self] (chrom-pos-vec))
  clojure.lang.ILookup
  (valAt [self i] (when (and (>= i 0) (< i (count starts)))
                    (.nth self i)))
  (valAt [self i notfound] (.nth self i notfound))
  clojure.lang.ISeq
  (first [self] (.valAt self 0))
  (next [self] (when (> (count chroms) 1)
                 (->ChromPosVec (next chroms) (next starts)
                                (next ends) (next strands) chrom-cache)))
  (more [self] (if (> (count chroms) 1)
                 (->ChromPosVec (rest chroms) (rest starts)
                                (rest ends) (rest strands) chrom-cache)
                 '()))
  (cons [self {:keys [chrom chromStart chromEnd strand]}]
    (->ChromPosVec (conj chroms (chrom-cache chrom)) (conj starts chromStart)
                   (conj ends chromEnd) (conj strands (or strand \0)) chrom-cache))
  (count [self] (count starts))
  clojure.lang.IFn
  (invoke [self i] (.nth self i))
  Object
  (toString [self] (str (into [] self))))

(defn strcpy [s] (String. ^String s))

(defn chrom-pos-vec []
  (->ChromPosVec [] (vector-of :long) (vector-of :long) (vector-of :char)
                 (memoize strcpy)))
