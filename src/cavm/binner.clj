(ns cavm.binner)

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


; Alternative implementation using lazy seqs instead of recur.
; match is a lazy seq that is nil until start-bin == end-bin.
;(defn- calc-bin-for-offsets [start end offsets]
;  (let [start-bin (bit-shift-right start first-shift)
;        end-bin (bit-shift-right (- end 1) first-shift)
;        start-bins (iterate #(bit-shift-right % next-shift) start-bin)
;        end-bins (iterate #(bit-shift-right % next-shift) end-bin)
;        match (map #(when (= %1 %2) (+ %1 %3)) start-bins end-bins offsets)]
;    (first (filter identity match)))) ; note that filter is also lazy

(defn calc-bin [start end]
  (calc-bin-for-offsets start end bin-offsets))
