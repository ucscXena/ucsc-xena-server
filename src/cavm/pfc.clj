(ns
  ^{:author "Brian Craft"
    :doc "front coding"}
  cavm.pfc
  (:import [java.util.concurrent ArrayBlockingQueue])
  (:require [clojure.data.json :as json])
  (:require [cavm.huffman :as huffman]))

;
; vbyte
;

; XXX use unchecked math?
(defn vbyte0 [i] (byte-array [(bit-or 0x80 i)]))
(defn vbyte1 [i] (byte-array
                     [(bit-shift-right i 7)
                      (bit-or 0x80 (bit-and i 0x7f))]))
(defn vbyte2 [i] (byte-array
                     [(bit-and (bit-shift-right i 14) 0x7f)
                      (bit-and (bit-shift-right i 7) 0x7f)
                      (bit-or 0x80 (bit-and i 0x7f))]))
(defn vbyte3 [i] (byte-array
                     [(bit-and (bit-shift-right i 21) 0x7f)
                      (bit-and (bit-shift-right i 14) 0x7f)
                      (bit-and (bit-shift-right i 7) 0x7f)
                      (bit-or 0x80 (bit-and i 0x7f))]))

; Note we don't handle things over 0x10000000
(defn vbyte-encode [i]
    (cond
        (< i 0x80) (vbyte0 i)
        (< i 0x4000) (vbyte1 i)
        (< i 0x200000) (vbyte2 i)
        :else (vbyte3 i)))

;(into [] (vbyte-encode 16500))
;(into [] (vbyte-encode 129))
;(into [] (vbyte-encode 5))

; XXX not very efficient. For testing.
(defn vbyte-decode
    ([w] (vbyte-decode w 0 0))
    ([w acc offset]
     (let [n (first w)]
         (if (pos? (bit-and 0x80 n))
             (bit-or acc (bit-shift-left (bit-and 0x7F n) offset))
             (recur (rest w) (bit-or acc (bit-shift-left n offset)) (+ offset 7))))))

;
; pfc
;

(defn common-prefix
    ([a b] (common-prefix a b 0))
    ([^String a ^String b n]
     (if (and (> (.length a) n)
              (> (.length b) n)
              (= (.charAt a n) (.charAt b n)))
         (recur a b (inc n))
         n)))

(defn diff-rec [[^String a ^String b]]
    (let [n (common-prefix a b)]
        [(vbyte-encode n) (.getBytes (.substring b n)) [0]]))

(defn decompress [[init diffs]]
    (reductions (fn [^String prev [n suffix]] (str (.substring prev 0 n) suffix))
                init diffs))

(defn compress-bin [strings]
    {:header (into-array Byte/TYPE (concat (.getBytes ^String (first strings)) [0]))
     :inner (mapcat diff-rec (partition 2 1 strings))})

(defn compress-pfc [strings bin-size]
    (let [ordered (sort strings)
          bins (partition-all bin-size ordered)
          compressed (map compress-bin bins)
          sizes (map #(apply + (map count %)) compressed)]
        [sizes (into-array Byte/TYPE (apply concat (apply concat compressed)))]))

; Compress string collection with front-coding and huffman coding.
; Strings are split into bins, and each bin front-coded.
; Each bin is thus a full string ("header"), and a list of [prefix length, suffix],
; "inner strings".
; A huffman dictionary is computed over the inner strings (lens & suffixes),
; and each inner string bin is compressed against the huffman dict.
; A hu-tucker dictionary is computed over the headers, to allow
; binary search over the bins.
; The returned buffer is
; [header dictionary]
; [bin dictionary]
; [bin sizes]
; [concatenated bins]

; Is there a faster way to do the huffman encoding, than lazily
; consuming each byte array? It's a bit crazy. Reducer? Transducer?
; Run some profiles.

; Is it worth trying to minimize byte copying?
(defn get-bytes [method this & args]
    (let [out (java.io.ByteArrayOutputStream. 1000)]
        (apply method this out args)
        (.toByteArray out)))

(defn compress-htfc [strings bin-size]
     (let [out (java.io.ByteArrayOutputStream. 1000)
           ordered (sort strings)
           bins (partition-all bin-size ordered)
           front-coded (map compress-bin bins)
           ht (huffman/make-hu-tucker (map #(:header %) front-coded))
           compressed-headers (map #(get-bytes huffman/write ht (:header %))
                                   front-coded)
           huff (huffman/make-huffman (mapcat :inner front-coded))
           compressed (map #(get-bytes huffman/write huff (apply concat (:inner %)))
                           front-coded)
           sizes (map #(+ (count %1) (count %2)) compressed-headers compressed)
           offsets (reductions + 0 (take (dec (count sizes)) sizes))]
         (huffman/write-dictionary ht out)
         (huffman/write-dictionary huff out)
         (huffman/write-int (count offsets) out)
         (doseq [s offsets]
             (huffman/write-int s out))
         (doseq [[header inner]  (map #(-> [%1 %2])
                                      compressed-headers
                                      compressed)]
             (.write out ^bytes header)
             (.write out ^bytes inner))
         (let [total (.size out)] ; align to word boundary
             (dotimes [i (rem (- 4 (rem total 4)) 4)]
                 (.write out 0)))
         (.toByteArray out)))
