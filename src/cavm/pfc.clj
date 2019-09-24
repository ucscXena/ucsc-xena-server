(ns
  ^{:author "Brian Craft"
    :doc "front coding"}
  cavm.pfc
  (:import [java.util.concurrent ArrayBlockingQueue])
  (:import [java.nio ByteBufferAsIntBufferL ByteBuffer])
  (:import [java.io ByteArrayOutputStream])
  (:import [cavm Huffman HTFC HFC])
  (:require [cavm.huffman :as huffman]))

(set! *unchecked-math* true)

;
; vbyte
;

(defn vbyte-decode-buff [^bytes buff8 offset8]
   (loop [^long i offset8 x 0]
     (let [b (aget buff8 i)]
       (if (= 0 (bit-and b 0x80))
         (recur (inc i) (bit-shift-left (bit-or x b) 7))
         [(bit-or x (bit-and b 0x7f)) (inc i)]))))

(defn vbyte0 [^ByteArrayOutputStream acc ^long i]
  (.write acc (bit-or 0x80 i)))

(defn vbyte1 [^ByteArrayOutputStream acc ^long i]
  (.write acc (bit-shift-right i 7))
  (.write acc (bit-or 0x80 (bit-and i 0x7f))))

(defn vbyte2 [^ByteArrayOutputStream acc ^long i]
  (.write acc (bit-and (bit-shift-right i 14) 0x7f))
  (.write acc (bit-and (bit-shift-right i 7) 0x7f))
  (.write acc (bit-or 0x80 (bit-and i 0x7f))))

(defn vbyte3 [^ByteArrayOutputStream acc ^long i]
  (.write acc (bit-and (bit-shift-right i 21) 0x7f))
  (.write acc (bit-and (bit-shift-right i 14) 0x7f))
  (.write acc (bit-and (bit-shift-right i 7) 0x7f))
  (.write acc (bit-or 0x80 (bit-and i 0x7f))))

; Note we don't handle things over 0x10000000

(defn vbyte-encode [^ByteArrayOutputStream acc ^long i]
    (cond
        (< i 0x80) (vbyte0 acc i)
        (< i 0x4000) (vbyte1 acc i)
        (< i 0x200000) (vbyte2 acc i)
        :else (vbyte3 acc i)))

;
; front-coding
;

(defn common-prefix
    ([a b] (common-prefix a b 0))
    ([^String a ^String b ^long n]
     (if (and (> (.length a) n)
              (> (.length b) n)
              (= (.charAt a n) (.charAt b n)))
         (recur a b (inc n))
         n)))

(defn write-str-null [^ByteArrayOutputStream acc ^String string]
  (let [ba (.getBytes string)]
      (.write acc ba 0 (alength ba)))
  (.write acc 0))

(defn diff-rec [^ByteArrayOutputStream acc ^String a ^String b]
  (let [n (common-prefix a b)]
    (vbyte-encode acc n)
    (write-str-null acc (.substring b n))))

(defn compute-inner-baos [strings acc]
  (let [sv (into [] strings)
        n (- (count sv) 1)]
    (loop [i 0]
      (if (< i n)
        (do
          (diff-rec acc (get sv i) (get sv (inc i)))
          (recur (inc i)))))))

; front-code inner bin (no initial string)
(defn front-code-rest [strings]
  (let [acc (ByteArrayOutputStream.)]
    (compute-inner-baos strings acc)
    [(.toByteArray acc)]))

; front-code bin (including initial string)
(defn front-code [strings]
  (let [acc (ByteArrayOutputStream.)]
    (write-str-null acc (first strings))
    (compute-inner-baos strings acc)
    [(.toByteArray acc)]))

(defn compress-bin [strings]
    {:header (into-array Byte/TYPE (concat (.getBytes ^String (first strings)) [0])) ; XXX eliminate concat. use implicit zero?
     :inner (front-code-rest strings)})

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

; Is it worth trying to minimize byte copying?
(defn get-bytes [method this & args]
    (let [out (java.io.ByteArrayOutputStream. 1000)]
        (apply method this out args)
        (.toByteArray out)))

(defn partition-all-v [n v]
  (let [c (count v)
        m (quot c n)
        acc (transient [])]
    (loop [i 0]
      (when (< i m)
        (conj! acc (subvec v (* i n) (* (inc i) n)))
        (recur (inc i))))
    (let [r (rem c n)]
      (when (> r 0)
        (conj! acc (subvec v (- c r) c))))
    (persistent! acc)))

(defn serialize-htfc [htfc]
  (let [out (java.io.ByteArrayOutputStream. 1000)
        {:keys [length bin-size header-dict inner-dict offsets headers inners]} htfc]
    (huffman/write-int length out)
    (huffman/write-int bin-size out)
    (huffman/write-dictionary header-dict out)
    (huffman/write-dictionary inner-dict out)
    (huffman/write-int (count offsets) out)
    (doseq [s offsets]
      (huffman/write-int s out))
    (doseq [[header inner]  (map #(-> [%1 %2])
                                 headers
                                 inners)]
      (.write out ^bytes header)
      (.write out ^bytes inner))
    (let [total (.size out)] ; align to word boundary
      (dotimes [i (rem (- 4 (rem total 4)) 4)]
        (.write out 0)))
    (.toByteArray out)))

(require '[clojure.core.reducers :as r])
(defn compress-htfc-sorted ^HTFC [ordered bin-size]
  (let [out (java.io.ByteArrayOutputStream. 1000)
        bins (partition-all-v bin-size (into [] ordered))
        ; we reduce this multiple times, so compute it as foldable.
        front-coded (r/foldcat (r/map compress-bin bins))

        ht (huffman/make-hu-tucker (map #(:header %) front-coded))
        ; XXX Does get-bytes make sense? Why do we need to do this?
        compressed-headers (map #(get-bytes huffman/write ht (:header %))
                                front-coded)
        huff (huffman/make-huffman (r/mapcat :inner front-coded))
        compressed (r/foldcat (r/map #(get-bytes huffman/write huff (:inner %))
                                     front-coded))
        sizes (map #(+ (count %1) (count %2)) compressed-headers compressed)
        offsets (reductions + 0 (take (dec (count sizes)) sizes))]
    (HTFC. (serialize-htfc {:length (count ordered)
                            :bin-size bin-size
                            :header-dict ht
                            :inner-dict huff
                            :offsets offsets
                            :headers compressed-headers
                            :inners compressed}))))

(defn compress-htfc [strings bin-size]
  (compress-htfc-sorted (sort strings) bin-size))

(defn serialize-hfc [hfc]
  (let [out (java.io.ByteArrayOutputStream. 1000)
        {:keys [length bin-size dict offsets bins]} hfc]
    (huffman/write-int length out)
    (huffman/write-int bin-size out)
    (huffman/write-dictionary dict out)
    (huffman/write-int (count offsets) out)
    (doseq [s offsets]
      (huffman/write-int s out))
    (doseq [bin bins]
      (.write out ^bytes bin))
    (let [total (.size out)] ; align to word boundary
      (dotimes [i (rem (- 4 (rem total 4)) 4)]
        (.write out 0)))
    (.toByteArray out)))

(defn compress-hfc [strings bin-size]
  (let [out (java.io.ByteArrayOutputStream. 1000)
        ordered (sort strings)
        bins (partition-all-v bin-size (into [] ordered))
        front-coded (r/foldcat (r/map front-code bins))
        huff (huffman/make-huffman (r/mapcat identity front-coded))
        compressed (r/foldcat (r/map #(get-bytes huffman/write huff %) front-coded))
        sizes (map count compressed)
        offsets (reductions + 0 (take (dec (count sizes)) sizes))]
    (HFC. (serialize-hfc {:length (count ordered)
                          :bin-size bin-size
                          :dict huff
                          :offsets offsets
                          :bins compressed}))))

(defn index-of [^bytes arr start v]
  (loop [i start]
      (if (= (aget arr i) v)
          i
          (recur (inc i)))))

(defn buff-to-string [^bytes buff ^long offset]
  (String. (java.util.Arrays/copyOfRange buff offset (long (index-of buff offset 0)))))

(defn next-str [buff i prev]
  (let [[plen nxt] (vbyte-decode-buff buff i)
        ^String s (buff-to-string buff nxt)
        n (str (.substring ^String prev 0 plen) s)]
    [n (+ nxt (.length s))]))

(defn uncompress-inner
  ([header buff N]
   (uncompress-inner header buff N 0))
  ([header buff N start]
   (let [acc (transient [header])]
     (loop [i 0 p start s header]
       (if (< i N)
         (let [[s p] (next-str buff p s)]
           (conj! acc s)
           (recur (inc i) (long p) s))
         (persistent! acc))))))

(defn uncompress-bin-htfc [dict i]
  (let [out (ByteArrayOutputStream.)
        {:keys [length bin-size bin-count first-bin bin-offsets header-dict ^ByteBuffer buff8 ^ByteBufferAsIntBufferL buff32 jhuff]} dict
        bin (+ (* 4 first-bin) (.get buff32 (long (+ bin-offsets i))))
        headerP (huffman/decode-to header-dict buff8 bin out 0)
        header (buff-to-string (.toByteArray out) 0)
        remainder (rem length bin-size)
        N (cond (= remainder 0) bin-size
                (= i (dec bin-count)) remainder
                :else bin-size)
        upper (if (= i (dec bin-count)) (.capacity buff8)
                (+ (* 4 first-bin) (.get buff32 (long (+ bin-offsets 1 i)))))]

    (.reset out)
    (.decodeRange ^Huffman jhuff buff8 headerP upper out)
    (uncompress-inner header (.toByteArray out) (dec N)))) ; already have 1st string

(defn uncompress-bin-hfc [dict i]
  (let [out (ByteArrayOutputStream.)
        {:keys [length bin-size bin-count first-bin bin-offsets bin-dict ^ByteBuffer buff8 ^ByteBufferAsIntBufferL buff32 jhuff]} dict
        bin (+ (* 4 first-bin) (.get buff32 (long (+ bin-offsets i))))
        remainder (rem length bin-size)
        N (cond (= remainder 0) bin-size
                (= i (dec bin-count)) remainder
                :else bin-size)
        upper (if (= i (dec bin-count)) (.capacity buff8)
                (+ (* 4 first-bin) (.get buff32 (long (+ bin-offsets 1 i)))))]

    (.decodeRange ^Huffman jhuff buff8 bin upper out)
    (let [ba (.toByteArray out)
          header (buff-to-string ba 0)]
      (uncompress-inner header ba (dec N) (inc (count header))))))

(defn uncompress-dict-htfc [dict]
  (apply concat (for [i (range 0 (:bin-count dict))] (uncompress-bin-htfc dict i))))

(defn uncompress-dict-hfc [dict]
  (apply concat (for [i (range 0 (:bin-count dict))] (uncompress-bin-hfc dict i))))

; constructor for use with clojure decompress methods.
; deprecated in favor of cavm.HTFC class.
(defn htfc-offsets [^ByteBuffer buff8]
  (let [buff8 (doto buff8 (.order java.nio.ByteOrder/LITTLE_ENDIAN))
        buff32 (.asIntBuffer buff8)
        length (.get buff32 0)
        bin-size (.get buff32 1)
        bin-dict-offset (+ 2 (huffman/ht-dict-len buff32 2))
        bin-count-offset (+ bin-dict-offset (huffman/huff-dict-len buff32 bin-dict-offset))
        bin-count (.get buff32 ^long bin-count-offset) ; why store this, vs. length / bin-size?
        first-bin (+ bin-count-offset bin-count 1)
        jhuff (Huffman.)]

    (.tree jhuff buff32 buff8 bin-dict-offset)

    {:buff8 buff8
     :buff32 buff32
     :length length
     :bin-size bin-size
     :bin-dict-offset bin-dict-offset
     :bin-count-offset bin-count-offset
     :bin-offsets (inc bin-count-offset)
     :bin-count bin-count
     :first-bin first-bin
     :header-dict (huffman/ht-tree buff32 buff8 8)
     :inner-dict (huffman/huff-tree buff32 buff8 bin-dict-offset); XXX deprecated
     :jhuff jhuff}))

(defn hfc-offsets [^ByteBuffer buff8]
  (let [buff32 (.asIntBuffer buff8)
        length (.get buff32 0)
        bin-size (.get buff32 1)
        bin-dict-offset 2
        bin-count-offset (+ bin-dict-offset (huffman/huff-dict-len buff32 bin-dict-offset))
        bin-count (.get buff32 ^long bin-count-offset)
        first-bin (+ bin-count-offset bin-count 1)
        jhuff (Huffman.)]

    (.tree jhuff buff32 buff8 bin-dict-offset)

    {:buff8 buff8
     :buff32 buff32
     :length length
     :bin-size bin-size
     :bin-dict-offset bin-dict-offset
     :bin-count-offset bin-count-offset
     :bin-offsets (inc bin-count-offset)
     :bin-count bin-count
     :first-bin first-bin
     :bin-dict (huffman/huff-tree buff32 buff8 bin-dict-offset)
     :jhuff jhuff}))

; use clojure decompress methods to return a seq
; deprecated
(defn dict-seq [dict]
  (mapcat #(uncompress-bin-htfc dict %)
          (range (:bin-count dict))))

;
; computing union
;

(defn merge-sorted [a b]
  (if-let [n (first a)]
    (if-let [m (first b)]
      (if (= n m)
        (lazy-seq (cons n (merge-sorted (rest a) (rest b))))
        (if (< (compare n m) 0)
          (lazy-seq (cons n (merge-sorted (rest a) b)))
          (lazy-seq (cons m (merge-sorted a (rest b))))))
      a)
    b))

; XXX put bin size somewhere
(defn merge-dicts [dict & dicts]
  (if (seq dicts)
    (compress-htfc-sorted
       (reduce #(merge-sorted %1 %2)
               dict
               dicts)
       256)
    dict))
;
;
;

(comment

  (require '[clojure.data.json :as json])
  (use 'criterium.core)

  ;let [file "toil.json"]
  (let [file "singlecell.json"]
    (def strings
      (json/read-str (slurp file))))

  (let [file "toil.json"]
    (def strings
      (json/read-str (slurp file))))

  (time
    (def buff2
      (compress-htfc strings 256)))

  (defn spy [msg x]
    (println msg x)
    x)

  (time (do (front-coding strings) nil))

  (defn subset-strings []
    (let [to-drop (into #{} (repeatedly 100 #(rand-int (count strings))))]
      (filter identity (map-indexed (fn [i s] (if (to-drop i) nil s)) strings))))

  (def strings-a (subset-strings))
  (def strings-b (subset-strings))
  (count strings-a)
  (count strings-b)
  (count strings)

  ; 2.1s merge
  (let [htfc-a (compress-htfc strings-a 256)
        htfc-b (compress-htfc strings-b 256)
        htfc-c (time (merge-dicts htfc-a htfc-b))]
    (count (seq (HTFC. htfc-c))))

  (require '[cavm.h2 :refer [sampleID-codes]])

  ; 3.6-4s, at 256 bin size. size 16 through 8192: basically no change.
  ; 1.3s, osx
  (def sample-codes
    (let [a (compress-htfc strings-a 256)
            b (compress-htfc strings-b 256)]
      (time (sampleID-codes a b))))

  (import 'cavm.HTFC)
  ; 300ms
  (def sample-codes
    (let [a (HTFC. (compress-htfc strings-a 256))
          b (HTFC. (compress-htfc strings-b 256))]
      (time (HTFC/join a b))))

  (let [a (compress-htfc strings-a 256)
        b (compress-htfc strings-b 256)
        ha (HTFC. a)
        hb (HTFC. b)
        hj (HTFC/join ha hb)
        sc (map #(if % (int %) nil) (sampleID-codes a b))]
    (= hj sc))

  (do
    (require '[clojure.data.json :as json])
    (let [file "toil.json"]
      (def strings
        (json/read-str (slurp file))))
    (defn subset-strings []
      (let [to-drop (into #{} (repeatedly 100 #(rand-int (count strings))))]
        (filter identity (map-indexed (fn [i s] (if (to-drop i) nil s)) strings))))
    (def strings-a (subset-strings))
    (def strings-b (subset-strings))
    (require '[cavm.h2 :refer [sampleID-codes0]])
    (import 'cavm.HFC))

  (def a (HFC. (compress-hfc strings-a 258)))
  (take 10 a)
  (let [a (compress-hfc strings-a 258)
          b (compress-hfc strings-b 256)
          ha (HFC. a)
          hb (HFC. b)
          hj (HFC/join ha hb)
          sc (map #(if % (int %) nil) (sampleID-codes0 a b))]
      (= hj sc))

  ; round trip hfc
  (let [strings strings
        in (time (compress-hfc strings 256))
        out (time (into [] (uncompress-dict-hfc (hfc-offsets (doto (ByteBuffer/wrap in)
                                                       (.order java.nio.ByteOrder/LITTLE_ENDIAN))))))]
    (= (sort strings) out))

  ; round trip
  (let [strings strings
        in (time (compress-htfc strings 256))
        out (time (into [] (uncompress-dict-htfc (htfc-offsets (doto (ByteBuffer/wrap in)
                                                    (.order java.nio.ByteOrder/LITTLE_ENDIAN))))))]
    (= (sort strings) out))

  (time
    (do
      (into [] (uncompress-dict-htfc (htfc-offsets in)))
      nil))

  (use 'clojure.java.io)
  (with-open [file (java.io.FileOutputStream. "toil.bin")]
    (.write file buff2))

  ; Is there some nio mapped buffer solution?
  (defn file-to-byte-array [filename]
    (let [^java.io.File file (clojure.java.io/as-file filename)
          result (byte-array (.length file))]
      (with-open [in (java.io.DataInputStream. (clojure.java.io/input-stream file))]
        (.readFully in result))
      (doto (java.nio.ByteBuffer/wrap result)
        (.order java.nio.ByteOrder/LITTLE_ENDIAN))))

  (def in (file-to-byte-array "toil.bin"))

; goal: back-off perf fixes if they aren't helping.
; goal: unify ht & huff methods
; goal: make decode methods more uniform wrt 32 vs. 8 bit access
; goal: make better method & property names
; add end-to-end test

  ; shannon
  (let [in (into-array Byte/TYPE (mapcat #(conj (seq (.getBytes %)) (char 0))
                                         strings))
        freqs (huffman/byte-freqs [in])
        len (count in)
        p (map (fn [[sym n]] (/ n len)) freqs)
        ln2 (Math/log 2)]
    (- (reduce + 0 (map #(* (/ (Math/log %) ln2) %) p))))

  (/ 4.6 1.3) ; 3.5 bytes per string, when headers are compressed
  (float (/ (count buff2) (count strings))) ; 5.7 bytes for toil???
  ; 4.6 *bits* shannon entropy???
  (float (/ (count buff2) (apply + (map count strings)))) ; 31% for toil
  ; 18% for singlecell

  ;(spit "singlecell.tsv" (clojure.string/join "\n" strings))
  ; around 128k for toil, 5.8 bytes per string
  ; not compressing the headers. Also, mis-counting sizes
  ; 5M for singlecell, 3.9 bytes per string
  (float (/ 207422 22000))    ; 9.42 bytes per string, toil
  (let [[[sizes symbols] blocks block-sizes headers] buff2
        total

        (+ (count sizes)
           (count symbols)
           (apply + (map count blocks))
           (* 4 (count block-sizes))
           (apply + (map count headers)))]
    [total (float (/ total (count strings)))])

  (time (compress-htfc strings 32))

  ; 1.3M 8,397,304 29%, at bin-size 100. At 32 it's 8.9M
  ;      orig is 28,988,248
  ;      gzip is, what, 7.7M, for 26.8%
  ; There was some off-the-shelf compressor I tried, that was better than gzip.
  ; What was it?
  ; In theory, we could push this closer to 10%?

  ; Should we combine the prefix len & the suffix len in one byte? We're probably
  ; wasting a lot of space there.


  ; toil
  ; 22k strings, 463964
  ; compressed   207422 ugh. This is pathetic. Why is it so bad? There's
  ;                          more entropy in tcga/gtex ids?

  (float (/ 207422 22000))    ; 9.42 bytes per string, toil
  (float (/ 8900000 1300000)) ; 6.8 bytes per string, singlecell

(defn reload []
  (virgil.compile/compile-all-java ["src-java/cavm"])
  (import 'cavm.HFC)
  (import 'cavm.HTFC)
  (import 'cavm.Huffman)
  )

(reload)

)
