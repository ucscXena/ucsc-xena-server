(ns
  ^{:author "Brian Craft"
    :doc "huffman coding"}
  cavm.huffman
  (:import [java.util.concurrent ArrayBlockingQueue]))

; XXX would aget be faster than nth? Note that string nulls
; are coming in as a vector of one
; XXX could be done in parallel & merged, perhaps via transducer
(defn byte-freqs [coll]
    (let [freqs (transient {})]
        (loop [coll coll freqs freqs]
            (if-let [item (first coll)]
                (recur
                    (rest coll)
                    (let [len (count item)]
                        (loop [i 0 freqs freqs]
                            (if (< i len)
                                (let [c (nth item i)]
                                    (recur (inc i)
                                           (assoc! freqs c (inc (get freqs c 0)))))
                                freqs))))
                (persistent! freqs)))))

(defn- take-lowest [^ArrayBlockingQueue q0
                   ^ArrayBlockingQueue q1]
    (let [v0 (.peek q0) v1 (.peek q1)]
        (if (or (not v1) (and v0 (>= (:priority v1) (:priority v0))))
            (.take q0)
            (.take q1))))

; compute huffman tree from char freqs
(defn- build-tree [freqs]
    (let [leaves (->> freqs
                      (sort-by second)
                      (map #(zipmap [:symbol :priority] %))
                      (ArrayBlockingQueue. (count freqs) false))
          nodes (ArrayBlockingQueue. (count freqs))]
        (while (> (+ (.size leaves) (.size nodes)) 1)
            (let [a (take-lowest leaves nodes) b (take-lowest leaves nodes)
                  new-node {:priority (+ (:priority a) (:priority b)) :left a :right b}]
                (.add nodes new-node)))
        (.take nodes)))

;
; hu-tucker
;

(defn- compatible-pairs [nodes]
  (let [len (count nodes)]
    (for [i (range len)
          :when (nodes i)
          j (range (inc i) len)
          :when (nodes j)
          :while (or (= j (inc i)) (not (:symbol (nodes (dec j)))))]
      [i j])))

(defn- best-merge [nodes]
  (->> nodes
       compatible-pairs
       (map (fn [pair] {:priority (+ (:priority (nodes (pair 0))) (:priority (nodes (pair 1)))) :pair pair}))
       (apply min-key :priority)))

;(let [nodes [{:symbol :a :priority 12} nil {:priority 5} {:priority 11 :symbol :c} {:priority 7} nil]]
;  (best-merge nodes))

; compute hu-tucker tree from char freqs, O(n^2)
; There are O(n log n), and O(n) algorithms, but they're quite complex.
(defn- build-ht-tree [freqs]
  (let [leaves (->> freqs
                    (sort-by first)
                    (mapv #(zipmap [:symbol :priority] %)))
        len (count leaves)]
    (loop [i (dec len)
           nodes leaves]
      (if (> i 0)
        (let [to-merge (best-merge nodes)
              pair (:pair to-merge)
              next-nodes (assoc nodes
                                (pair 0) (assoc to-merge :left (nodes (pair 0)) :right (nodes (pair 1)))
                                (pair 1) nil)]
          (recur (dec i) next-nodes))
        (some identity nodes)))))

; would this work for huffman, too? It might simplify things to only have a single
; internal representation of a code. Or would it mess up the compact encoding of
; the dictionary? Speaking of which, is there a compact encoding of the hu-tucker
; dictionary?
(defn- make-ht-codes [tree]
  (loop [acc [] q [[0 0 tree]]]
    (if (seq q)
      (let [[[len code node] & more] q]
       (if-let [symbol (:symbol node)]
         (recur (conj acc {:symbol symbol :length len :code code}) more)
         (recur acc (conj more
                          [(inc len) (bit-shift-left code 1) (:left node)]
                          [(inc len) (bit-or (bit-shift-left code 1) 1) (:right node)]))))
      acc)))

(defn- ht-dictionary [codes]
  (->> codes
   (map #(-> [(:symbol %) %]))
   (into {})))

(defn write-int [i ^java.io.ByteArrayOutputStream out]
  (.write out (bit-and 0xff i))
  (.write out (bit-and 0xff (bit-shift-right i 8)))
  (.write out (bit-and 0xff (bit-shift-right i 16)))
  (.write out (bit-and 0xff (bit-shift-right i 24))))

; [[codes0] [codes1] ..
; Should the dict encoding be to bytes, or ByteArrayOutputStream,
; or something seqable?
; Might be able to use smaller ints here, instead of 32 bits.

; not sure grouping by length is worth the extra effort.
; Why output length for each code when we've grouped by length?
(comment (defn- ht-coded-dictionary [codes ^java.io.ByteArrayOutputStream out]
           (let [order (group-by :length codes)
                 len (:length (apply max-key :length codes))]
             (write-int len out)
             (doseq [i (range len)]
               (let [g (order i)]
                 (if g
                   (do
                     (write-int (count g) out)
                     (doseq [{:keys [code length symbol]} g]
                       (write-int code out)
                       (write-int length out)
                       (.write out ^byte symbol)))
                   (write-int 0 out)))))))

; if we word-align the ints, I suspect we'll be in a better place.
(defn- ht-coded-dictionary [codes ^java.io.ByteArrayOutputStream out]
  (write-int (count codes) out)
  (doseq [{:keys [code length]} codes]
            (write-int code out)
            (write-int length out))
  (doseq [{:keys [symbol]} codes]
            (.write out ^byte symbol))
  (dotimes [i (rem (- 4 (rem (count codes) 4)) 4)] ; extend to word boundary
    (.write out 0)))


(comment
  (defn- make-ht-codes 
    ([tree] (make-ht-codes tree []))
    ([tree codes]
     (if-let [sym (:symbol tree)]
       [{:symbol sym :codes (apply str codes)}]
       (concat
         (make-ht-codes (:left tree) (conj codes \0))
         (make-ht-codes (:right tree) (conj codes \1))))))

  (let [freqs {\d 17 \a 12 \z 2 \c 10 \b 5}]
    (->> freqs
         build-ht-tree
         make-ht-codes)))

(comment
  (let [freqs {\d 17 \a 12 \z 2 \c 10 \b 5}]
    (->> freqs
         build-ht-tree
         make-ht-codes
         ht-dictionary
         display-dictionary
         )))

; group huffman tree leaves by length from root
(defn- find-depth
    ([tree] (find-depth [tree] 0 []))
    ([nodes depth acc]
     (if (seq nodes)
         (let [{terminals true inner false} (group-by #(contains? % :symbol) nodes)
               children (mapcat #(-> [(:left %) (:right %)]) inner)]
             (recur children
                    (inc depth)
                    (if terminals (conj acc {:length depth :symbols terminals}) acc)))
         acc)))

; Compute canonical codes for tree leaves. This isn't a simple 'map' call
; because we need the 'code' and 'depth' context for each successive leaf depth.
(defn- make-codes
    ([groups] (make-codes groups 0 (:length (first groups)) (transient [])))
    ([groups code depth0 acc]
     (if-let [group (first groups)]
         (let [{depth1 :length nodes :symbols} group
               nodes (sort-by :symbol nodes)
               start (bit-shift-left code (- depth1 depth0))]
             (recur
                 (rest groups)
                 (+ start (count nodes))
                 depth1
                 (conj! acc 
                        (assoc group
                               :length depth1
                               :symbols (map #(assoc %1 :code %2)
                                             nodes
                                             (drop start (range))))))) ; XXX this is weird. Why don't we pass 'start' to 'range'?
         (persistent! acc))))

; build the huffman encoding
(defn build [items]
  (->> items byte-freqs build-tree find-depth make-codes))

; Build a hash for encoding the text
(defn dictionary [groups]
    (into {}
          (mapcat (fn [group]
                      (let [{:keys [length symbols]} group]
                          (map #(-> [(:symbol %) (assoc % :length length)]) symbols)))
                  groups)))

(comment
(defn spy [msg x]
  (println msg x)
  x))

; Return the compactly-encoded canonical dictionary
; len count, lens, bytes
(defn coded-dictionary [groups ^java.io.ByteArrayOutputStream out]
  (let [len-counts (into {} (map #(-> [(% :length) (count (% :symbols))]) groups))
        m (apply max (keys len-counts))]
    (write-int m out)
    (doseq [c (map #(get len-counts % 0) (range 1 (inc m)))]
      (write-int c out))
    (let [symbols (mapcat #(map :symbol (:symbols %)) groups)]
      (doseq [^byte b symbols]
        (.write out b))
      ; XXX this is probably the wrong place for this, since the input
      ; buffer is passed in.
      (dotimes [i (rem (- 4 (rem (count symbols) 4)) 4)] ; extend to word boundary
        (.write out 0)))))

; This is surprisingly complex. We're packing bits into bytes,
; which requires tracking the input and output positions independently,
; emitting bytes when they're full, and zero-padding the last
; partial byte.
(set! *unchecked-math* true)
(def masks (mapv #(- (bit-shift-left 1 %) 1) (range 9)))
(defn encode-bytes [^java.io.ByteArrayOutputStream output dict text]
    (let [codes (map #(get dict %) text)]
        (when (seq codes)
            (loop [codes codes
                   code (first codes)
                   out 0
                   avail 8
                   ^long remaining (:length code)]

                (let [bits (if (< avail remaining) avail remaining)
                      rs (- remaining bits)
                      out (bit-or (bit-shift-left out bits)
                                  (bit-and (masks bits) (bit-shift-right (:code code)
                                                                         rs)))
                      remaining (- remaining bits)
                      avail (- avail bits)]
                    (when (= avail 0)
                        (.write output out))

                    (if (= remaining 0)
                        (if-let [codes (seq (rest codes))]
                            (let [code (first codes)
                                  remaining (:length code)]
                                (if (= avail 0)
                                    (recur codes code 0 8 remaining)
                                    (recur codes code out avail remaining)))
                            (.write output (bit-shift-left out avail)))
                        (if (= avail 0)
                            (recur codes code 0 8 remaining)
                            (recur codes code out avail remaining))))))))
(set! *unchecked-math* false)

(defprotocol Encoder
  (write-dictionary [this out])
  (write [this out text]))

(defrecord huffman [groups dictionary]
  Encoder
  (write-dictionary [this out] (coded-dictionary groups out))
  (write [this out text] (encode-bytes out dictionary text)))

(defrecord hu-tucker [codes dictionary]
  Encoder
  (write-dictionary [this out] (ht-coded-dictionary codes out))
  (write [this out text] (encode-bytes out dictionary text)))

(defn make-huffman [items]
  (let [groups (build items)]
    (->huffman groups (dictionary groups))))

(defn make-hu-tucker [items]
  (let [codes (->> items byte-freqs build-ht-tree make-ht-codes)]
    (->hu-tucker codes (ht-dictionary codes))))

; Really want a decode for the output buff.

(defn- ^String toBinaryStr [n]
    (.replace (format "%8s" (Integer/toBinaryString n)) " " "0"))

(defn display-dictionary [dict]
    (clojure.pprint/print-table
      (map #(-> {:symbol (char (:symbol %))
                 :code (.substring (toBinaryStr (:code %)) (- 8 (:length %)) 8)})
           (sort-by :symbol (vals dict)))))

(comment
  (let [s (map #(.getBytes ^String %) ["foo" "bar" "baz" "one" "two" "three"])]
        (display-dictionary (dictionary (build s))))
  (let [s (map #(.getBytes ^String %) ["foo" "bar" "baz" "one" "two" "three" "zzzzzzz"])
        huffman (build s)
        dict (dictionary huffman)
        encoded (encode-bytes dict (apply concat s))
        ]
    (display-dictionary dict)
    [(count encoded) (into [] encoded)]))
