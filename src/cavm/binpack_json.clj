(ns
  ^{:author "Brian Craft"
    :doc "json with packed binary objects"}
  cavm.binpack-json
  (:import [java.io PrintWriter StringWriter ByteArrayOutputStream])
  (:import [cavm HTFC])
  (:require [cavm.pfc :refer [index-of]]) ; maybe move this method
  (:import [org.h2.jdbc JdbcBlob])
  (:import [java.util Arrays])
  (:import [java.nio ByteBuffer ByteOrder])
  (:require clojure.walk)
  (:require [clojure.data.json :as json]))


; Kind of hacky way of allowing packed binary objects in json, without needing a
; json binary solution.

; Encodes an object tree as a series of word-aligned binary buffers, where the
; first buffer is json with json graph references to an implicit array, $bin,
; holding the following binary buffers.
; The first buffer is null-terminated. The following buffers start with a
; 32-bit 'length' field.

; adapted from clojure.data.json

(defprotocol BinpackWriter
  (-write [x buff out i]))

(defn- key-fn
  [x]
  (cond (instance? clojure.lang.Named x)
        (name x)
        (nil? x) "null" ; json semantics. Not sure why clojure.data.json is different
        :else (str x)))

; little endian

(defn write-int [i ^java.io.ByteArrayOutputStream out]
  (.write out (bit-and 0xff i))
  (.write out (bit-and 0xff (bit-shift-right i 8)))
  (.write out (bit-and 0xff (bit-shift-right i 16)))
  (.write out (bit-and 0xff (bit-shift-right i 24))))

(defn read-int [i ^bytes in]
  (bit-or (bit-and 0xff (aget in i))
          (bit-shift-left (bit-and 0xff (aget in (+ i 1))) 8)
          (bit-shift-left (bit-and 0xff (aget in (+ i 2))) 16)
          (bit-shift-left (bit-and 0xff (aget in (+ i 3))) 24)))

(def pad (byte-array [0 0 0]))

(defn write-bin [^bytes bin ^ByteArrayOutputStream buff ^PrintWriter out i]
  (let [len (alength bin)
        padding (mod (- len) 4)]
    (.write out (str "{\"$type\":\"ref\",\"value\":{\"$bin\":" @i "}}"))
    (swap! i inc)
    (write-int len buff)
    (.write buff bin 0 len)
    (.write buff pad 0 padding))) ; align to word boundary

; Have to re-implement recursive types, instead of re-using the json
; ones, so the -write calls stay in our methods, instead of escaping
; into the json methods.
(defn- write-array [s buff ^PrintWriter out i]
  (.print out \[)
  (loop [x s]
    (when (seq x)
      (let [fst (first x)
            nxt (next x)]
        (-write fst buff out i)
        (when (seq nxt)
          (.print out \,)
          (recur nxt)))))
  (.print out \]))

; see comment above write-array
(defn- write-object [m buff ^PrintWriter out i]
  (do
    (.print out \{)
    (loop [x m, have-printed-kv false]
      (when (seq m)
        (let [[k v] (first x)
              out-key (key-fn k)
              out-value v
              nxt (next x)]
          (when have-printed-kv
            (.print out \,))
          (-write out-key buff out i)
          (.print out \:)
          (-write out-value buff out i)
          (when (seq nxt)
            (recur nxt true)))))
    (.print out \})))

(defn unwrap-blob [^JdbcBlob blob]
  (let [len (.length blob)]
    (.getBytes blob 0 len)))

(defn- write-jdbc-blob [m buff ^PrintWriter out i]
  (write-bin (unwrap-blob m) buff out i))

(defn- write-htfc [^HTFC m buff ^PrintWriter out i]
  (write-bin (.getBytes m) buff out i))

; ugh. java buffer support sucks. Two copies on the way out
; due to weird buffer APIs.
(defn- write-float-bin [^floats m buff ^PrintWriter out i]
  (let [bb (doto (ByteBuffer/allocate (* (count m) 4))
             (.order java.nio.ByteOrder/LITTLE_ENDIAN))]
    (doseq [f m]
      (.putFloat bb f))
    (write-bin (.array bb) buff out i)))

(defn- write-double-bin [^doubles m buff ^PrintWriter out i]
  (let [bb (doto (ByteBuffer/allocate (* (count m) 4))
             (.order java.nio.ByteOrder/LITTLE_ENDIAN))]
    (doseq [f m]
      (.putFloat bb (float f)))
    (write-bin (.array bb) buff out i)))

(extend Object BinpackWriter {:-write (fn [x buff out i] (json/write x out))})
(extend nil BinpackWriter {:-write (fn [x buff out i] (json/write x out))})
(extend java.util.Map BinpackWriter {:-write write-object})
(extend java.util.Collection BinpackWriter {:-write write-array})
(extend (Class/forName "[B") BinpackWriter {:-write write-bin})
(extend (Class/forName "[F") BinpackWriter {:-write write-float-bin})
(extend (Class/forName "[D") BinpackWriter {:-write write-double-bin})
(extend JdbcBlob BinpackWriter {:-write write-jdbc-blob})
(extend HTFC BinpackWriter {:-write write-htfc})

(defn write-buff [x]
  (let [sw (StringWriter.)
        out (ByteArrayOutputStream.)
        bins (ByteArrayOutputStream.)
        i (atom 0)]
    (-write x bins (PrintWriter. sw) i)
    (let [ba (.getBytes (.toString sw))
          padding (mod (- (inc (count ba))) 4)]
      (.write out ba 0 (count ba))
      (.write out 0)
      (.write out pad 0 padding))
    (.writeTo bins out)
    (.toByteArray out)))

(defn align [p]
  (* 4 (quot (+ 3 p) 4)))

(defn parse [parser ^bytes buff]
  (let [len (alength buff)
        txt-len (index-of buff 0 0)
        bins (loop [out []
                    in-p (align (inc txt-len))]
               (if (< in-p len)
                 (let [bin-len (read-int in-p buff)]
                   (recur (conj out
                                (Arrays/copyOfRange buff ^long (+ in-p 4) ^long (+ in-p 4 bin-len)))
                          (+ in-p 4 (align bin-len))))
                 out))
        link (fn [x] (if (= (get x "$type") "ref")
                       (bins (get-in x ["value" "$bin"]))
                       x))]
    (clojure.walk/postwalk link (parser (String. ^bytes (Arrays/copyOfRange buff 0 ^long txt-len))))))

(def parse-json (partial parse json/read-str))

(comment
(defn write [x buff writer]
  (-write x buff (PrintWriter. writer))))

(comment
  ;(import '[java.io StringWriter])
  ;(import '[java.io ByteArrayOutputStream])
  (let [buff (write-buff {"foo" "bar" "baz" [1 2 3 nil] })]
    (String. (java.util.Arrays/copyOfRange buff 4 (alength buff)))
    )

  (instance? java.util.Map (pfc/->htfc (byte-array [1 2 3])))
  (instance? htfc (pfc/->htfc (byte-array [1 2 3])))

  ; XXX should bins be word-aligned? Probably.
  (let [^bytes buff (write-buff {"foo" "bar" "baz" (pfc/->htfc (byte-array [1 2 3])) "samples" (pfc/->htfc (byte-array [1 2 3]))})]
    ;(String. (java.util.Arrays/copyOfRange buff 20 (alength buff)))
    (into [] buff)
    )

  (defn run-length [nums]
    (loop [nums nums acc [] p []]
      (if-let [n (first nums)]
        (if (= n 0)
          (recur (rest nums) acc (conj p n))
          (recur (rest nums) (conj (if (seq p) (conj acc p) acc) n) []))
        acc)))

  (defn ln2
    ([n] (ln2 n 0))
    ([n i]
     (if (= n 0) i (recur (bit-shift-right n 1) (inc i)))))

  (run-length [0 1 2 3 0 0 0 7])


  ; if 5% of genes are populated,
  (let [N 1000000
        exp (repeatedly N #(if (< (rand-int 100) 10) (inc (rand-int 100)) 0))
        p (count (filter #(not= 0 %) exp))]
    (println (str p " populated(" (float (* 100 (/ p N))) "%)"))
    (println (str (* 4 2 p) " bytes for p 32 bit floats + p 32 bit int cell ids"))
    (let [rl (run-length exp)
          n (count rl)
          cs (count (filter vector? rl))
          m (apply max (map count (filter vector? rl)))
          bits (long (Math/ceil (/ (ln2 m) 8)))
          ]

      (println (str (+ (* 4 n) (* cs bits)) " min bytes " n " slots, " cs " bins in run-length, max len " m ", " bits " bytes")))

    )

  )

