(ns cavm.huffman-test
  (:require [clojure.test :as ct])
  (:require [clojure.test.check :as tc])
  (:require [clojure.test.check.generators :as gen])
  (:require [clojure.test.check.clojure-test :as tcct :refer [defspec]])
  (:require [clojure.test.check.properties :as prop])
  (:require [cavm.huffman :as huffman])
  (:import [java.io ByteArrayOutputStream])
  (:import [java.nio ByteBuffer ByteOrder])
  )

(defn spy [msg x]
  (println msg x)
  x)

(def ^:dynamic *test-runs* 4000)

(defspec coded-dictionary
  *test-runs*
  (prop/for-all
    [symbols (gen/vector (gen/tuple (gen/choose 1 10) (gen/choose 0 255)))]
    (let [out (ByteArrayOutputStream.)
          groups (map (fn [[l s]] {:length l :symbols s}) (group-by :length (map (fn [[l s]] {:length l :symbol s}) symbols)))]
      (huffman/coded-dictionary groups out)
      (let [buff (.toByteArray out)
            buff32 (doto (ByteBuffer/wrap buff) (.order ByteOrder/LITTLE_ENDIAN))
            len (/ (count buff) 4)]
        ; Check that our computed length (for decoding) matches the output length
        (= (huffman/huff-dict-len (.asIntBuffer buff32) 0) len)))))

(defspec ht-coded-dictionary
  *test-runs*
  (prop/for-all
    [codes-tuple (gen/vector (gen/tuple (gen/choose 1 10) (gen/choose 0 255) (gen/choose 0 255)))]
    (let [out (ByteArrayOutputStream.)
          codes (map (fn [[c l s]] {:code c :length l :symbol s}) codes-tuple)]
      (huffman/ht-coded-dictionary codes out)
      (let [buff (.toByteArray out)
            buff32 (doto (ByteBuffer/wrap buff) (.order ByteOrder/LITTLE_ENDIAN))
            len (/ (count buff) 4)]
        ; Check that our computed length (for decoding) matches the output length
        (= (huffman/ht-dict-len (.asIntBuffer buff32) 0) len)))))
