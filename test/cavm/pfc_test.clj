(ns cavm.pfc-test
  (:require [cavm.pfc :as pfc])
  (:require [clojure.java.shell :refer [sh]])
  (:require [clojure.java.io :as io])
  (:require [clojure.data.json :as json])
  (:require [clojure.test :refer [deftest testing is]])
  (:require [clojure.test.check :as tc])
  (:require [clojure.test.check.generators :as gen])
  (:require [clojure.test.check.clojure-test :as tcct :refer [defspec]])
  (:require [clojure.test.check.properties :as prop])
  (:import [cavm HTFC])
  (:import [java.io ByteArrayOutputStream])
  (:import [java.nio ByteBuffer]))

(def ^:dynamic *test-runs* 4000)

; using ascii string.
(def gen-string (gen/not-empty gen/string-ascii))

(defn gen-distinct [gen]
  (gen/fmap #(distinct %) gen))

;
; vbyte tests
;

(defspec vbyte-round-trip-test
  *test-runs*
  (prop/for-all [i (gen/such-that #(< % 0x10000000) gen/nat)]
                (let [out (ByteArrayOutputStream.)]
                  (pfc/vbyte-encode out i)
                  (= (first (pfc/vbyte-decode-buff (.toByteArray out) 0)) i))))

;
;
;

(comment (deftest basic-test
           (testing "fixed set"
             (let [in ["one" "two" "three" "four" "five" "six"]
                   dict (pfc/compress-htfc in 5)]
               (with-open [file (java.io.FileOutputStream. "test.bin")]
                 (.write file dict))
               (let [{out :out} (sh "node" "isomorphic.js" "test.bin")
                     res (json/read-str out)]
                 (is (= (set in) (set res))))))
           (comment (testing "fixed set 2"
                      ; check handling of zero-length front-coding, caused by
                      ; repeated string, in encode-bytes.
                      (let [in ["one" "one" "two" "three" "four" "five" "six"]
                            dict (pfc/compress-htfc in 5)]
                        (with-open [file (java.io.FileOutputStream. "test.bin")]
                          (.write file dict))
                        (let [{out :out} (sh "node" "isomorphic.js" "test.bin")
                              res (json/read-str out)]
                          (is (= (set in) (set res)))))))
           (comment (testing "toil"
              (let [in (json/read-str (slurp "toil.json"))
                    dict (pfc/compress-htfc in 5)]
                (with-open [file (java.io.FileOutputStream. "test.bin")]
                  (.write file dict))
                (let [{out :out} (sh "node" "isomorphic.js" "test.bin")
                      res (json/read-str out)]
                  (is (= (set in) (set res)))))))))

(defn dict-to-vec [dict]
  (into []
        (pfc/uncompress-dict-htfc (pfc/htfc-offsets (doto (ByteBuffer/wrap dict)
                                         (.order java.nio.ByteOrder/LITTLE_ENDIAN))))))

(deftest basic-test-clj
  (testing "fixed set"
    (let [in ["one" "two" "three" "four" "five" "six"]
          dict (pfc/compress-htfc in 5)
          out (dict-to-vec dict)]
      (is (= (sort in) out))))
  (testing "fixed set 2"
    ; check handling of zero-length front-coding, caused by
    ; repeated string, in encode-bytes.
    (let [in ["one" "one" "two" "three" "four" "five" "six"]
          dict (pfc/compress-htfc in 5)
          out (dict-to-vec dict)]
      (is (= (sort in) out))))
  ; where to get a large test set w/o committing all this garbage?
  ; maybe compress it? Or fetch & cache? Or compress with htfc?
  (comment (testing "toil"
     (let [in (json/read-str (slurp "toil.json"))
           dict (pfc/compress-htfc in 256)
           out (dict-to-vec dict)]
       (is (= (sort in) out))))))

(defn htfc [strings bin-size]
  (pfc/to-htfc (pfc/compress-htfc strings bin-size)))

(defn htfc-merge [a b]
  (pfc/merge-dicts a b))

(defn check-merge [ctor merger coll-a coll-b bin-size-1]
  (let [coll-a (sort coll-a)
        coll-b (vec (sort coll-b))
        ; enforcing bin size of at least 2, to avoid uninteresting boundaries.
        bin-size (inc bin-size-1)
        db-idx (apply hash-map (mapcat vector coll-b (range)))
        expected (sort (set (concat coll-a coll-b)))
        hcoll-a (ctor coll-a bin-size)
        hdb (ctor coll-b bin-size)
        result (pfc/dict-seq (pfc/to-htfc (merger hcoll-a hdb)))]
    (= expected result)))

(defn combine [[both only-coll-a only-coll-b]]
  [(distinct (concat only-coll-a both))
   (distinct (concat only-coll-b both))])

(defspec merge-htfc-test
  100
  (prop/for-all
    [[coll-a coll-b] (gen/such-that
                       (fn [[coll-a coll-b]]
                         (and (> (count coll-a) 2)
                              (> (count coll-b) 2)))
                       (gen/fmap combine (gen/tuple (gen/vector gen-string)
                                                    (gen/vector gen-string)
                                                    (gen/vector gen-string))))
     bin-size-1 gen/s-pos-int]
    (check-merge htfc htfc-merge coll-a coll-b bin-size-1)))
