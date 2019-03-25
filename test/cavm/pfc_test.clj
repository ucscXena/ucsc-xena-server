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
  (:import [java.nio ByteBuffer])
  )

(comment (deftest basic-test
           (testing "fixed set"
             (let [in ["one" "two" "three" "four" "five" "six"]
                   dict (pfc/serialize-htfc (pfc/compress-htfc in 5))]
               (with-open [file (java.io.FileOutputStream. "test.bin")]
                 (.write file dict))
               (let [{out :out} (sh "node" "isomorphic.js" "test.bin")
                     res (json/read-str out)]
                 (is (= (set in) (set res))))))
           (comment (testing "fixed set 2"
                      ; check handling of zero-length front-coding, caused by
                      ; repeated string, in encode-bytes.
                      (let [in ["one" "one" "two" "three" "four" "five" "six"]
                            dict (pfc/serialize-htfc (pfc/compress-htfc in 5))]
                        (with-open [file (java.io.FileOutputStream. "test.bin")]
                          (.write file dict))
                        (let [{out :out} (sh "node" "isomorphic.js" "test.bin")
                              res (json/read-str out)]
                          (is (= (set in) (set res)))))))
           (testing "toil"
             (let [in (json/read-str (slurp "toil.json"))
                   dict (pfc/serialize-htfc (pfc/compress-htfc in 5))]
               (with-open [file (java.io.FileOutputStream. "test.bin")]
                 (.write file dict))
               (let [{out :out} (sh "node" "isomorphic.js" "test.bin")
                     res (json/read-str out)]
                 (is (= (set in) (set res))))))))

(defn dict-to-vec [dict]
  (into []
        (pfc/uncompress-dict (pfc/htfc-offsets (doto (ByteBuffer/wrap dict)
                                         (.order java.nio.ByteOrder/LITTLE_ENDIAN))))))

(deftest basic-test-clj
  (testing "fixed set"
    (let [in ["one" "two" "three" "four" "five" "six"]
          dict (pfc/serialize-htfc (pfc/compress-htfc in 5))
          out (dict-to-vec dict)]
      (is (= (sort in) out))))
  (testing "fixed set 2"
    ; check handling of zero-length front-coding, caused by
    ; repeated string, in encode-bytes.
    (let [in ["one" "one" "two" "three" "four" "five" "six"]
          dict (pfc/serialize-htfc (pfc/compress-htfc in 5))
          out (dict-to-vec dict)]
      (is (= (sort in) out))))
  ; where to get a large test set w/o committing all this garbage?
  ; maybe compress it? Or fetch & cache? Or compress with htfc?
  (testing "toil"
    (let [in (json/read-str (slurp "toil.json"))
          dict (pfc/serialize-htfc (pfc/compress-htfc in 256))
          out (dict-to-vec dict)]
      (is (= (sort in) out)))))
