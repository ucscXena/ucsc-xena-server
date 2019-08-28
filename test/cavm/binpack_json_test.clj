(ns cavm.binpack-json-test
  (:require [cavm.binpack-json :as binpack-json])
  (:require [clojure.test :refer [deftest testing is]])
  (:require [clojure.test.check :as tc])
  (:require [clojure.test.check.generators :as gen])
  (:require [clojure.test.check.clojure-test :as tcct :refer [defspec]])
  (:require [clojure.test.check.properties :as prop]))

(def ^:dynamic *test-runs* 400)

; 1.0 00 00 80 3f
; 2.0 00 00 00 40
; 3.0 00 00 40 40
; 4.0 00 00 80 40


(deftest basic-test
  (testing "encode"
    (let [buff (binpack-json/write-buff {:foo (float-array [1.0])})
          txt "{\"foo\":{\"$type\":\"ref\",\"value\":{\"$bin\":0}}}" ; len = 42
          null [0] ; txt + null len = 43
          padding [0] ; pad to 4 byte boundary
          len [0x04 0x00 0x00 0x00]
          f10 [0x00 0x00 0x80 0x3f] ; 1.0
          expected (byte-array (concat (.getBytes txt) null padding len f10))]
      (is (java.util.Arrays/equals buff expected))))
  (testing "encode four"
    (let [buff (binpack-json/write-buff {:foo [(float-array [1.0]) (float-array [2.0]) (float-array [3.0]) (float-array [4.0])]})
          txt "{\"foo\":[{\"$type\":\"ref\",\"value\":{\"$bin\":0}},{\"$type\":\"ref\",\"value\":{\"$bin\":1}},{\"$type\":\"ref\",\"value\":{\"$bin\":2}},{\"$type\":\"ref\",\"value\":{\"$bin\":3}}]}" ; len = 149
          null [0] ; txt + null len = 150
          padding [0 0] ; pad to 4 byte boundary
          len0 [0x04 0x00 0x00 0x00]
          f10 [0x00 0x00 0x80 0x3f] ; 1.0
          len1 [0x04 0x00 0x00 0x00]
          f20 [0x00 0x00 0x00 0x40] ; 2.0
          len2 [0x04 0x00 0x00 0x00]
          f30 [0x00 0x00 0x40 0x40] ; 3.0
          len3 [0x04 0x00 0x00 0x00]
          f40 [0x00 0x00 0x80 0x40] ; 4.0
          expected (byte-array (concat (.getBytes txt) null padding len0 f10 len1 f20 len2 f30 len3 f40))]
;      (println (into [] buff))
;      (println (into [] expected))
      (is (java.util.Arrays/equals buff expected)))))

;(clojure.test/run-tests)
