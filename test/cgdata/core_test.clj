(ns cgdata.core-test
  (:require [clojure.test :as ct])
  (:require [clojure.java.io :as io])
  (:require [cgdata.core :as cgdata]))

(def cwd (.getCanonicalFile (io/file ".")))

;
; references
;

(ct/deftest test-reference-samedir
  (ct/is (=
          {":a" "one/two/ack"}
          (cgdata/references cwd "one/two/three" {":a" "ack"}))))

(ct/deftest test-reference-parentdir
  (ct/is (=
          {":a" "one/ack"}
          (cgdata/references cwd "one/two/three" {":a" "../ack"}))))

(ct/deftest test-reference-absolute-ref
  (ct/is (=
          {":a" "foo/ack"}
          (cgdata/references cwd "one/two/three" {":a" "/foo/ack"}))))

(ct/deftest test-reference-top-of-path
  (ct/is (=
          {":a" "ack"}
          (cgdata/references cwd "one/two/three" {":a" "../../ack"}))))

(ct/deftest test-reference-outside-path
  (ct/is (thrown?
           IllegalArgumentException
           (cgdata/references cwd "one/two/three" {":a" "../../../ack"}))))
