(ns cgdata.core-test
  (:require [clojure.test :as ct])
  (:require [clojure.java.io :as io])
  (:require [cgdata.core :as cgdata]))

(def cwd (.getCanonicalFile (io/file ".")))

;
; normalize
;


(ct/deftest test-normalize-relative-path
  (ct/is (=
          (str (io/file cwd "one/file"))
          (str (cgdata/normalized-path "one/file")))))

(ct/deftest test-normalize-relative-path-dotdot
  (ct/is (=
          (str (io/file cwd "file"))
          (str (cgdata/normalized-path "one/../file")))))

(ct/deftest test-normalize-absolute
  (ct/is (=
          "/two"
          (str (cgdata/normalized-path "/one/../two")))))

(ct/deftest test-normalize-absolute2
  (ct/is (=
          "/two"
          (str (cgdata/normalized-path "/one/../../two")))))

;
; relativize
;


(ct/deftest test-from-root1
  (ct/is (=
          "one/file"
          (str (cgdata/relativize "docroot" "docroot/one/file")))))

(ct/deftest test-from-root2
  (ct/is (thrown?
           IllegalArgumentException
           (cgdata/relativize "docroot" "docroot2/file"))))


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
