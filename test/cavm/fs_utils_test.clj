(ns cavm.fs-utils-test
  (:require [clojure.test :as ct])
  (:require [clojure.java.io :as io]) 
  (:require [cavm.fs-utils :refer [normalized-path relativize]]))

(def cwd (.getCanonicalFile (io/file ".")))

;
; normalize
;


(ct/deftest test-normalize-relative-path
  (ct/is (=
          (str (io/file cwd "one/file"))
          (str (normalized-path "one/file")))))

(ct/deftest test-normalize-relative-path-dotdot
  (ct/is (=
          (str (io/file cwd "file"))
          (str (normalized-path "one/../file")))))

(ct/deftest test-normalize-absolute
  (ct/is (=
          "/two"
          (str (normalized-path "/one/../two")))))

(ct/deftest test-normalize-absolute2
  (ct/is (=
          "/two"
          (str (normalized-path "/one/../../two")))))

;
; relativize
;


(ct/deftest test-from-root1
  (ct/is (=
          "one/file"
          (str (relativize "docroot" "docroot/one/file")))))

(ct/deftest test-from-root2
  (ct/is (thrown?
           IllegalArgumentException
           (relativize "docroot" "docroot2/file"))))

