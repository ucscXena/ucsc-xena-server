(ns cavm.HTFC-test
  (:require [clojure.test :as ct])
  (:require [clojure.test.check :as tc])
  (:require [clojure.test.check.generators :as gen])
  (:require [clojure.test.check.clojure-test :as tcct :refer [defspec]])
  (:require [clojure.test.check.properties :as prop])
  (:require [cavm.pfc :as pfc])
  (:import [cavm HTFC HFC]))

(def ^:dynamic *test-runs* 4000)

; using ascii string. Should clean it up for unicode.
(def gen-string (gen/not-empty gen/string-ascii))

(defspec cmp
  *test-runs*
  (prop/for-all
    [str-a gen-string
     str-b gen-string
     suff-a gen-string
     suff-b gen-string]
    (let [a (.getBytes (str str-a suff-a))
          lena (count (.getBytes ^String str-a))
          b (.getBytes (str str-b suff-b))
          lenb (count (.getBytes ^String str-b))
          c0 (compare str-a str-b)
          c1 (HTFC/cmp a lena b lenb)]
      (or (and (< c0 0) (< c1 0))
          (and (> c0 0) (> c1 0))
          (and (= c0 0) (= c1 0))))))

(defn check-join [ctor join req db bin-size-1]
  (let [req (sort req)
        db (vec (sort db))
        ; enforcing bin size of at least 2, to avoid uninteresting boundaries.
        bin-size (inc bin-size-1)
        db-idx (apply hash-map (mapcat vector db (range)))
        expected (map #(get db %) (map db-idx req))
        hreq (ctor req bin-size)
        hdb (ctor db bin-size)
        result (map #(get db %) (join hreq hdb))]
    (= expected result)))

(defn gen-distinct [gen]
  (gen/fmap #(distinct %) gen))

(defn db-req [[both only-req only-db]]
  [(distinct (concat only-req both))
   (distinct (concat only-db both))])

(defn htfc [strings bin-size]
  (with-bindings {#'cavm.pfc/*bin-size* bin-size}
    (pfc/compress-htfc strings )))

(defn htfc-join [a b]
  (HTFC/join a b))

(defn hfc [strings bin-size]
  (pfc/compress-hfc strings bin-size))

(defn hfc-join [a b]
  (HFC/join a b))

; XXX enforcing collections of at least two, to avoid some edge
; cases. Should fix these later.
(defspec htfc-join-test
   100
   (prop/for-all
     [[req db] (gen/such-that
                 (fn [[req db]]
                   (and (> (count req) 2)
                        (> (count db) 2)))
                 (gen/fmap db-req (gen/tuple (gen/vector gen-string)
                                             (gen/vector gen-string)
                                             (gen/vector gen-string))))
      bin-size-1 gen/s-pos-int]
     (check-join htfc htfc-join req db bin-size-1)))

(defspec hfc-join-test
   100
   (prop/for-all
     [[req db] (gen/such-that
                 (fn [[req db]]
                   (and (> (count req) 2)
                        (> (count db) 2)))
                 (gen/fmap db-req (gen/tuple (gen/vector gen-string)
                                             (gen/vector gen-string)
                                             (gen/vector gen-string))))
      bin-size-1 gen/s-pos-int]
     (check-join hfc hfc-join req db bin-size-1)))
