(ns cavm.binner-test
  (:require [cavm.binner :refer [calc-bin overlapping-bins]])
  (:require [cavm.h2 :as h2])
  (:require [clojure.test :refer [deftest testing is]])
  (:require [clojure.test.check :as tc])
  (:require [clojure.test.check.generators :as gen])
  (:require [clojure.test.check.clojure-test :as tcct :refer [defspec]])
  (:require [clojure.test.check.properties :as prop]))

(def ^:dynamic *test-runs* 400)

(deftest basic-test
  (testing "Some bins"
    (is (= 4681 (calc-bin 100 1000)))
    (is (= 4684 (calc-bin 400000 500000)))
    (is (= 1061 (calc-bin 499900000 500000000)))
    (is (= 585 (calc-bin 100000 500000)))))

(def gen-position (gen/resize (* 1024 1024) gen/s-pos-int))

; Assert that, for a given interval, and a set of queries that
; overlap the interval, overlapping-bins will always return a set
; of bins that includes the bin of the interval.
(defspec overlap-test
  *test-runs*
  (prop/for-all
    [intvl (gen/tuple gen-position gen-position)
     query-intvls (gen/such-that not-empty
                                 (gen/resize 1000 (gen/vector
                                                    (gen/tuple gen-position
                                                               gen-position))))]
    (let [[start end] intvl
          bin (apply calc-bin (sort intvl))
          query-intvls (map (comp vec sort) query-intvls)
          overlapping (filter (fn [[s e]] (and (<= start e) (>= end s))) query-intvls)]
      (every? (fn [q]
                (let [bins (apply overlapping-bins q)]
                  (some (fn [[s e]] (and (<= bin e) (>= bin s))) bins)))
              overlapping))))
