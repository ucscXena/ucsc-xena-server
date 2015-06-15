(ns cavm.h2-test
  (:require [cavm.h2 :as h2])
  (:require [clojure.test :as ct])
  (:require [clojure.test.check :as tc])
  (:require [clojure.test.check.generators :as gen])
  (:require [clojure.test.check.clojure-test :as tcct :refer [defspec]])
  (:require [clojure.test.check.properties :as prop]))

(def ^:dynamic *test-runs* 4000)

(defspec concat-field-bins
  *test-runs*
  (prop/for-all
    [data (gen/such-that not-empty (gen/vector (gen/choose -100 100)))
     p gen/s-pos-int]
    (let [d (map #(-> {:name "foo"
                       :scores (h2/score-encode %1)
                       :i %2})
              (partition-all p data) (range))]
      (= (map float data) (into [] (h2/concat-field-bins d))))))
