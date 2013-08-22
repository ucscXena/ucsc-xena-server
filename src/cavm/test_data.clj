(ns cavm.test-data
  (:require [clojure.string :refer [join]]))

(defn cell [m n i j]
  (- (* 5 (/ (+ i (* m j)) (* m n))) 2.5))

(defn- header [m]
  (join "\t" (cons "probes" (range m))))

(defn- row [m n i]
  (join "\t" (cons i (map (partial cell m n i) (range m)))))

(defn matrix
  "Create test matrix with m samples, n probes"
  [m n]
  (cons (header m) (map (partial row m n) (range n))))
