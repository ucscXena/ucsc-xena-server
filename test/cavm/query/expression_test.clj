(ns cavm.query.expression-test
  (:require [cavm.query.expression :as expr])
  (:use [cavm.query.functions :only [functions]])
  (:use clojure.core.matrix)
  (:use clojure.core.matrix.operators)
  (:refer-clojure :exclude  [* - + == /])
  (:use clojure.test))

(set-current-implementation :vectorz)

(deftest parse-constant
  (testing "parse constant"
    (let [f (expr/expression "5" {})]
      (is (== 5 f)))))

(deftest parse-sum
  (testing "parse sum"
    (let [f (expr/expression "(+ 5 2)" functions)]
      (is (== 7 f)))))

(deftest parse-var
  (testing "parse var"
    (let [f (expr/expression "x" {'x 2})]
      (is (== 2 f)))))

(deftest parse-var-sum
  (testing "parse var sum"
    (let [f (expr/expression "(+ x 2)" {'x 9} functions)]
      (is (== 11 f)))))

(deftest parse-fn
  (testing "parse fn"
    (let [f (expr/expression "((fn [x] x) 3)" {})]
      (is (== 3 f)))))

(deftest parse-fn-2
  (testing "parse fn 2"
    (let [f (expr/expression "((fn [x] (+ 1 x)) 3)" functions)]
      (is (== 4 f)))))

(deftest parse-fn-nested
  (testing "parse fn nested"
    (let [f (expr/expression "((fn [f x] (f x)) (fn [x] (+ 1 x)) 3)" functions)]
      (is (== 4 f)))))

(deftest parse-matrix-var-sum
  (testing "parse matrix var sum"
    (let [f (expr/expression "(+ x 2)" functions {'x (matrix [1 2 3])})]
      (is (== (matrix [3, 4, 5]) f)))))

(deftest parse-matrix-var-mult
  (testing "parse matrix var mult"
    (let [x (matrix [-1, 0, 1])
          y (matrix [1, 2, 3])
          f (expr/expression "(* x y 2)" functions {'x x 'y y})]
      (is (== (matrix [-2, 0, 6]) f)))))

(deftest parse-matrix-var-div
  (testing "parse matrix var div"
    (let [x (matrix [12, 10, 3])
          y (matrix [3, -2, 3])
          f (expr/expression "(/ x y)" functions {'x x 'y y})]
      (is (== (matrix [4, -5, 1]) f)))))

(deftest parse-matrix-var-div-twice
  (testing "parse matrix var div twice"
    (let [x (matrix [12, 10, 3])
          y (matrix [3, -2, 3])
          z (matrix [2, 1, -1])
          f (expr/expression "(/ x y z)" functions {'x x 'y y 'z z})]
      (is (== (matrix [2, -5, -1]) f)))))

(deftest parse-var-twice
  (testing "parse var twice"
    (let [f (expr/expression "(+ x x)" functions {'x 2})]
      (is (== 4 f)))))

(deftest parse-float-math
  (testing "math should be floating point"
    (let [f (expr/expression "(/ 5 2)" functions)]
      (is (== 2.5 f)))))

(deftest parse-float-math-vars
  (testing "math should be floating point with vars"
    (let [f (expr/expression "(/ x y)" functions, {'x 5 'y 2})]
      (is (== 2.5 f)))))

;; XXX bad function test
;; XXX bad atom test
;
(deftest parse-2d-matrix-var-sum
  (testing "parse 2d matrix var sum"
    (let [x (matrix [[1, 2, 3], [4, 5, 6]])
          f (expr/expression "(+ x 2)" functions {'x x})]
      (is (== (matrix [[3, 4, 5], [6, 7, 8]]) f)))))

(deftest parse-2d-matrix-var-diff
  (testing "parse 2d matrix var diff"
    (let [x (matrix [[1, 2, 3], [4, 5, 6]])
          z (matrix [1, 2, 3])
          f (expr/expression "(- x z)" functions {'x x 'z z})]
      (is (== (matrix [[0, 0, 0], [3, 3, 3]]) f)))))

(deftest parse-2d-matrix-mean
  (testing "parse matrix mean"
    (let [y (matrix [[1, 2, 3], [4, Float/NaN, 6]])
          f (expr/expression "(mean y 0)" functions {'y y})]
      (is (== (matrix [[2.5, 2, 4.5]]) f)))))

(deftest parse-2d-matrix-mean-second
  (testing "parse matrix mean second"
    (let [y (matrix [[1, 2, 3], [4, Float/NaN, 6]])
          f (expr/expression "(mean y 1)" functions {'y y})]
      (is (== (matrix [[2], [5]]) f)))))

(deftest parse-quote
  (testing "parse quote"
    (let [f (expr/expression "(quote (5))" {})]
      (is (= '(5) f)))))

(deftest parse-apply
  (testing "parse apply"
    (let [f (expr/expression "(apply + (quote (1 2 3)))" functions)]
      (is (== 6 f)))))

(deftest parse-if-false
  (testing "parse if false"
    (let [f (expr/expression "(if false 3 4)" {})]
      (is (== 4 f)))))

(deftest parse-if-true
  (testing "parse if true"
    (let [f (expr/expression "(if true 3 4)" functions)]
      (is (== 3 f)))))
