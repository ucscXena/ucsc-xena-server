(ns cavm.jdbc
  (:import [java.sql PreparedStatement])
  (:require [clojure.java.jdbc :as jdbc]))

(extend-protocol jdbc/ISQLParameter
  clojure.lang.Seqable
  (set-parameter [v ^PreparedStatement s ^long i]
    (.setObject s i (to-array v))))
