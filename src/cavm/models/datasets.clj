(ns cavm.models.datasets
  (:require [cavm.h2 :as db])
  (:gen-class))

(defn query [db q]
  (db/with-db db
    (db/run-query q)))
