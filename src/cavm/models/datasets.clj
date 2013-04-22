(ns cavm.models.datasets
  (:require [cavm.h2 :as db])
  (:gen-class))

(defn datasets []
  (db/datasets))

(defn dataset-by-genes [id genes]
  (-> (db/select-scores)
      (db/for-experiment-named id)
      (db/with-genes genes)
      (db/do-select)))
