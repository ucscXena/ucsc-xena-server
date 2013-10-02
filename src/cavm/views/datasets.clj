(ns cavm.views.datasets
  (:use [noir.core])
  (:use [noir.request])
  (:use [clojure.core.matrix])
  (:require [cavm.models.datasets :as d])
  (:require [cavm.query.expression :as expr])
  (:require [cavm.query.functions :as f])
  (:require [cavm.query.sources :as sources])
  (:require [noir.response :as response])
  (:require [ring.util.codec :as codec])
  (:require [clojure.edn :as edn])
  (:require [clojure.data.json :as json])
  (:require [cavm.h2 :as h2])
  (:gen-class))

(defpage [:get "/datasets"] []
         (response/json (d/datasets)))

; XXX temporary alias, to interoperate with client
(defpage [:get "/tracks"] []
  (response/json (d/datasets)))

(defpage [:options [":any" :any #".*"]] []
  (response/empty))

(defn genes-decode [genes]
  (map codec/url-decode (clojure.string/split genes #",")))

(defpage [:get ["/datasets/:id" :id #".+"]] {:keys [id genes]}
  (response/json (d/dataset-by-genes id (genes-decode genes))))

(defpage [:get ["/query/:query" :query #".+"]] {:keys [query]}
  (let [{db :db} (ring-request)]
    (response/json (d/query db (edn/read-string query)))))

(defpage [:get ["/query-p/:query" :query #".+"]] {:keys [query]}
  (let [{db :db} (ring-request)]
    (response/content-type "application/json; charset=utf-8"
                           (binding [clojure.pprint/*print-right-margin* 200]
                             (with-out-str (json/pprint (d/query db (edn/read-string query)) :escape-slash false))))))

(defn samples-for-attr [attr]
  {:select [[:experiments.name "experiment"] :exp_samples.name]
   :from [:exp_samples]
   :left-join [:experiments [:= :exp_samples.experiments_id :experiments.id]]
   :where [:in :experiments.id
           {:select [:experiments.id]
            :modifiers [:distinct]
            :from [:features]
            :left-join [:probes [:= :features.probes_id :probes.id]
                        :experiments [:= :probes.eid :experiments.id]]
            :where [:= :features.longTitle attr]}]})

; XXX This is probably not the fastest way to do
; this.
(defn- boolean-matrix [m pred]
  (map pred (vec m)))

(defn- filter-matches [samples matches]
  (->> (map vector samples matches)
       (filter second)
       (map first)))

(defn attr-filter [attr pred]
  (->> attr
       (samples-for-attr)
       (h2/run-query)
       (group-by :EXPERIMENT)
       (map (fn [[exp samps]]
              {'table exp
               'columns [attr]
               'samples (map :NAME samps)}))
       (map #(assoc % 'data (sources/read-symbols [%])))
       (map #(assoc % 'samples
                    (filter-matches ('samples %)
                                    (boolean-matrix
                                      (first ('data %)) pred))))
       (mapcat #(map (fn [exp samp]
                         {:experiment exp
                          :sample samp}) (repeat ('table %)) ('samples %)))))

(defn filter-attr [samples attr pred]
  (->> samples
       (group-by :EXPERIMENT)
       (map (fn [[exp samps]]
              {'table exp
               'columns [attr]
               'samples (map :NAME samps)}))
       (map #(assoc % 'data (sources/read-symbols [%])))
       (map #(assoc % 'samples
                    (filter-matches ('samples %)
                                    (boolean-matrix
                                      (first ('data %)) pred))))
       (mapcat #(map (fn [exp samp]
                         {:experiment exp
                          :sample samp}) (repeat ('table %)) ('samples %)))))

(def functions
  {'fetch sources/read-symbols
   'query h2/run-query
   'filter filter-attr})

; XXX The call to read-symbols needs a list of sources, I think.
(defpage [:get ["/data/:expression" :expression #".+"]] {:keys [expression]}
  (h2/with-db (:db (ring-request))
    (response/json (map vec (expr/expression expression f/functions functions)))))
