(ns cavm.views.datasets
  (:use [clojure.core.matrix])
  (:require [cavm.models.datasets :as d])
  (:require [cavm.query.expression :as expr])
  (:require [cavm.query.functions :as f])
  (:require [cavm.query.sources :as sources])
  (:require [ring.util.codec :as codec])
  (:require [clojure.edn :as edn])
  (:require [clojure.data.json :as json])
  (:require [cavm.h2 :as h2])
  (:require [liberator.core :refer [defresource]])
  (:require [compojure.core :refer [defroutes ANY]])
  (:gen-class))

; disabling old noir pages for now.
(defmacro defpage [& args])

(defpage [:get "/datasets"] []
         (response/json (d/datasets)))

; XXX temporary alias, to interoperate with client
(defpage [:get "/tracks"] []
  (response/json (d/datasets)))

; XXX for CORS?
(defpage [:options [":any" :any #".*"]] []
  (response/empty))

(defn genes-decode [genes]
  (map codec/url-decode (clojure.string/split genes #",")))

(defpage [:get ["/datasets/:id" :id #".+"]] {:keys [id genes]}
  (response/json (d/dataset-by-genes id (genes-decode genes))))

(defpage [:get ["/query/:query" :query #".+"]] {:keys [query]}
  (let [{db :db} (ring-request)]
    (response/json (d/query db (edn/read-string query)))))

; pretty-print query
(defpage [:get ["/query-p/:query" :query #".+"]] {:keys [query]}
  (let [{db :db} (ring-request)]
    (response/content-type "application/json; charset=utf-8"
                           (binding [clojure.pprint/*print-right-margin* 200]
                             (with-out-str (json/encode (d/query db (edn/read-string query)) {:pretty true}))))))

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

;
; Cheshire encoders. Not using them any more.
;

;; Add a json encoder for primitive float arrays, since that's what
;; we get back from the expression engine.
;(defn encode-array
;  "Encode a primitive array to the json generator."
;  [arr jg]
;  (json/to-json (seq arr) jg))
;
;(json/add-encoder (Class/forName "[F") encode-array)
;; Mike suggests extending mikera.arrayz.INDArray to hit all
;; core.matrix types. Leaving these for now.
;(json/add-encoder mikera.vectorz.impl.ArraySubVector encode-array)
;(json/add-encoder mikera.matrixx.Matrix encode-array)

;
; clojure.data.json encoders
;

(defn write-array
  "Write a core.matrix array to json"
  [arr out]
  (json/-write (seq arr) out))

(extend mikera.arrayz.INDArray json/JSONWriter {:-write write-array})

; (json/json-str (float-array [1 2 3]))
; (json/json-str (double-array [1 2 3]))
; (json/json-str (matrix (double-array [1 2 3])))
; (json/json-str (matrix [(double-array [1 2 3])]))

;
; liberator handler for simple values
;

(defn- simple-as-response [this ctx]
  (liberator.representation/as-response (str this) ctx))

(extend Double liberator.representation/Representation
  {:as-response simple-as-response})

; (liberator.representation/as-response 1.0 {:representation {:media-type "application/json"}})
; (liberator.representation/as-response 1.0 {:representation {:media-type "application/edn"}})

;
;
;

; XXX map vec is being used to convert the [F so core.matrix can work on them.
; double-array might be a faster solution.
(def functions
  {'fetch #(map vec (sources/read-symbols %))
   'query h2/run-query
   'filter filter-attr})

(defresource expression [exp]
  :available-media-types ["application/json" "application/edn"]
  :handle-ok (fn [{{db :db} :request}]
               (h2/with-db db
                 (expr/expression exp f/functions functions))))

; XXX add the custom patter #".+" to avoid nil, as above?
(defroutes routes
  (ANY "/data/:exp" [exp] (expression exp)))
