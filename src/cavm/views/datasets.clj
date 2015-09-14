(ns cavm.views.datasets
  (:use [clojure.core.matrix])
  (:require [cavm.db :as cdb])
  (:require [cavm.query.expression :as expr])
  (:require [cavm.query.functions :as f])
  (:require [ring.util.codec :as codec])
  (:require [ring.util.response :as response ])
  (:require [ring.util.request :refer [body-string] ])
  (:require [clojure.edn :as edn])
  (:require [clojure.data.json :as json])
  (:require [cavm.h2 :as h2])
  (:require [liberator.core :refer [defresource]])
  (:require [compojure.core :refer [defroutes ANY GET POST]])
  (:require [compojure.route :refer [not-found]])
  (:require [clojure.java.io :as io])
  (:import (java.io PrintWriter))
  (:require [taoensso.timbre.profiling :as profiling :refer [p profile]])
  (:gen-class))

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
  (json/-write (seq arr) out)) ; XXX is the seq here expensive? Using vec fails.


(defn- write-floating-point [x ^PrintWriter out]
  (.print out
          (if (Double/isNaN x)
            "\"NaN\""
            (format "%.6g" x))))

(extend mikera.arrayz.INDArray json/JSONWriter {:-write write-array})
(extend java.lang.Float json/JSONWriter {:-write write-floating-point})
(extend java.lang.Double json/JSONWriter {:-write write-floating-point})

; (json/json-str (float-array [1 2 3]))
; (json/json-str (double-array [1 2 3]))
; (json/json-str (matrix (double-array [1 2 3])))
; (json/json-str (matrix [(double-array [1 2 3])]))

; (json/json-str Float/NaN)
; (json/json-str [1.2 Float/NaN])
; (json/json-str (float-array [1 Float/NaN 3]))
; (json/json-str (double-array [1 Float/NaN 3]))
; (json/json-str (matrix (double-array [1 Double/NaN 3])))
; (json/json-str (matrix [(double-array [1 Double/NaN 3])]))

;
; liberator handler for simple values
;

(defn- simple-as-response [this ctx]
  (liberator.representation/as-response (str this) ctx))

(extend Number liberator.representation/Representation
  {:as-response simple-as-response})

; (liberator.representation/as-response 1.0 {:representation {:media-type "application/json"}})
; (liberator.representation/as-response 1.0 {:representation {:media-type "application/edn"}})
; (liberator.representation/as-response 1 {:representation {:media-type "application/edn"}})
; (liberator.representation/as-response (Integer. 1) {:representation {:media-type "application/edn"}})
; (liberator.representation/as-response (Float. 1.0) {:representation {:media-type "application/edn"}})

;
;
; collect data in order of columns
; copied from cavm.query.sources
(defn- collect [{:keys [data columns]}]
  (map data columns))

; XXX map vec is being used to convert the [F so core.matrix can work on them.
; double-array might be a faster solution. Would really like core.matrix to
; convert as necessary. Is there a protocol we can extend?
(defn functions [db]
  {'fetch #(p ::fetch (map vec (apply concat (map collect (cdb/fetch db %))))) ; XXX concat? see below
   'xena-query #(p ::query (cdb/column-query db %))
   'query #(p ::query (cdb/run-query db %))})

; XXX concat is copied from cavm.query.sources. This is something to do with
; pulling from differen sources, or different datasets, and returning a single
; set of vectors. Not sure how it was supposed to work.

(defresource expression [exp]
  :allowed-methods [:post :get]
  :available-media-types ["application/json" "application/edn"]
  :new? (fn [req] false)
  :respond-with-entity? (fn [req] true)
  :multiple-representations? (fn [req] false)
  :handle-ok (fn [{{db :db} :request}]
               (profile :trace ::expression (expr/expression exp f/functions (functions db)))))

(defn- is-local? [ip]
  (or (= ip "0:0:0:0:0:0:0:1")
      (= ip "::1")
      (= ip "127.0.0.1")))

; Return immediately, queuing a loader job.
(defn loader-request [ip loader files always delete]
  (when (is-local? ip)
    (future (doseq [f (if (coll? files) files [files])]
              (loader f {:always (boolean always) :delete (boolean delete)})))
    "ok"))

; XXX add the custom pattern #".+" to avoid nil, as above?
(defroutes routes
  (GET ["/download/:dataset" :dataset #".+"] [dataset :as {docroot :docroot}]
       (response/file-response dataset {:root docroot :index-files? false}))
  (GET "/data/:exp" [exp] (expression exp))
  (POST "/data/" r (expression (body-string r)))
  (POST "/update/" [file always delete :as {ip :remote-addr loader :loader}]
        (loader-request ip loader file always delete))
  (not-found "not found"))
