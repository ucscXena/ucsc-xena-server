(ns cavm.views.datasets
  (:use [clojure.core.matrix])
  (:require [cavm.db :as cdb])
  (:require [cavm.query.expression :as expr])
  (:require [cavm.query.functions :as f])
  (:require [ring.util.codec :as codec])
  (:require [ring.util.response :as response ])
  (:require [ring.util.request :refer [body-string] ])
  (:require [clojure.edn :as edn])
  (:require [cavm.json :as json])
  (:require cavm.edn)
  (:require [cavm.h2 :as h2])
  (:require [liberator.core :refer [defresource]])
  (:require [compojure.core :refer [defroutes ANY GET POST]])
  (:require [compojure.route :refer [not-found]])
  (:require [clojure.java.io :as io])
  (:require [taoensso.timbre.profiling :as profiling :refer [p profile]])
  (:require [cavm.fs-utils :refer [docroot-path]])
  (:require [honeysql.types :as hsqltypes])
  (:import [org.h2.jdbc JdbcBlob])
  (:import [cavm HTFC])
  (:require [cavm.binpack-json :as bj]))

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

; Override the liberator json methods. We want to customize the floating-point
; format, which requires a NumberFormat, which is not thread-safe. So, we have
; to allocate one before encoding the response.
(defmethod liberator.representation/render-map-generic "application/json" [data context]
  (json/write-str data))

(defmethod liberator.representation/render-seq-generic "application/json" [data context]
  (json/write-str data))

(defmethod liberator.representation/render-map-generic "application/binpack-json" [data context]
  (bj/write-buff data))

(defmethod liberator.representation/render-seq-generic "application/binpack-json" [data context]
  (bj/write-buff data))

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

(defn- bytes-as-response [this ctx]
  (liberator.representation/as-response (clojure.java.io/input-stream this) ctx))

(extend (Class/forName "[B") liberator.representation/Representation
  {:as-response bytes-as-response})

; XXX somehow liberator won't call the correct serialization method
; on things that aren't map or vector. What is going on? Same problem
; as simple types, but here we're assuming the return should be binpack.
; See this:
; https://groups.google.com/forum/#!searchin/clojure-liberator/from$3Ame%7Csort:date/clojure-liberator/TzhfJlml3Sw/GMbWEbRRxyEJ
; There might be some way to dispatch to liberator serialization, instead of
; repeating that here.
(defn blob-as-response [this ctx]
  (bytes-as-response (bj/write-buff this) ctx))

(extend JdbcBlob liberator.representation/Representation
  {:as-response blob-as-response})

(defn htfc-as-response [this ctx]
  (bytes-as-response (bj/write-buff (:buff this)) ctx))

(extend HTFC liberator.representation/Representation
  {:as-response htfc-as-response})

(extend (Class/forName "[F") liberator.representation/Representation
  {:as-response blob-as-response})

(extend (Class/forName "[D") liberator.representation/Representation
  {:as-response blob-as-response})

; (liberator.representation/as-response 1.0 {:representation {:media-type "application/json"}})
; (liberator.representation/as-response 1.0 {:representation {:media-type "application/edn"}})
; (liberator.representation/as-response 1 {:representation {:media-type "application/edn"}})
; (liberator.representation/as-response (Integer. 1) {:representation {:media-type "application/edn"}})
; (liberator.representation/as-response (Float. 1.0) {:representation {:media-type "application/edn"}})

;
;

; XXX map vec is being used to convert the [F so core.matrix can work on them.
; double-array might be a faster solution. Would really like core.matrix to
; convert as necessary. Is there a protocol we can extend?
; Have to drop this for binary support.
(defn functions [db]
  {'fetch #(p ::fetch (apply concat (cdb/fetch db %)))
   'xena-query #(p ::query (cdb/column-query db %))
   'query #(p ::query (cdb/run-query db %))})

(def edn-read-str
  (partial edn/read-string {:readers {'sql/call hsqltypes/read-sql-call}}))

(defn parse-exp [headers exp]
  (cond
    (= (headers "content-type") "application/binpack-edn") (bj/parse edn-read-str exp)
    :default (edn-read-str (String. exp))))

(defresource expression [exp]
  :allowed-methods [:post :get]
  :available-media-types ["application/json" "application/edn" "application/binpack-json"]
  :new? (fn [req] false)
  :respond-with-entity? (fn [req] true)
  :multiple-representations? (fn [req] false)
  :handle-ok (fn [{{db :db headers :headers} :request}]
               (profile :trace ::expression (expr/expression (parse-exp headers exp) f/functions (functions @db)))))

(defn- is-local? [ip]
  (or (= ip "0:0:0:0:0:0:0:1")
      (= ip "::1")
      (= ip "127.0.0.1")))

; Return immediately, queuing a loader job. Returns the
; state of the queue after adding the last file in the request.
(defn loader-request [ip loader files always delete]
  (when (is-local? ip)
    (last (map #(loader % {:always (boolean always) :delete (boolean delete)})
                (if (coll? files) files [files])))))

(defn upload-files [ip loader docroot file append]
  (when (is-local? ip)
    (let [files (if (vector? file) file [file])]
      (doseq [{:keys [bytes filename]} files]
        ; XXX catch errors & return http code
        (let [dest (docroot-path docroot filename)]
          (with-open [w (clojure.java.io/output-stream dest :append append)]
            (.write w ^bytes bytes)))))
    "ok"))

(defn body-bytes [{body :body}]
  (with-open [xout (java.io.ByteArrayOutputStream.)]
    (clojure.java.io/copy body xout)
    (.toByteArray xout)))

; XXX add the custom pattern #".+" to avoid nil, as above?
(defroutes routes
  (POST ["/upload/"] [file append :as req]
        (let [{loader :loader docroot :docroot ip :remote-addr} req]
          (upload-files ip loader docroot file append)))
  (GET ["/download/:dataset" :dataset #".+"] [dataset :as {docroot :docroot}]
       (let [resp (response/file-response dataset {:root docroot :index-files? false})]
         (if (re-find #"\.gz$" dataset)
           ; set Content-Encoding to coerce wrap-gzip to pass this w/o further compression.
           ; Otherwise .gz files are gzipped twice.
           (assoc-in resp [:headers "Content-Encoding"] "identity")
           resp)))
  (GET "/" [:as req] (response/redirect
                       (str "https://xenabrowser.net/datapages/?hub="
                            (name (:scheme req)) "://"
                            (:server-name req) ":" (:server-port req))))
  (GET "/data/:exp" [exp] (expression exp))
  (POST "/data/" r (expression (body-bytes r)))
  (POST "/update/" [file always delete :as {ip :remote-addr loader :loader}]
        (json/write-str (loader-request ip loader file always delete)))
  (GET "/load-queue/" [:as {load-queue :load-queue}] (json/write-str (deref load-queue)))
  (not-found "not found"))
