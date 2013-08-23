(ns cavm.views.datasets
  (:use [noir.core])
  (:require [cavm.models.datasets :as d])
  (:require [cavm.query.sources :as sources])
  (:require [noir.response :as response])
  (:require [ring.util.codec :as codec])
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

; XXX The call to read-symbols needs a list of sources, I think.
(defpage [:get ["/data/:expression" :expression #".+"]] {:keys [expression]}
  (response/json (sources/read-symbols expression)))
