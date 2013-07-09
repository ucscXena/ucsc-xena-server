(ns cavm.views.datasets
  (:use [noir.core])
  (:require [cavm.models.datasets :as d])
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
  (map #(codec/url-decode %) (clojure.string/split genes #",")))

(defpage [:get ["/datasets/:id" :id #".+"]] {:keys [id genes]}
  (response/json (d/dataset-by-genes id (genes-decode genes))))
