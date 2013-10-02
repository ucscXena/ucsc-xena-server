(ns cavm.query.sources)

(def ^:private sources (atom {}))

(defn register [id handler]
  (swap! sources assoc id handler))

; collect data in order of columns
(defn- collect [{data 'data columns 'columns}]
  (map data columns))

; resolve requests, returning seq of data.
(defn read-symbols [reqs]
  (let [op (reduce #(%2 %1) reqs (vals @sources))]
    (apply concat (map collect op))))
