(ns cavm.query.sources)

; collect data in order of columns
(defn collect [{data 'data columns 'columns}]
  (map data columns))

; resolve requests, returning seq of data.
(defn read-symbols [sources reqs]
  (let [op (reduce #(%2 %1) reqs sources)]
    (apply concat (map collect op))))
