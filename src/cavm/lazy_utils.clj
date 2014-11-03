(ns
  ^{:author "Brian Craft"
    :doc "Utilities for more laziness from clojure data structures."}
  cavm.lazy-utils)

; When processing datasets that should be sorted by row, we have to hold
; entire columns in memory during the sort. That leaves us with a vector of
; rows. If we process that vector via a seq, the whole vector remains in memory.
; This is problematic for long datasets, when it is expensive to hold
; multiple columns in memory. This wrapper will return seq over a vector that
; does not hold the head of the vector. This feel like re-inventing queues,
; however I'm not sure how to take advantage of queues. PersistentQueue will
; not release its head, either. Probably need a loop over an atom, or something,
; but then we'd need a back-pressure solution, instead of relying on db write
; speed to limit seq consumption.
(defn consume-vec
  "Return a seq of a vector which allows the beginning of the
  vector to be garbage collected as the vector is consumed."
  [v]
  (let [c-vec (fn c-vec [v]
                (when (seq v)
                  (cons (peek v) (lazy-seq (c-vec (pop v))))))]
    (c-vec (into (empty v) (rseq v)))))

; core mapcat will evaluate the first element of the first
; chunk of collections. In the cavm loader this forces 32 columns
; to be loaded at once, which runs us out of memory.
(defn lazy-mapcat
  "mapcat, evaluating one collection at a time."
  [f coll]
  (lazy-seq
    (when-let [s (seq coll)]
      (concat (f (first s)) (lazy-mapcat f (rest s))))))
