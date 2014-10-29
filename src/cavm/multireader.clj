(ns cavm.multireader
  ^{:author "Brian Craft"
    :doc "util for repeated sequential file reading"}
  (:require [clojure.java.io :as io]))


(defn multi-reader
  "Return object that can return multiple readers over source,
  closing all of them when the close method is invoked."
  [source]
  (let [open (atom [])]
    {:reader (fn [] (let [r (io/reader source)]
                      (swap! open conj r)
                      r))
     :close (fn [] (doseq [f @open]
                     (.close ^java.io.Reader f)))}))

(defmacro with-multi
  "Execute body with multi-reader bindings, ensuring that :close is invoked
  for all readers."
  [bindings & body]
  (cond
    (= (count bindings) 0) `(do ~@body)
    (symbol? (bindings 0)) `(let ~(subvec bindings 0 2)
                              (try
                                (with-multi ~(subvec bindings 2) ~@body)
                                (finally
                                  ((:close ~(bindings 0))))))
    :else (throw (IllegalArgumentException.
                   "with-multi only allows Symbols in bindings"))))
