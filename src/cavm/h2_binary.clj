(ns
  ^{:author "Brian Craft"
    :doc "Experimental custom aggregate function for H2, that will
         group-concatenate binary blob objects."}
  cavm.h2-binary)

; low-level byte array concat
(defn- bconcat [& arrays]
  (let [sizes (map count arrays)
        sizes-r (vec (reductions + sizes))
        offsets (cons 0 (drop-last sizes-r))
        total (last sizes-r)
        out (make-array (.getComponentType (class (first arrays))) total)]
    (dorun (map #(System/arraycopy %2 0 out %1 %3) offsets arrays sizes))
    out))

(defn bconcat-init-state []
  [[] (atom [])])

(def ^:private allowed
  #{java.sql.Types/BINARY
    java.sql.Types/VARBINARY
    java.sql.Types/LONGVARBINARY
    java.sql.Types/ARRAY})

; AggregateFunction interface methods.

(defn bconcat-init [this conn])

(defn bconcat-getType [this inputTypes]
  (when-not (= 1 (count inputTypes))
    (throw (java.lang.IllegalArgumentException. "Wrong number of arguments.")))
  (let [[t] inputTypes]
    (when-not (contains? allowed t)
      (throw (java.lang.IllegalArgumentException. "Wrong argument type.")))
    t))

(defn bconcat-getResult [this]
  (apply bconcat @(.state this)))

(defn bconcat-add [this value]
  (swap! (.state this) conj value))

(gen-class :name group_bconcat
           :prefix "bconcat-"
           :init "init-state"
           :state "state"
           :implements [org.h2.api.AggregateFunction])
