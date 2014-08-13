(ns
  ^{:doc "Shim to let us run schemaSpy, with lein fetching the jar."}
  schemaspy
  (:import net.sourceforge.schemaspy.Main))

(defn -main [& args]
  (Main/main (into-array String args)))
