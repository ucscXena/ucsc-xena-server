(ns cavm.version
   (:gen-class))

(defn version []
  (-> cavm.version .getPackage .getImplementationVersion))
