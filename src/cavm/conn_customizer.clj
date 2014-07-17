(ns cavm.conn-customizer)

(def acquire-watchers (atom []))
(defn watch-acquire [f]
  (swap! acquire-watchers conj f))

(def destroy-watchers (atom []))
(defn watch-destroy [f]
  (swap! destroy-watchers conj f))

(defn- -onAcquire [this conn id]
  (doseq [f @acquire-watchers]
    (f conn)))

(defn- -onDestroy [this conn id]
  (doseq [f @destroy-watchers]
    (f conn)))

(gen-class :name conn_customizer
           :extends com.mchange.v2.c3p0.AbstractConnectionCustomizer)
