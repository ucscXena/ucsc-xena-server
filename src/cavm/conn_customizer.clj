(ns
  ^{:author "Brian Craft"
    :doc "Utilities for receiving notification of c3p0
         connection acquizition and destruction. c3p0 is
         a database connection pool library.

         Used internally for managing prepared statements."}
  cavm.conn-customizer)

(def ^:private acquire-watchers (atom []))
(defn watch-acquire
  "Add a callback to be invoked on connection acquizition."
  [f]
  (swap! acquire-watchers conj f))

(def ^:private destroy-watchers (atom []))
(defn watch-destroy
  "Add a callback to be invoked on connection destruction."
  [f]
  (swap! destroy-watchers conj f))

(defn- -onAcquire [this conn id]
  (doseq [f @acquire-watchers]
    (f conn)))

(defn- -onDestroy [this conn id]
  (doseq [f @destroy-watchers]
    (f conn)))

(gen-class :name conn_customizer
           :extends com.mchange.v2.c3p0.AbstractConnectionCustomizer)
