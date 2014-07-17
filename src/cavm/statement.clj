(ns cavm.statement
  (:require [clojure.java.jdbc :as jdbc])
  (:require [cavm.conn-customizer :refer [watch-destroy]])
  (:import java.sql.PreparedStatement))

;
; Utils for prepared statements
;

; Record for PreparedStatements that can be a target for with-open, and
; execute the PreparedStatement.
(defrecord PStatement [^PreparedStatement stmt execute]
  java.sql.Statement
  (close [this] (.close stmt))
  clojure.lang.IFn
  (invoke [this values]
    (execute values))
  (applyTo [this args]
    (apply execute args)))

(defn stmt-execute
  ([^PreparedStatement conn stmt fields]
   (fn [values]
     (jdbc/db-do-prepared {:connection conn} false stmt (mapv values fields))))
  ([^PreparedStatement conn stmt]
   (fn [values]
     (jdbc/db-do-prepared {:connection conn} false stmt values))))

(defn sql-stmt ^cavm.statement.PStatement [conn stmt-str & args]
  (let [stmt (jdbc/prepare-statement conn stmt-str)]
    (->PStatement
      stmt
      (apply stmt-execute conn stmt args))))

(defn query-execute
  ([^PreparedStatement conn stmt fields]
   (fn [values]
     (jdbc/query {:connection conn} [stmt (mapv values fields)])))
  ([^PreparedStatement conn stmt]
   (fn [values]
     (jdbc/query {:connection conn} (into [stmt] values)))))

(defn sql-stmt-result ^cavm.statement.PStatement [conn stmt-str & args]
  (let [stmt (jdbc/prepare-statement conn stmt-str)]
    (->PStatement
      stmt
      (apply query-execute conn stmt args))))

;
;
; Utility to cache prepared statements, per-connection. This may seem to
; duplicate c3p0 statement caching, however it's per-statement, so hashing is
; on the connection, not the statement string. Not positive this matters, but
; the c3p0 statement cache has been slow in testing, and it can deadlock when
; executing a prepared statement that needs to create a prepared statement.

(defn cached-statement [stmt-str result?]
  (let [mk-stmt (if result? sql-stmt-result sql-stmt)
        cache (atom {})
        lookup (fn [conn]
                 (or (@cache conn)
                     ((swap! cache assoc conn (mk-stmt conn stmt-str)) conn)))]
    (watch-destroy (fn [conn] (swap! cache dissoc conn))) ; XXX leak stmt?
    (fn [conn & args]
      ((lookup conn) args))))
