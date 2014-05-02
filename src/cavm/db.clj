(ns cavm.db)

(defprotocol XenaDb
  "Xena database protocol"
  (write-matrix [this files metadata data-fn features always]
                "Write a matrix to storage")
  (write-probemap [this id files metadata data] "Write a probemap to storage")
  (run-query [this query] "Execute a sql query")
  (close [this] "Close the database"))
