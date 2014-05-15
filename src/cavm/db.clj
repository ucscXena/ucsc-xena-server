(ns cavm.db)

(defprotocol XenaDb
  "Xena database protocol"
  (write-matrix [this mname files metadata data-fn features always]
                "Write a matrix to storage")
  (write-probemap [this pname files metadata data-fn always]
                  "Write a probemap to storage")
  (run-query [this query] "Execute a metadata sql query")
  (close [this] "Close the database"))
