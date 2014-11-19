(ns
  ^{:author "Brian Craft"
    :doc "Protocol for xena operations on a database."}
  cavm.db)

(defprotocol XenaDb
  "Xena database protocol"
  (write-matrix [this mname files metadata data-fn features always]
                "Write a dataset to the database.
                <mname> --- the (unique) name of the dataset.
                <files> ---  a vector of file names that were read for the dataset.
                <metadata> ---  a map of metadata to be associated with the dataset.
                <data-fn> --- a function returning the data of each field in the dataset.
                <features> --- a map of metadata describing the fields.
                <always> --- force update of database, even if the files are unchanged
                             since the last load of this dataset.")
  (delete-matrix [this mname]
                 "Remove a dataset from the database.
                 <mname> --- the (unique) name of the dataset.")
  (run-query [this query] "Execute a sql query.
                          <query> is a honeysql map.")
  (fetch [this reqs] "Retrieve rows from specified fields in a dataset,
                     matching a list of sampleIDs, as an array of floats.

                     reqs is an array of maps
                     [{:table <dataset_name>
                       :columns [<field_name>, ...]
                       :samples [<sampleID>, <sampleID>, ..]}, ...]")
  (close [this] "Close the database."))
