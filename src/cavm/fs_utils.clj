(ns cavm.fs-utils
  (:require [me.raynes.fs :as fs])
  (:require [clojure.java.io :as io]))


; java's lovely File API will not resolve all ".." appearing
; in a path. "/one/../../two", for example, will resolve to
; "/../two". The wonky workaround below calls the API repeatedly
; until the return value stops changing. Ugh.

(defn normalized-path
  "Normalize a (possibly relative) path with embedded . and .."
  [path]
  (loop [p (io/file path)]
    (let [np (.getCanonicalFile p)]
      (if (= p np)
        np
        (recur np)))))

(defn relativize
  "Return file path relative to docroot"
  [docroot file]
  (let [docroot (normalized-path docroot)
        file (normalized-path file)]
    (when-not (fs/child-of? docroot file)
      (throw (IllegalArgumentException. (str file " not in docroot path: " docroot))))
    (apply io/file (drop (count (fs/split docroot)) (fs/split file)))))
