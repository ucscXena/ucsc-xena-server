(def version "0.1.9")

(defproject cavm (str version "-SNAPSHOT")
  :description "Cancer Analytics Virtual Machine"
  :url "https://genome-cancer.ucsc.edu"
  :manifest {"Implementation-Version" ~version}
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.cli "0.3.1"]
                 [net.mikera/core.matrix "0.8.0"]
                 [net.mikera/vectorz-clj "0.11.0"]
                 [korma "0.3.1"]
                 [org.clojars.smee/binary "0.2.0"]
                 [clj-time "0.5.0"]
                 [liberator "0.10.0"]
                 [compojure "1.1.3"]
                 [ring/ring-core "1.2.2"]
                 [ring/ring-devel "1.2.2"] ; XXX only in dev
                 [ring/ring-jetty-adapter "1.2.2"]
                 [amalloy/ring-gzip-middleware "0.1.3"]
                 [me.raynes/fs "1.4.2"]
                 [digest "1.4.3"]
                 [org.clojure/data.json "0.2.3"]
;                 [org.codehaus.jsr166-mirror/jsr166y "1.7.0"] ; for core.reducers
                 [clojure-csv/clojure-csv "2.0.1"]
                 [honeysql "0.4.2"]
                 [com.h2database/h2 "1.3.175"]
                 [filevents "0.1.0"]
                 [clj-http "0.9.1"]
                 [org.clojure/tools.logging  "0.3.0"]
                 [com.taoensso/timbre  "3.2.0"]]
  :javac-options ["-target" "1.6" "-source" "1.6"]
  :aot [cavm.h2-binary cavm.version cavm.h2-unpack-rows cavm.conn-customizer]
  :global-vars {*warn-on-reflection* true}
  :main cavm.core)
