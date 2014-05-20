(defproject cavm "0.1.0-SNAPSHOT"
  :description "Cancer Analytics Virtual Machine"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.cli "0.2.2"]
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
                 [com.h2database/h2 "1.3.171"]
                 [filevents  "0.1.0"]]
  :aot [cavm.h2-binary]
  :global-vars {*warn-on-reflection* true}
  :main cavm.core)
