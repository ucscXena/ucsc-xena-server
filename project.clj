(def version "0.2.0-SNAPSHOT")

(defproject cavm version
  :description "Cancer Analytics Virtual Machine"
  :url "https://genome-cancer.ucsc.edu"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :profiles {:dev {:dependencies
                   [[org.jumpmind.symmetric.schemaspy/schemaspy "5.0.0"]]}}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.cli "0.3.1"]
                 [net.mikera/core.matrix "0.8.0"]
                 [net.mikera/vectorz-clj "0.11.0"]
                 [org.clojure/java.jdbc "0.3.4"]
                 [com.mchange/c3p0 "0.9.5-pre8"]
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
                 [org.clojure/core.cache  "0.6.3"]
                 [ch.qos.logback/logback-classic  "1.1.1"]
                 [org.clojure/tools.logging  "0.3.0"]
                 [com.taoensso/timbre  "3.2.0"]]
  :target-path  "target/%s"
  :aliases {"uberdoc" ["do" ["alldocs"] ["clientsrc"] ["docinstall"] ["uberjar"]]
            ; XXX this is a bit raw
            "docinstall" ["do"
                          ["shell" "rm" "-rf" "resources/public/docs"]
                          ["shell" "mv" "doc/_build" "resources/public/docs"]]
            "clientsrc" ["shell" "cp" "-r" "python" "doc/_build"]
            "schemaspy" ["shell" "./schemaspy"]
            "alldocs" ["do"
                       ["sphinx"]
                       ["doc"]
                       ["schemaspy"]]}
  :jar-exclusions     [#"schemaspy.clj" #"(?:^|/)\..*\.swp$" #"(?:^|/)\.nfs"]
  :uberjar-exclusions [#"schemaspy.clj" #"(?:^|/)\..*\.swp$" #"(?:^|/)\.nfs"]
  :javac-options ["-target" "1.6" "-source" "1.6"]
  :aot [cavm.h2-binary cavm.h2-unpack-rows cavm.conn-customizer]
  :global-vars {*warn-on-reflection* true}
  :plugins [[lein-sphinx  "1.0.1"]
            [codox  "0.8.10"]
            [lein-shell  "0.4.0"]]
  :sphinx {:setting-values {:version ~version
                            :project ~(str  "UCSC Xena Server " version)}}
  :codox {:output-dir "doc/_build/implementation"}
  :main cavm.core)
