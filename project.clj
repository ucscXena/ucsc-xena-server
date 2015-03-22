(def version "0.15.0-SNAPSHOT")

(defproject cavm version
  :description "Cancer Analytics Virtual Machine"
  :url "https://genome-cancer.ucsc.edu"
  :license {:name "Apache License Version 2.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :profiles {:dev {:dependencies
                   [[org.jumpmind.symmetric.schemaspy/schemaspy "5.0.0"]]}
             :uberjar {:omit-source true :aot [mikera.vectorz.core]}}
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
                 [org.clojure/test.check  "0.5.9"]
                 [com.google.code.gson/gson  "2.2.4"]
                 [less-awful-ssl  "1.0.0"]
                 [com.taoensso/timbre  "3.2.0"]]
  :java-source-paths ["src-java"]
  :target-path  "target/%s"
  :clean-targets [:target-path :compile-path "build"]
  :aliases {"uberdoc" ["do" ["alldocs"] ["clientsrc"] ["docinstall"] ["uberwrap"]]
            ; XXX this is a bit raw
            "docinstall" ["do"
                          ["shell" "rm" "-rf" "resources/public/docs"]
                          ["shell" "mv" "doc/_build" "resources/public/docs"]]
            "uberwrap" ["do"
                        ["uberjar"]
                        ["uberfix"]
                        ["install4j"]
                        ["downloads"]]
            "downloads"  ["shell" "sh" "-c" "cp get-xena/* build"]
            "uberfix" ["shell" "./fixjar" ~(str "target/uberjar/cavm-" version "-standalone.jar")]
            "install4j" ["shell" "install4jc" "-r" ~version "build1.install4j"]
            "clientsrc" ["shell" "cp" "-r" "python" "doc/_build"]
            "schemaspy" ["shell" "./schemaspy"]
            "alldocs" ["do"
                       ["sphinx"]
                       ["doc"]
                       ["hiera"]
                       ["schemaspy"]]}
  :jar-exclusions     [#"schemaspy.clj" #"(?:^|/)\..*\.swp$" #"(?:^|/)\.nfs"]
  :uberjar-exclusions [#"schemaspy.clj" #"(?:^|/)\..*\.swp$" #"(?:^|/)\.nfs" #"\.(clj|java)$"]
  :javac-options ["-target" "1.6" "-source" "1.6"]
  :aot [cavm.core cavm.h2-binary cavm.h2-unpack-rows cavm.conn-customizer]
  :global-vars {*warn-on-reflection* true}
  :plugins [[lein-sphinx  "1.0.1"]
            [codox  "0.8.10"]
            [lein-shell  "0.4.0"]
            [lein-hiera  "0.8.0"]]
  :sphinx {:setting-values {:version ~version
                            :project ~(str  "UCSC Xena Server " version)}}
  :codox {:output-dir "doc/_build/implementation"}
  :hiera {:ignore-ns #{cavm.h2-query}
          :cluster-depth 1
          :vertical? false
;          :show-external? true
          :path "doc/_build/dependencies.png"}
  :main cavm.core)
