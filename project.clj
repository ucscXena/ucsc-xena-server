(defproject cavm "0.1.0-SNAPSHOT"
  :description "Cancer Analytics Virtual Machine"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [net.mikera/core.matrix "0.8.0"]
                 [net.mikera/vectorz-clj "0.11.0"]
                 [korma "0.3.0-RC5"]
                 [org.clojars.smee/binary "0.2.0"]
                 [clj-time "0.5.0"]
                 [noir "1.3.0"]
                 [ring "1.2.0"]
                 [me.raynes/fs "1.4.2"]
                 [com.h2database/h2 "1.3.171"]]
  :main cavm.core)
