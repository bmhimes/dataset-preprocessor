(defproject adp "0.5.0-SNAPSHOT"
  :description "Automated Dataset Processor"
  :url "https://github.com/bmhimes"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [clojure-csv "2.0.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [log4j/log4j "1.2.16"]
                 [org.clojure/tools.cli "0.3.1"]
                 [criterium "0.4.3"]
                 [me.raynes/fs "1.4.4"]
                 [com.taoensso/nippy "2.6.1"]
                 [org.apache.commons/commons-io "1.3.2"]
                 [org.apache.hadoop/hadoop-core "0.20.2"]
                 [javax.mail/mail "1.4.1"]
                 [org.apache.geronimo.specs/geronimo-jms_1.1_spec "1.0"]]
  :resource-paths ["resources/shared"]
  :jvm-opts ["-Xmx3096M" "-server"]
  :main ^:skip-aot adp.adp.core
  :target-path "target/%s"
  :testing-path "test"
  :test-selectors {:default (fn [m] (not (or (:quick-bench m) (:scaling-bench m))))
                   :quick-bench :quick-bench
                   :scaling-bench :scaling-bench
                   :simulation :simulation
                   :unit :unit
                   :claims :claims}

  :profiles {
             :uberjar {
                       :aot :all}
             :dev {
                   :resource-paths ["resources/dev"]}
             :appliance [:uberjar {
                         :resource-paths ["resources/appliance"]}]}
  :omit-source true) 
