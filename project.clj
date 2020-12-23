(defproject exoscale/fetch "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "1.1.0"]
                 [cc.qbits/auspex "0.1.0-alpha2"]
                 [spootnik/unilog "0.7.27"]
                 [exoscale/ex "0.3.16"]
                 [org.foundationdb/fdb-java "6.2.21"]
                 [exoscale/fetch-proto      "0.1.4-SNAPSHOT"]
                 [exoscale/mania            "0.1.306"]]
  :jvm-opts ["-Dio.netty.tryReflectionSetAccessible=false"]
  :profiles {:dev {:resource-paths ["test/resources"]
                   :jvm-opts ["-Dlogback.configurationFile=stdout.logback.xml"]}}
  :repositories [["exoscale" {:url "https://artifacts.exoscale.ch"}]]
  :repl-options {:init-ns exoscale.fetch})
