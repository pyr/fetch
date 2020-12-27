(defproject exoscale/fetch "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "1.1.0"]
                 [cc.qbits/auspex "0.1.0-alpha2"]
                 [spootnik/unilog "0.7.27"]
                 [exoscale/ex "0.3.16"]
                 [org.foundationdb/fdb-java "6.2.21"]
                 [exoscale/fetch-proto      "0.1.4-SNAPSHOT"]
                 [io.grpc/grpc-netty        "1.34.1"]
                 [io.grpc/grpc-core         "1.34.1"]
                 [io.grpc/grpc-api          "1.34.1"]
                 [exoscale/mania            "0.1.306"]]
  :main fetch.main
  :jvm-opts ["-Dio.netty.tryReflectionSetAccessible=false"]
  :profiles {:dev     {:resource-paths ["test/resources"]
                       :jvm-opts       ["-Dlogback.configurationFile=stdout.logback.xml"]}
             :uberjar {:global-vars {*warn-on-reflection* true}
                       :aot         :all}}
  :repositories [["exoscale" {:url "https://artifacts.exoscale.ch"}]]
  :repl-options {:init-ns exoscale.fetch})