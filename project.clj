(defproject exoscale/fetch "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure       "1.10.1"]
                 [org.clojure/tools.logging "1.1.0"]
                 [cc.qbits/auspex           "0.1.0-alpha2"]
                 [spootnik/unilog           "0.7.27"]
                 [exoscale/ex               "0.3.16"]
                 [org.foundationdb/fdb-java "6.2.21"]
                 [exoscale/fetch-proto      "0.1.4-SNAPSHOT"]
                 [io.grpc/grpc-netty        "1.35.0"]
                 [io.grpc/grpc-core         "1.35.0"]
                 [io.grpc/grpc-api          "1.35.0"]
                 [exoscale/mania            "0.1.306"]
                 [exoscale/interceptor      "0.1.9"]]
  :main fetch.main
  :jvm-opts ["-Dio.netty.tryReflectionSetAccessible=false"]
  :profiles {:dev     {:resource-paths ["test/resources"]
                       :source-paths   ["dev"]
                       :jvm-opts       ["-Dlogback.configurationFile=stdout.logback.xml"]}
             :uberjar {:global-vars {*warn-on-reflection* true}
                       :jvm-opts ["-Dio.netty.tryReflectionSetAccessible=false"]
                       :aot         :all}}
  :repositories [["exoscale" {:url "https://artifacts.exoscale.ch"}]]
  :repl-options {:init-ns exoscale.fetch})
