(ns fetch.main
  (:require [com.stuartsierra.component :as component :refer [using]]
            [exoscale.mania             :as mania]
            [clojure.tools.logging      :as log]
            [clojure.spec.alpha         :as s]
            [fetch.fdb                  :as fdb]
            [fetch.store                :as store]
            [fetch.grpc.kv              :as kv]
            [fetch.grpc.lease           :as lease]
            [fetch.grpc.watch           :as watch]
            [fetch.grpc.auth            :as auth]
            [fetch.grpc.server          :as server])
  (:gen-class))

(def components
  "The full map of operators and managers"
  {::fdb             (using fdb/handle  [::fdb/cluster-file ::fdb/prefix])
   ::kv              kv/service
   ::lease           lease/service
   ::watch           watch/service
   ::auth/kv         (using auth/service {::store/factory  ::fdb
                                          ::server/service ::kv})
   ::auth/watch      (using auth/service {::store/factory  ::fdb
                                          ::server/service ::kv})
   ::server/services [::auth/kv ::lease ::auth/watch]
   ::server          (using server/server [::server/services
                                           ::server/port
                                           ::auth/kv
                                           ::lease
                                           ::auth/watch])})

(defn build-system
  "We build a system map by merging the provided configuration
   with the map of components stored in `components`.

   This allows the components in the above map to request keys
   in the config (which the `::config` spec can assert are present)."
  [config]
  (into (component/system-map) (merge config components)))

;; Defer the actual work to our main generation library
(mania/def-component-main
  {:exoscale.mania.service/name      "FoundationDB backed etcd"
   :exoscale.mania.config/type       :aero
   :exoscale.mania.component/builder build-system
   :exoscale.mania.hooks/on-error    #(log/error % "error during startup")
   :exoscale.mania.config/spec       ::config})

;; Configuration specs
;; ===================
(s/def ::config (s/keys))

(comment

  (require '[aero.core :as aero])

  (def sys
    (build-system
     (aero/read-config "resources/config.edn")))

  (def sys (component/start-system sys))

  (def sys (component/stop-system sys)))
