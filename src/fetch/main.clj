(ns fetch.main
  (:require [com.stuartsierra.component :as component :refer [using]]
            [exoscale.mania             :as mania]
            [clojure.tools.logging      :as log]
            [clojure.spec.alpha         :as s]
            [fetch.store                :as store]
            [fetch.fdb                  :as fdb]
            [fetch.grpc.kv              :as kv]
            [fetch.grpc.lease           :as lease]
            [fetch.grpc.watch           :as watch]
            [fetch.grpc.server          :as server])
  (:gen-class))

(def components
  "The full map of operators and managers"
  {::fdb             (using fdb/handle    [::fdb/cluster-file ::fdb/prefix])
   ::store/engine    (using fdb/store {:db ::fdb})
   ::kv              (using kv/service [::store/engine])
   ::lease           lease/service
   ::watch           (using watch/service [::store/engine])
   ::server/services [::kv ::lease ::watch]
   ::server          (using server/server [::server/services
                                           ::server/port
                                           ::kv
                                           ::lease
                                           ::watch])})

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
