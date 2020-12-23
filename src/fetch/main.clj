(ns fetch.main
  (:require [com.stuartsierra.component :as component :refer [using]]
            [exoscale.mania             :as mania]
            [clojure.tools.logging      :as log]
            [clojure.spec.alpha         :as s]
            [fetch.api                  :as api]
            [fetch.fdb                  :as fdb]
            [fetch.grpc.service         :as service]
            [fetch.grpc.server          :as server])
  (:gen-class))

(def components
  "The full map of operators and managers"
  {::fdb             (using fdb/handle    [::fdb/cluster-file])
   ::kvapi           (using api/kv        {:db ::fdb})
   ::kv              (using service/kv    {::service/kv-api ::kvapi})
   ::server/services [::kv]
   ::server          (using server/server [::server/services ::kv])})

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
(s/def ::config (s/keys ))
