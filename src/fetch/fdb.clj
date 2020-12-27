(ns fetch.fdb
  (:require [clojure.spec.alpha    :as s]
            [exoscale.ex           :as ex]
            [fetch.fdb.db          :as db]
            [fetch.fdb.store       :as store]
            [clojure.tools.logging :as log])
  (:import java.util.concurrent.Executor))

(defn- component-start
  [{::keys [cluster-file executor] :as fdb}]
  (ex/assert-spec-valid ::config fdb)
  (let [db (db/open cluster-file executor)]
    (log/info "successfully opened connection to database")
    (assoc fdb ::db/database db)))

(defn- component-stop
  [{::db/keys [database] :as fdb}]
  (when (some? database)
    (db/close database))
  (dissoc fdb ::db/database))

(defn make-database
  [opts]
  (with-meta opts
    {'com.stuartsierra.component/start component-start
     'com.stuartsierra.component/stop  component-stop}))

(def handle
  "A component representing a handle to a FoundationDB database.
   Expects `::cluster-file` in its configuration, and an optional
   `::executor` argument."
  (make-database {}))

(def store
  "A component which implements the main storage engine abstraction
   from fetch"
  (store/map->FDBStoreEngine {}))

(s/def ::executor (partial instance? Executor))
(s/def ::cluster-file string?)
(s/def ::config (s/keys :req [::cluster-file]
                        :opt [::executor]))
