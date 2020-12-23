(ns fetch.fdb
  (:require [clojure.spec.alpha :as s]
            [exoscale.ex        :as ex]
            [fetch.fdb.fn       :as fn])
  (:import com.apple.foundationdb.Database
           com.apple.foundationdb.FDB
           java.util.concurrent.Executor))

(def default-path
  "Default path for the cluster file"
  "/etc/foundationdb/fdb.cluster")

(defn- open
  ([^String cluster-file ^Executor executor]
   (let [fdb      (FDB/selectAPIVersion 630)
         executor (or executor FDB/DEFAULT_EXECUTOR)]
     (.open fdb cluster-file executor))))

(defn- component-start
  [{::keys [cluster-file executor] :as fdb}]
  (ex/assert-spec-valid ::config fdb)
  (assoc fdb ::database (open cluster-file executor)))

(defn- component-stop
  [{::keys [database] :as fdb}]
  (when (some? database)
    (.close ^Database database))
  (dissoc fdb ::database))

(defn run-in-transaction
  [this f]
  (.run ^Database (::database this) (fn/wrap f)))

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

(s/def ::executor (partial instance? Executor))
(s/def ::cluster-file string?)
(s/def ::config (s/keys :req [::cluster-file]
                        :opt [::executor]))
