(ns fetch.fdb.db
  (:require [fetch.fdb.fn          :as fn]
            [fetch.fdb.tuple       :as tuple]
            [fetch.fdb.payload     :as payload]
            [qbits.auspex          :as a]
            [exoscale.ex           :as ex]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha    :as s])
  (:import com.apple.foundationdb.Database
           com.apple.foundationdb.FDB
           com.apple.foundationdb.TransactionContext
           com.apple.foundationdb.directory.DirectoryLayer
           com.apple.foundationdb.directory.DirectorySubspace
           com.apple.foundationdb.subspace.Subspace
           java.util.concurrent.Executor))

(defn open
  ([^String cluster-file ^Executor executor]
   (let [fdb      (FDB/selectAPIVersion 620)
         executor (or executor FDB/DEFAULT_EXECUTOR)]
     (.open fdb cluster-file executor))))

(defn close
  [^Database db]
  (.close db))

(defn run-in-transaction
  [this f]
  (.run ^Database (::database this) (fn/wrap #(f % (::dirs this)))))

(defn create-dir
  [^TransactionContext txc path]
  (let [path (mapv str (if (coll? path) path [path]))]
    (-> (DirectoryLayer.)
        (.createOrOpen txc path))))

(defn ^Subspace subspace
  [^DirectorySubspace dir path]
  (.subspace dir (tuple/encode path)))

(defn make-dirs
  "Creates a map of subspaces for the various data needed.
  All subspaces will be located in a FoundationDB *directory*,
  with the top-level name, and the etcd instance ID"
  [db instance-id]
  @(a/chain (create-dir db ["etcd" instance-id])
            (fn [dir]
              {:keys     (subspace dir "k")
               :instance (subspace dir "i")
               :watches  (subspace dir "w")
               :events   (subspace dir "e")
               :metadata (subspace dir "m")
               :revision (subspace dir "r")})))

(def ^:private static-instance-id
  #uuid "4a7517f8-40f0-41ad-9e1d-cae1397c1b23")

(defn- component-start
  [{::keys [cluster-file executor] :as fdb}]
  (ex/assert-spec-valid ::config fdb)
  (let [db (open cluster-file executor)]
    (log/info "successfully opened connection to database")
    (assoc fdb
           ::database db
           ::dirs (make-dirs db static-instance-id))))

(defn- component-stop
  [{::keys [database] :as fdb}]
  (when (some? database)
    (close database))
  (dissoc fdb ::database ::dirs))

(defn make-database
  [opts]
  (with-meta opts
    {'com.stuartsierra.component/start component-start
     'com.stuartsierra.component/stop  component-stop}))

(s/def ::executor (partial instance? Executor))
(s/def ::cluster-file string?)
(s/def ::config (s/keys :req [::cluster-file]
                        :opt [::executor]))
