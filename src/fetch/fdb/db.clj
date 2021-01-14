(ns fetch.fdb.db
  (:require [exoscale.ex                :as ex]
            [com.stuartsierra.component :as component]
            [fetch.store                :as store]
            [fetch.fdb.store            :as fstore]
            [clojure.spec.alpha         :as s])
  (:import com.apple.foundationdb.Database
           com.apple.foundationdb.FDB
           com.apple.foundationdb.TransactionContext
           com.apple.foundationdb.directory.Directory
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

(defn create-dir
  [^TransactionContext txc path]
  (let [path (mapv str (if (coll? path) path [path]))]
    (-> (DirectoryLayer.)
        (.createOrOpen txc path)
        (deref))))

(defn remove-dir
  [^TransactionContext txc ^Directory dir]
  (-> (DirectoryLayer.)
      (.removeIfExists txc (.getPath dir))
      (deref)))

(defn ^Subspace subdir
  [^TransactionContext txc ^DirectorySubspace dir path]
  @(.createOrOpen dir txc [(name path)]))

(defn make-dirs
  "Creates a map of subspaces for the various data needed.
  All subspaces will be located in a FoundationDB *directory*,
  with the top-level name, and the etcd instance ID"
  [db prefix instance-id]
  (let [topdir      (create-dir db [prefix])
        instancedir (create-dir db [prefix instance-id])]
    (reduce #(assoc %1 %2 (subdir db instancedir %2))
            {::top topdir ::instance instancedir}
            [:keys :instances :watches :events :metadata])))

(def top-dir
  (comp ::top ::dirs))

(def get-handle
  ::database)

(defrecord DatabaseHandle []
  component/Lifecycle
  (start [this]
    (let [{:fetch.fdb/keys [cluster-file executor]} this]
      (ex/assert-spec-valid ::config this)
      (assoc this ::database (open cluster-file executor))))
  (stop [this]
    (when-some [db (::database this)]
      (close db))
    (dissoc this ::database))
  store/StorageEngineFactory
  (namespaced [this ns]
    (fstore/->FDBStoreEngine
     (assoc this ::dirs (make-dirs (::database this)
                                   (:fetch.fdb/prefix this)
                                   ns)))))

(def make-database #'map->DatabaseHandle)

(s/def :fetch.fdb/executor (partial instance? Executor))
(s/def :fetch.fdb/cluster-file string?)
(s/def :fetch.fdb/prefix string?)
(s/def ::config (s/keys :req [:fetch.fdb/cluster-file :fetch.fdb/prefix]
                        :opt [:fetch.fdb/executor]))
