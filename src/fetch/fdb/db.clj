(ns fetch.fdb.db
  (:require [fetch.fdb.fn    :as fn]
            [fetch.fdb.tuple :as tuple]
            [qbits.auspex    :as a])
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
  (.run ^Database (::database this) (fn/wrap f)))

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
  [this instance-id]
  @(a/chain (create-dir (::database this) ["etcd" instance-id])
            (fn [dir]
              {:keys     (subspace dir "k")
               :watches  (subspace dir "w")
               :events   (subspace dir "e")
               :metadata (subspace dir "m")
               :revision (subspace dir "r")})))
