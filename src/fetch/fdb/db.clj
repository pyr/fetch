(ns fetch.fdb.db
  (:require [fetch.fdb.fn :as fn])
  (:import com.apple.foundationdb.Database
           com.apple.foundationdb.FDB
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
