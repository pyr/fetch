(ns fetch.fdb.dir
  (:require [fetch.fdb.tuple :as tuple])
  (:import com.apple.foundationdb.directory.DirectoryLayer
           com.apple.foundationdb.directory.DirectorySubspace
           com.apple.foundationdb.subspace.Subspace
           com.apple.foundationdb.TransactionContext
           com.apple.foundationdb.tuple.Tuple
           com.apple.foundationdb.Range))

(defn create-or-open
  [^TransactionContext txc path]
  (let [path (mapv str (if (coll? path) path [path]))]
    (-> (DirectoryLayer.)
        (.createOrOpen txc path))))

(defn ^Subspace subspace
  [^DirectorySubspace dir path]
  (.subspace dir (tuple/encode path)))

(defn ^bytes pack
  ([^Subspace sub]
   (.pack sub))
  ([^Subspace sub ^Tuple t]
   (.pack sub t)))

(defn ^Tuple unpack
  [^Subspace sub ^bytes k]
  (.unpack sub k))

(defn ^Range subrange
  ([^Subspace sub]
   (.range sub))
  ([^Subspace sub ^Tuple from]
   (.range sub from)))
