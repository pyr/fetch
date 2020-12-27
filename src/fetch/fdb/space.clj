(ns fetch.fdb.space
  (:refer-clojure :exclude [range])
  (:require [fetch.fdb.tuple :as tuple]
            [exoscale.ex     :as ex])
  (:import com.apple.foundationdb.subspace.Subspace
           com.apple.foundationdb.tuple.Tuple
           com.apple.foundationdb.Range))

(defn by-name
  [dirs space]
  (or (get dirs space)
      (ex/ex-not-found! (str "unknown space: " space) {:space space})))

(defn ^bytes pack
  ([^Subspace sub]
   (.pack sub))
  ([^Subspace sub ^Tuple t]
   (.pack sub t)))

(defn ^Tuple unpack
  ([dirs space bytes]
   (unpack (by-name dirs space) bytes))
  ([^Subspace space ^bytes bytes]
   (.unpack space bytes)))

(defn ^Range subrange
  ([^Subspace sub]
   (.range sub))
  ([^Subspace sub ^Tuple from]
   (.range sub from)))

(defn range
  [dirs space & objs]
  (subrange (by-name dirs space) (tuple/from-seq objs)))

(defn from-seq
  [dirs space objs]
  (pack (by-name dirs space) (tuple/from-seq objs)))

(defn from
  [dirs space & objs]
  (from-seq dirs space objs))
