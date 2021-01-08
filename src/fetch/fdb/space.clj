(ns fetch.fdb.space
  (:refer-clojure :exclude [range])
  (:require [fetch.fdb.tuple :as tuple]
            [exoscale.ex     :as ex])
  (:import com.apple.foundationdb.subspace.Subspace
           com.apple.foundationdb.tuple.Tuple
           com.apple.foundationdb.Range))

(defn starts-with
  [prefix]
  (Range/startsWith (tuple/encode-and-pack prefix)))

(defn bounded
  [begin end]
  (Range. (tuple/encode-and-pack begin)
          (tuple/encode-and-pack end)))

(defn bounded-bytes
  [begin end]
  (Range. ^bytes begin ^bytes end))

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

(defn- inc-prefix
  "Given an object path, yield the next semantic one."
  [^String p]
  (when (seq p)
    (let [[c & s]  (reverse p)
          reversed (conj s (-> c int inc char))]
      (->> reversed
           reverse
           (map byte)
           byte-array))))

(defn- concat-arrays
  [^bytes b1 ^bytes b2]
  (byte-array (concat (seq b1) (seq b2))))

(defn range-prefix
  [dirs space prefix]
  (let [head  (pack (by-name dirs space))
        start (byte-array (map byte prefix))
        end   (inc-prefix prefix)]
    (bounded-bytes (concat-arrays head start) (concat-arrays head end))))

(defn from-seq
  [dirs space objs]
  (pack (by-name dirs space) (tuple/from-seq objs)))

(defn from
  [dirs space & objs]
  (from-seq dirs space objs))
