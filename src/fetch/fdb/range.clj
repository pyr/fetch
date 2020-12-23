(ns fetch.fdb.range
  (:require [fetch.fdb.tuple :as tuple])
  (:import com.apple.foundationdb.Range))

(defn starts-with
  [prefix]
  (Range/startsWith (tuple/encode-and-pack prefix)))

(defn bounded
  [begin end]
  (Range. (tuple/encode-and-pack begin)
          (tuple/encode-and-pack end)))
