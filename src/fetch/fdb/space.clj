(ns fetch.fdb.space
  (:refer-clojure :exclude [range])
  (:require [qbits.auspex    :as a]
            [fetch.fdb.dir   :as dir]
            [fetch.fdb.tuple :as tuple]
            [exoscale.ex     :as ex]))

(defn create-or-open
  "Creates a map of subspaces for the various data needed.
  All subspaces will be located in a FoundationDB *directory*,
  with the top-level name, and the etcd instance ID"
  [tx instance-id]
  (a/chain (dir/create-or-open tx ["etcd" instance-id])
           (fn [dir]
             {:fetch.db/keys       (dir/subspace dir "k")
              :fetch.db/leases     (dir/subspace dir "l")
              :fetch.db/lease-keys (dir/subspace dir "L")
              :fetch.db/watches    (dir/subspace dir "w")
              :fetch.db/metadata   (dir/subspace dir "m")
              :fetch.db/revision   (dir/subspace dir "r")})))

(defn by-name
  [dirs space]
  (let [qspace (if-not (qualified-keyword? space)
                 (keyword "fetch.db" (name space))
                 space)]
    (or (get dirs qspace)
        (ex/ex-not-found! (str "unknown space: " qspace) {:space qspace}))))

(defn from-seq
  [dirs space objs]
  (dir/pack (by-name dirs space) (tuple/from-seq objs)))

(defn from
  [dirs space & objs]
  (from-seq dirs space objs))

(defn pack
  [space]
  (dir/pack space))

(defn unpack
  [dirs space bytes]
  (dir/unpack (by-name dirs space) bytes))

(defn range
  [dirs space & objs]
  (dir/subrange (by-name dirs space) (tuple/from-seq objs)))
