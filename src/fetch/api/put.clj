(ns fetch.api.put
  "Abstract handling of PUT operations for etcd.
   This does not rely on concrete operations provided by a database
   implementation such as FoundationDB but goes through the protocoled
   operations available from `fetch.api`."
  (:require [fetch.api.common   :as common]
            [fetch.fdb.payload  :as p]
            [fetch.fdb.op       :as op]
            [exoscale.ex        :as ex]))

(defn handle
  [tx sz {:etcd.api.kv/keys [key value lease previous?
                             ignore-value? ignore-lease?]}]

  (when (and (some? value) (true? ignore-value?))
    (ex/ex-incorrect! "ignore_value was set for a payload containing a value"))
  (when (and (some? lease) (true? ignore-lease?))
    (ex/ex-incorrect! "ignore_lease was set for a payload containing a lease"))

  (let [rev        (common/increment-revision tx sz)
        previous   (common/previous tx sz key)
        lease-id   (cond (some? lease) lease
                         ignore-lease? (:fetch.api/lease previous)
                         :else         0)
        value      (if ignore-value? (:fetch.api/value previous) value)
        create-rev (:fetch.api/create-revision previous)]
    (op/set tx
            (p/key sz key rev)
            (p/encode-val lease-id create-rev value))
    ))
