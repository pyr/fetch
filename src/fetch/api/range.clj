(ns fetch.api.range
  "Abstract handling of RANGE operations for etcd.
   This does not rely on concrete operations provided by a database
   implementation such as FoundationDB but goes through the protocoled
   operations available from `fetch.api`."
  (:require [fetch.fdb.op       :as op]
            [fetch.fdb.payload  :as p]
            [fetch.fdb.tuple    :as tuple]
            [fetch.fdb.space    :as space]
            [fetch.fdb.kv       :as kv]
            [exoscale.ex        :as ex]))

;; Keys are stored as tuple of

(defn with-key-filter
  [[sz {:etcd.api.kv/keys [keys-only? min-create-revision max-create-revision]
        :or               {min-create-revision 0
                           max-create-revision Integer/MAX_VALUE
                           keys-only?          false}}]
   kvs]
  (first
   (for [kv    kvs
         :let  [[k revision] (tuple/expand (space/unpack sz :keys (kv/k kv)))
                [lease-id v] (tuple/decode-and-expand (kv/v kv))]
         :when (and (>= revision min-create-revision)
                    (<= revision max-create-revision))]
     {:etcd.api.kv/key k
      :etcd.api.kv/value v
      :etcd.api.kv/lease-id lease-id})))

(defn handle-single-key
  [tx sz {:etcd.api.kv/keys [key revision] :as req}]
  (with-key-filter [tx req]
    (if (and (some? revision) (pos? revision))
      [@(op/get tx (p/key sz key revision))]
      @(op/range-with-range tx (p/key-range sz key) 100 true))))

(defn handle
  [tx sz {:etcd.api.kv/keys [range-end] :as req}]
  (if (nil? range-end)
    (handle-single-key tx sz req)
    (ex/ex-unsupported! "actual ranges are unsupported")))
