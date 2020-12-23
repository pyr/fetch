(ns fetch.api
  (:require [fetch.api.put          :as put]
            [fetch.api.range        :as range]
            [fetch.api.delete-range :as delete-range]
            [fetch.api.txn          :as txn]
            [fetch.api.proto        :as proto]
            [fetch.fdb.payload      :as payload]
            [fetch.fdb              :as fdb])
  (:refer-clojure :exclude [get set]))

(def static-instance-id #uuid "ec2db552-a575-4878-923d-ddad123eac3c")

(defn wrap-handler-fn
  [f req]
  (let [serializer (payload/space-serializer static-instance-id)]
    (fn [tx]
      (f tx serializer req))))

(defrecord FDBKVApi [db]
  proto/KVProto
  (-put [_ req]
    (fdb/run-in-transaction db (wrap-handler-fn put/handle req)))
  (-range [_ req]
    (fdb/run-in-transaction db (wrap-handler-fn range/handle req)))
  (-delete-range [_ req]
    (fdb/run-in-transaction db (wrap-handler-fn delete-range/handle req)))
  (-txn [_ req]
    (fdb/run-in-transaction db (wrap-handler-fn txn/handle req)))
  (-compact [_ _]))

(def kv (map->FDBKVApi {}))
