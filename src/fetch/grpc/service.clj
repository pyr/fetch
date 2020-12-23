(ns fetch.grpc.service
  (:require [fetch.grpc.common :as grpc]
            [fetch.api.proto   :as proto]
            [fetch.grpc.types  :as types])
  (:import exoscale.etcd.api.KVGrpc$KVImplBase))

(defn- make-kv-service
  [impl]
  (let [put-fn          (grpc/wrap-unary-fn (partial proto/-put impl)
                                            types/put-request->map
                                            types/map->put-response)
        range-fn        (grpc/wrap-unary-fn (partial proto/-range impl)
                                            types/range-request->map
                                            types/map->range-response)
        delete-range-fn (grpc/wrap-unary-fn (partial proto/-delete-range impl)
                                            types/delete-range-request->map
                                            types/map->delete-range-response)
        txn-fn          (grpc/wrap-unary-fn (partial proto/-txn impl)
                                            types/txn-request->map
                                            types/map->txn-response)
        compact-fn      (grpc/wrap-unary-fn (partial proto/-compact impl)
                                            types/compaction-request->map
                                            types/map->compaction-response)]
    (proxy [KVGrpc$KVImplBase] []
      (put [req resp]
        (put-fn req resp))
      (range [req resp]
        (range-fn req resp))
      (delete-range [req resp]
        (delete-range-fn req resp))
      (txn [req resp]
        (txn-fn req resp))
      (compact [req resp]
        (compact-fn req resp)))))

(defn- get-kv-service
  [{::keys [kv-api]}]
  (make-kv-service kv-api))

(def kv
  (with-meta {}
    {'fetch.grpc.server/get-service get-kv-service}))
