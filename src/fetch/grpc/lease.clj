(ns fetch.grpc.lease
  (:require [fetch.grpc.types  :as types]
            [fetch.grpc.stream :as stream]
            [exoscale.ex       :as ex])
  (:import exoscale.etcd.api.LeaseGrpc$LeaseImplBase))

(def ^:private lease-service
  (proxy [LeaseGrpc$LeaseImplBase] []
    (leaseGrant [req resp]
      (stream/complete!
       (types/lease-grant-response req)))
    (leaseRevoke [_ resp]
      (stream/error! (ex/ex-unsupported "lease revocation is not supported")))
    (leaseKeepAlive [_ resp]
      (stream/error! (ex/ex-unsupported "lease keep-alive is not supported")))
    (leaseTimeToLive [_ resp]
      (stream/error! (ex/ex-unsupported "lease time-to-live is not supported")))
    (leaseLeases [_ resp]
      (stream/error! (ex/ex-unsupported "listing leases is not supported")))))

(def lease
  (with-meta {}
    {'fetch.grpc.server/get-service (constantly lease-service)}))
