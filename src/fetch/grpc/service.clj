(ns fetch.grpc.service
  "Shim to hide the guts of GRPC java to downstream code.
   We take the opportunity here to short-circuit for the
   cases we are interested in, this smoothes over parts of
   the etcd API that Kubernetes does not care about. Actual
   operations are then carried out by "
  (:require [fetch.grpc.common :as grpc]
            [fetch.api.proto   :as proto]
            [fetch.grpc.types  :as types]
            [fetch.grpc.stream :as stream]
            [exoscale.ex       :as ex]
            [qbits.auspex      :as a])
  (:import exoscale.etcd.api.KVGrpc$KVImplBase
           exoscale.etcd.api.LeaseGrpc$LeaseImplBase
           exoscale.etcd.api.WatchGrpc$WatchImplBase))

(defn wrap-unary-fn
  [f coax-out]
  (fn [req resp]
    (try
      (a/catch
          (a/chain (f req)
                   (if (some? coax-out)
                     (comp (partial stream/complete! resp) coax-out)
                     (partial stream/complete! resp)))
          (fn [e] (stream/error! resp (ex-cause e))))
      (catch Exception e
        (stream/error! resp e)))))

(defn get-request?
  [{:keys [range-end]}]
  (zero? (count range-end)))

(defn invalid-range?
  [_]
  false)

(defonce last-txn-request (atom nil))

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

(defn- make-watch-service
  [{::proto/keys [watchbackend]}]
  (proxy [WatchGrpc$WatchImplBase] []
    (watch [resp]
      (reify io.grpc.stub.StreamObserver
        (onNext [_ raw-req]
          (let [{:keys [type] :as req} (types/watch-request->map raw-req)]
            (if (= type :create)
              (wrap-stream-call (partial proto/create-watch watchbackend req)
                                resp)
              (wrap-stream call (partial proto/cancel-watch watchbackend req)
                           resp))))))))

(defn- range-prefix
  [range-end]
  (let [ba (byte-array (concat (map byte (drop-last range-end))
                               [(-> range-end last byte dec byte)]))
        s (String. ^bytes ba "UTF-8")]
    (cond-> s (not= \/ (last s)) (str "/"))))

(defn single-get-handler
  [kvdb {:keys [key revision]}]
  (let [[rev kv] (if (pos? revision)
                   (proto/get-at-revision kvdb key revision)
                   (proto/get-latest kvdb key))]
    (proto/get-response rev kv)))

(defn range-handler
  [db {:keys [key revision range-end count-only? limit]}]
  (if count-only?
    (let [[rev num-keys] (proto/count-keys db (range-prefix range-end))]
      (types/range-count-response rev num-keys))
    (let [[rev kvs] (proto/range-keys db revision limit key
                                      (range-prefix range-end))]
      (types/range-response rev limit kvs))))

(defn update-handler
  [db {:keys [key revision value lease]}]
  (let [[rev kv succeeded?] (proto/update-at-revision db key revision
                                                      value lease)]
    (if succeeded?
      (types/update-success-response rev)
      (types/update-failure-response rev kv))))

(defn delete-handler
  [kvdb {:keys [key revision]}]
  (let [[rev kv succeeded?] (proto/delete-key kvdb key revision)]
    (types/delete-response rev kv succeeded?)))

(defn create-handler
  [kvdb {:keys [key value lease put]}]
  (when (:ignore-lease? put)
    (ex/ex-unsupported! "ignoreLease in put"))
  (when (:ignore-value? put)
    (ex/ex-unsupported! "ignoreValue in put"))
  (when (:previous? put)
    (ex/ex-unsupported! "prevKv in put"))
  (let [[rev ok?] (proto/create-if-absent kvdb key value lease)]
    (if ok?
      (types/create-success-response rev)
      (types/create-already-exists-response rev))))

(defn dispatch-txn
  [kvdb {:keys [type] :as req}]
  (case type
    :update  (update-handler kvdb req)
    :delete  (delete-handler kvdb req)
    :create  (create-handler kvdb req)
    :compact types/txn-compaction-response))

(defn- make-kv-service
  [{::proto/keys [kvdb]}]
  (let [range-fn (grpc/wrap-unary-fn (partial range-handler kvdb)
                                     types/map->range-response)
        txn-fn   (grpc/wrap-unary-fn (partial dispatch-txn kvdb)
                                     types/map->txn-response)
        get-fn   (grpc/wrap-unary-fn (partial single-get-handler kvdb)
                                     types/map->range-response)]
    (proxy [KVGrpc$KVImplBase] []
      (put [req resp]
        (stream/error! resp (ex/ex-unsupported "put is not supported")))
      (range [raw-req resp]
        (let [{:keys [unsupported] :as req} (types/range-request->map raw-req)]
          (cond
            (get-request? req)   (get-fn req resp)
            (some? unsupported)  (stream/error!
                                  resp
                                  (ex/ex-unsupported unsupported))
            :else                (range-fn req resp))))
      (deleteRange [req resp]
        (stream/error! resp (ex/ex-unsupported "deleterange is not supported")))
      (txn [raw-req resp]
        (reset! last-txn-request raw-req)
        (let [{:keys [unsupported] :as req} (types/txn-request->map raw-req)]
          (if (some? unsupported)
            (stream/error! resp (ex/ex-unsupported unsupported))
            (txn-fn req resp))))
      (compact [req resp]
        (let [payload (types/compaction-response req)]
          (stream/complete! resp payload))))))

(def kv
  (with-meta {}
    {'fetch.grpc.server/get-service make-kv-service}))

(def watch
  (with-meta {}
    {'fetch.grpc.server/get-service make-watch-service}))

(def lease
  (with-meta {}
    {'fetch.grpc.server/get-service (constantly lease-service)}))
