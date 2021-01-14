(ns fetch.grpc.kv
  "Shim to hide the guts of GRPC java to downstream code.
   We take the opportunity here to short-circuit for the
   cases we are interested in, this smoothes over parts of
   the etcd API that Kubernetes does not care about. Actual
   operations are then carried out by "
  (:require [fetch.store            :as store]
            [fetch.grpc.types       :as types]
            [fetch.grpc.stream      :as stream]
            [fetch.grpc.context     :as ctx]
            [exoscale.ex            :as ex])
  (:import exoscale.etcd.api.KVGrpc$KVImplBase))

;; XXX: debug
(defonce last-txn-request (atom nil))

(defn- wrap-fn
  "A wrapper for unary grpc functions"
  [f coax-in]
  (fn [req resp]
    (try
      (stream/complete! resp (f (coax-in req)))
      (catch Exception e
        (stream/error! resp e)))))

(defn update-handler
  [{:keys [key revision value lease]}]
  (let [[rev kv succeeded?] (store/update-at-revision (ctx/engine) key revision
                                                      value lease)]
    (if succeeded?
      (types/update-success-response rev)
      (types/update-failure-response rev kv))))

(defn- delete-handler
  [{:keys [key revision]}]
  (let [[rev kv succeeded?] (store/delete-key (ctx/engine) key revision)]
    (types/delete-response rev kv succeeded?)))

(defn- create-handler
  [{:keys [key value lease put]}]
  (when (:ignore-lease? put)
    (ex/ex-unsupported! "ignoreLease in put"))
  (when (:ignore-value? put)
    (ex/ex-unsupported! "ignoreValue in put"))
  (when (:previous? put)
    (ex/ex-unsupported! "prevKv in put"))
  (let [[rev ok?] (store/create-if-absent (ctx/engine) key value lease)]
    (if ok?
      (types/create-success-response rev)
      (types/create-already-exists-response rev))))

(defn- txn-handler
  [{:keys [type unsupported] :as req}]
  (when (some? unsupported)
    (ex/ex-unsupported! unsupported))
  (case type
    :update  (update-handler req)
    :delete  (delete-handler req)
    :create  (create-handler req)
    :compact types/txn-compaction-response))

(defn- get-handler
  [{:keys [key revision]}]
  (let [[rev kv] (if (pos? revision)
                   (store/get-at-revision (ctx/engine) key revision)
                   (store/get-latest (ctx/engine) key))]
    (types/get-response rev kv)))

(defn- range-prefix-handler
  [{:keys [key revision count-only? limit]}]
  (if count-only?
    (let [[rev num-keys] (store/count-keys (ctx/engine) key)]
      (types/range-count-response rev num-keys))
    (let [[rev kvs] (store/range-keys (ctx/engine) revision limit key)]
      (types/range-response rev limit kvs))))

(defn- range-handler
  [{:keys [unsupported range-end] :as req}]
  (when (some? unsupported)
    (ex/ex-unsupported! unsupported))
  (if (zero? (count range-end))
    (get-handler req)
    (range-prefix-handler req)))

(defn make-service
  "Kubernetes only needs two out of five signatures implemented for its
   interaction with etcd: txn and range. create, delete, and update operations
   are performed as part of transactions"
  [_]
  (let [range-fn (wrap-fn range-handler types/range-request->map)
        txn-fn   (wrap-fn txn-handler types/txn-request->map)]
    (proxy [KVGrpc$KVImplBase] []
      ;; put and deleteRange can safely report failure
      (put [req resp]
        (stream/error! resp (ex/ex-unsupported "put is not supported")))
      (deleteRange [req resp]
        (stream/error! resp (ex/ex-unsupported "deleterange is not supported")))
      (compact [req resp]
        ;; We return a dummy payload for compaction requests
        (stream/complete! resp (types/compaction-response req)))
      (range [req resp]
        (range-fn req resp))
      (txn [req resp]
        ;; XXX: debug
        (reset! last-txn-request req)
        (txn-fn req resp)))))

(def service
  (with-meta {}
    {'fetch.grpc.server/get-service make-service}))
