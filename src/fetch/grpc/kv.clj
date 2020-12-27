(ns fetch.grpc.kv
  "Shim to hide the guts of GRPC java to downstream code.
   We take the opportunity here to short-circuit for the
   cases we are interested in, this smoothes over parts of
   the etcd API that Kubernetes does not care about. Actual
   operations are then carried out by "
  (:require [fetch.api.proto   :as proto]
            [fetch.grpc.types  :as types]
            [fetch.grpc.stream :as stream]
            [exoscale.ex       :as ex])
  (:import exoscale.etcd.api.KVGrpc$KVImplBase))

(defn- wrap-fn
  "A wrapper for unary grpc functions"
  [kvdb f coax-in]
  (fn [req resp]
    (try
      (stream/complete! resp (f kvdb (coax-in req)))
      (catch Exception e
        (stream/error! resp e)))))

(defonce last-txn-request (atom nil))

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

(defn txn-handler
  [kvdb {:keys [type unsupported] :as req}]
  (when (some? unsupported)
    (ex/ex-unsupported! unsupported))
  (case type
      :update  (update-handler kvdb req)
      :delete  (delete-handler kvdb req)
      :create  (create-handler kvdb req)
      :compact types/txn-compaction-response))

(defn- get-handler
  [kvdb {:keys [key revision]}]
  (let [[rev kv] (if (pos? revision)
                   (proto/get-at-revision kvdb key revision)
                   (proto/get-latest kvdb key))]
    (types/get-response rev kv)))

(defn- range-prefix-handler
  [kvdb {:keys [key revision range-end count-only? limit]}]
  (let [ba     (byte-array (concat (map byte (drop-last range-end))
                               [(-> range-end last byte dec byte)]))
        prefix (cond-> (String. ba "UTF-8") (not= \/ (last s)) (str "/"))]
    (if count-only?
      (let [[rev num-keys] (proto/count-keys kvdb prefix)]
        (types/range-count-response rev num-keys))
      (let [[rev kvs] (proto/range-keys kvdb revision limit key prefix)]
        (types/range-response rev limit kvs)))))

(defn range-handler
  [kvdb {:keys [unsupported range-end] :as req}]
  (when (some? unsupported)
    (ex/ex-unsupported! unsupported))
  (if (zero? (count range-end))
    (get-handler kvdb req)
    (range-prefix-handler kvdb req)))

(defn- make-kv-service
  "Kubernetes only needs two out of five signatures implemented for its
   interaction with etcd: txn and range. create, delete, and update operations
   are performed as part of transactions"
  [{::proto/keys [kvdb]}]
  (let [range-fn (wrap-fn kvdb range-handler types/range-request->map)
        txn-fn   (wrap-fn kvdb txn-handler types/txn-request->map)]
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
        (reset! last-txn-request raw-req)
        (txn-fn req resp)))))

(def kv
  (with-meta {}
    {'fetch.grpc.server/get-service make-kv-service}))
