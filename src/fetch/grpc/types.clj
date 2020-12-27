(ns fetch.grpc.types
  (:require [clojure.string :as str]
            [clojure.set :as set])
  (:import com.google.protobuf.ByteString
           exoscale.etcd.api.ResponseHeader
           exoscale.etcd.api.RangeRequest
           exoscale.etcd.api.RangeResponse
           exoscale.etcd.api.DeleteRangeRequest
           exoscale.etcd.api.DeleteRangeResponse
           exoscale.etcd.api.LeaseGrantRequest
           exoscale.etcd.api.LeaseGrantResponse
           exoscale.etcd.api.PutRequest
           exoscale.etcd.api.PutResponse
           exoscale.etcd.api.ResponseOp
           exoscale.etcd.api.TxnRequest
           exoscale.etcd.api.TxnResponse
           exoscale.etcd.api.Compare
           exoscale.etcd.api.RequestOp
           exoscale.etcd.api.CompactionResponse
           exoscale.etcd.api.CompactionRequest
           exoscale.etcd.api.WatchRequest
           exoscale.etcd.api.WatchResponse
           exoscale.etcd.api.KeyValue
           exoscale.etcd.api.Event
           exoscale.etcd.api.Event$EventType))

;; Utility functions
(defn update-vals
  [input coaxes]
  (into {} (for [[k v] input :let [coax (get coaxes k)]]
             [k (cond-> v (some? coax) coax)])))

(defn byte-string->byte-array
  [^ByteString bs]
  (.toByteArray bs))

(defprotocol AsByteString
  :extend-via-metadata true
  (-->byte-string [this]))

(defn to-string
  [^ByteString bs]
  (.toStringUtf8 bs))

(defn ->byte-string
  [x]
  (cond
    (bytes? x)                           (ByteString/copyFrom x)
    (string? x)                          (ByteString/copyFromUtf8 x)
    (satisfies? AsByteString x)          (-->byte-string x)
    (contains? (meta x) `-->byte-string) (-->byte-string x)))

(defn enum->kw
  [e]
  (-> e bean :valueDescriptor bean :name str/lower-case keyword))

(defn transformer
  ([renames coaxes]
   (fn [x]
     (-> (bean x)
         (select-keys (keys renames))
         (set/rename-keys renames)
         (update-vals coaxes))))
  ([renames coaxes finalizer]
   (comp finalizer (transformer renames coaxes))))

;; Request types
(defn range-request->map
  [^RangeRequest req]
  (cond-> {:key         (to-string (.getKey req))
           :range-end   (byte-string->byte-array (.getRangeEnd req))
           :limit       (.getLimit req)
           :revision    (.getRevision req)
           :count-only? (.getCountOnly req)}
    (true? (.getKeysOnly req))         (assoc :unsupported "KeysOnly")
    (true? (.getSerializable req))     (assoc :unsupported "Serializable")
    (pos? (.getMaxCreateRevision req)) (assoc :unsupported "MaxCreateRevision")
    (pos? (.getMinCreateRevision req)) (assoc :unsupported "MinCreateRevision")
    (pos? (.getMaxModRevision req))    (assoc :unsupported "MaxModRevision")
    (pos? (.getMinModRevision req))    (assoc :unsupported "MinModRevision")
    (pos? (.getSortOrderValue req))    (assoc :unsupported "SortOrder")
    (pos? (.getSortTargetValue req))   (assoc :unsupported "SortTarget")))

(defn put-request->map
  [^PutRequest req]
  {:key           (to-string (.getKey req))
   :value         (byte-string->byte-array (.getValue req))
   :previous?     (.getPrevKv req)
   :ignore-lease? (.getIgnoreLease req)
   :ignore-value? (.getIgnoreValue req)})

(defn delete-range-request->map
  [^DeleteRangeRequest req]
  {:key       (to-string (.getKey req))
   :previous? (.getPrevKv req)})

(def ^:const request-op-types
  {RequestOp/REQUEST_PUT_FIELD_NUMBER          :request-put
   RequestOp/REQUEST_RANGE_FIELD_NUMBER        :request-range
   RequestOp/REQUEST_DELETE_RANGE_FIELD_NUMBER :request-delete-range})

(defn request-op-type
  [^RequestOp op]
  (let [num (some-> op .getRequestCase .getNumber)]
    (get request-op-types num :unknown)))

(defn request-op->map
  [op-type ^RequestOp payload]
  (case op-type
    :request-put          (put-request->map (.getRequestPut payload))
    :request-range        (range-request->map (.getRequestRange payload))
    :request-delete-range (range-request->map (.getRequestDeleteRange payload))
    nil))

(defn compare->map
  [^Compare payload]
  (let [target-type (some-> payload .getTargetUnionCase .getNumber)]
    (cond-> {:result (enum->kw (.getResult payload))
             :target (enum->kw (.getTarget payload))
             :key    (to-string (.getKey payload))
             :type   target-type}
      (= target-type Compare/MOD_REVISION_FIELD_NUMBER)
      (assoc :mod-revision (.getModRevision payload))
      (= target-type  Compare/VERSION_FIELD_NUMBER)
      (assoc :version (.getVersion payload)))))

(defn txn-success-put
  [^TxnRequest req]
  (let [last-success ^RequestOp (-> (.getSuccessList req) last)
        put-request  ^PutRequest (.getRequestPut last-success)]
    (cond-> {:value (byte-string->byte-array (.getValue put-request))
             :lease (.getLease put-request)}
      (true? (.getIgnoreLease req)) (assoc :unsupported "ignoreLease")
      (true? (.getIgnoreValue req)) (assoc :unsupported "ignoreValue")
      (true? (.getPrevKv req))      (assoc :unsupported "prevKv"))))

(defn txn-delete-key
  [^TxnRequest req]
  (let [last-success ^RequestOp (-> (.getSuccessList req) last)
        delete-req   ^DeleteRangeRequest (.getRequestDeleteRange last-success)]
    (to-string (.getKey delete-req))))

(defn txn-request->map
  "A minimal request translator, only the bare minimum is decoded, avoiding
   reflection."
  [^TxnRequest req]
  (let [compare-count (.getCompareCount req)
        success-count (.getSuccessCount req)
        failure-count (.getFailureCount req)
        counts        [compare-count success-count failure-count]
        compare       (some-> (.getCompareList req) (first) (compare->map))
        types         {:failure       (some-> (.getFailureList req)
                                              (first)
                                              (request-op-type))
                       :success       (some-> (.getSuccessList req)
                                              (last)
                                              (request-op-type))
                       :first-success (some-> (.getSuccessList req)
                                              (first)
                                              (request-op-type))}]
    (cond

       ;; A kubernetes create or update request
      (and
       (= [1 1 1] counts)
       (= :mod (:target compare))
       (= :equal (:result compare))
       (= :request-put (:success types))
       (= :request-range (:failure types)))
      (assoc (txn-success-put req)
             :key (:key compare)
             :mod-revision (:mod-revision compare)
             :type (if (zero? (:mod-revision compare))
                     :create
                     :update))

      ;; First kubernetes delete variant, no comparison
      (and
       (= [0 0 2] counts)
       (= :request-range (:first-success types))
       (= :request-delete-range (:success types)))
      {:type :delete
       :key  (txn-delete-key req)
       :mod  0}

      ;; Second kubernetes delete variant, with comparison
      (and
       (= [1 1 1] counts)
       (= :mod (:target compare))
       (= :equal (:result compare))
       (= :request-delete-range (:success types))
       (= :request-range (:failure types)))
      {:type :delete
       :key  (txn-delete-key req)
       :mod  (:mod-revision compare)}

      ;; A kubernetes compaction request
      (and
       (= [1 1 1] counts)
       (= :version (:target compare))
       (= :equal (:result compare))
       (= "compact_rev_key" (:key compare))
       (= :request-put (:success types))
       (= :request-range (:failure types)))
      {:type :compact})))

(defn map->key-value
  [{:keys [key create-revision mod-revision version value lease]}]
  (cond-> (KeyValue/newBuilder)
    (some? key)
    (.setKey (->byte-string key))
    (some? create-revision)
    (.setCreateRevision create-revision)
    (some? mod-revision)
    (.setModRevision mod-revision)
    (some? version)
    (.setVersion version)
    (some? value)
    (.setValue (->byte-string value))
    (some? lease)
    (.setLease lease)
    true
    (.build)))

(def kw->event-type {:put    Event$EventType/PUT
                     :delete Event$EventType/DELETE})

(defn map->event
  [{:keys [event-type kv previous]}]
  (cond-> (Event/newBuilder)
    (some? kv)
    (.setKv (map->key-value kv))
    (some? previous)
    (.setPrevKv (map->key-value kv))
    (some? type)
    (.setType (kw->event-type event-type))))

(defn ^ResponseHeader rev-header
  [rev]
  (-> (ResponseHeader/newBuilder)
      (.setRevision rev)
      (.build)))

(defn ^TxnResponse create-already-exists-response
  [rev]
  (-> (TxnResponse/newBuilder)
      (.setSucceeded false)
      (.setHeader (rev-header rev))))

(defn ^TxnResponse create-success-response
  [rev]
  (-> (TxnResponse/newBuilder)
      (.setSucceeded true)
      (.setHeader (rev-header rev))
      (.addAllResponses
       [(-> (ResponseOp/newBuilder)
            (.setResponsePut (-> (PutResponse/newBuilder)
                                 (.setHeader (rev-header rev))
                                 (.build)))
            (.build))])
      (.build)))

(defn ^TxnResponse update-success-response
  [rev]
  (-> (TxnResponse/newBuilder)
      (.setSucceeded true)
      (.setHeader (rev-header rev))
      (.addAllResponses
       [(-> (ResponseOp/newBuilder)
            (.setResponsePut (-> (PutResponse/newBuilder)
                                 (.setHeader (rev-header rev))
                                 (.build)))
            (.build))])
      (.build)))

(defn ^TxnResponse update-failure-response
  [rev kv]
  ;; XXX: bad payload
  (-> (TxnResponse/newBuilder)
      (.setSucceeded false)
      (.setHeader (rev-header rev))
      (.build)))

(defn ^TxnResponse delete-response
  [rev kv succeeded?]
  ;; XXX: bad payload
  (-> (TxnResponse/newBuilder)
      (.setSucceeded succeeded?)
      (.setHeader (rev-header rev))
      (.addAllResponses
       [(-> (ResponseOp/newBuilder)
            (.setResponseDeletRange (-> (DeleteRangeResponse/newBuilder)
                                        (.setHeader (rev-header rev))
                                        (.build)))
            (.build))])
      (.build)))

(def txn-compaction-response
  (-> (TxnResponse/newBuilder)
      (.setHeader (-> (ResponseHeader/newBuilder)
                      (.build)))
      (.setSucceeded false)
      (.addAllResponses
       [(-> (ResponseOp/newBuilder)
            (.setResponseRange
             (-> (RangeResponse/newBuilder)
                 (.setHeader (.build (ResponseHeader/newBuilder)))
                 (.addAllKvs [(.build (KeyValue/newBuilder))])
                 (.setCount 1)
                 (.build)))
            (.build))])
      (.build)))

(defn compaction-response
  [^CompactionRequest req]
  (-> (CompactionResponse/newBuilder)
      (.setHeader (rev-header (.getRevision req)))
      (.build)))

(defn lease-grant-response
  [^LeaseGrantRequest req]
  (-> (LeaseGrantResponse/newBuilder)
      (.setHeader (-> (ResponseHeader/newBuilder) (.build)))
      (.setID (.getTTL req))
      (.setTTL (.getTTL req))
      (.build)))

(defn range-response
  [rev limit kvs]
  (let [kvcount (count kvs)
        kvs     (take limit kvs)]
    (-> (RangeResponse/newBuilder)
        (.setHeader (rev-header rev))
        (.setCount (count kvs))
        (cond-> (pos? kvcount)
          (.addAllKvs (mapv map->key-value kvs)))
        (cond-> (> kvcount (count kvs))
          (.setMore true))
        (.build))))

(defn get-response
  [rev kv]
  (-> (RangeResponse/newBuilder)
      (.setHeader (rev-header rev))
      (.setCount (if (some? kv) 1 0))
      (cond-> (some? kv)
        (.addKv (map->key-value kv)))
      (.build)))

(defn range-count-response
  [rev num-keys]
  (-> (RangeResponse/newBuilder)
      (.setHeader (rev-header rev))
      (.setCount num-keys)
      (.build)))

(defn watch-request->map
  [^WatchRequest req]
  (if (= WatchRequest/CREATE_REQUEST_FIELD_NUMBER
         (some-> req .getRequestUnionCase .getNumber))
    {:type           :create
     :key            (some-> req .getCreateRequest .getKey to-string)
     :start-revision (some-> req .getCreateRequest .getStartRevision)}
    {:type     :cancel
     :watch-id (-> req .getCancelRequest .getWatchId)}))

(defmulti map->watch-response :type)

(defmethod map->watch-response :create
  [{:keys [watch-id]}]
  (-> (WatchResponse/newBuilder)
      (.setWatchId watch-id)
      (.setCreated true)
      (.build)))

(defmethod map->watch-response :cancel
  [{:keys [watch-id]}]
  (-> (WatchResponse/newBuilder)
      (.setWatchId watch-id)
      (.setCancelled true)
      (.setCancelReason "watch closed")
      (.build)))

(defmethod map->watch-response :events
  [{:keys [watch-id revision events]}]
  (-> (WatchResponse/newBuilder)
      (.setWatchId watch-id)
      (.build)))
