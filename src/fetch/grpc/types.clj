(ns fetch.grpc.types
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.set :as set]
            [exoscale.ex :as ex])
  (:import com.google.protobuf.ByteString
           exoscale.etcd.api.ResponseHeader
           exoscale.etcd.api.RangeResponse
           exoscale.etcd.api.DeleteRangeResponse
           exoscale.etcd.api.PutResponse
           exoscale.etcd.api.ResponseOp
           exoscale.etcd.api.TxnResponse
           exoscale.etcd.api.CompactionResponse
           exoscale.etcd.api.AuthenticateResponse
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

(defn oneof-transformer
  [source-field type-field transformers]
  (fn [x]
    (let [bx     (bean x)
          case   (-> bx (get source-field) .toString str/lower-case keyword)
          target (get transformers case)]
      (when (nil? target)
        (ex/ex-not-found! (str "unknown case:" case)))
      (merge {type-field (:name target)}
             (when-let [f (or (:fn target) identity)]
               (f (get bx (:key target))))))))

;; Request types
(def range-request->map
  (transformer {:key               :etcd.api.kv/key
                :rangeEnd          :etcd.api.kv/range-end
                :limit             :etcd.api.kv/limit
                :revision          :etcd.api.kv/revision
                :sortOrder         :etcd.api.kv/sort-order
                :sortTarget        :etcd.api.kv/sort-target
                :serializable      :etcd.api.kv/serializable?
                :keysOnly          :etcd.api.kv/keys-only?
                :countOnly         :etcd.api.kv/count-only?
                :minModRevision    :etcd.api.kv/min-mod-revision
                :maxModRevision    :etcd.api.kv/max-mod-revision
                :minCreateRevision :etcd.api.kv/min-create-revision
                :maxCreateRevision :etcd.api.kv/max-create-revision}
               {:etcd.api.kv/key         byte-string->byte-array
                :etcd.api.kv/range-end   byte-string->byte-array
                :etcd.api.kv/sort-order  enum->kw
                :etcd.api.kv/sort-target enum->kw}))

(def put-request->map
  (transformer {:key         :etcd.api.kv/key
                :value       :etcd.api.kv/value
                :lease       :etcd.api.kv/lease
                :prevKv      :etcd.api.kv/previous?
                :ignoreValue :etcd.api.kv/ignore-value?
                :ignoreLease :etcd.api.kv/ignore-lease?}
               {:etcd.api.kv/key   byte-string->byte-array
                :etcd.api.kv/value byte-string->byte-array}))

(def delete-range-request->map
  (transformer {:key      :etcd.api.kv/key
                :rangeEnd :etcd.api.kv/range-end
                :prevKv   :etcd.api.kv/previous?}
               {:etcd.api.kv/key       byte-string->byte-array
                :etcd.api.kv/range-end byte-string->byte-array}))

(declare txn-request->map)

(def request-oneof->map
  (oneof-transformer :requestCase
                     :etcd.api.kv/type
                     {:request_range        {:name :range
                                             :fn   range-request->map
                                             :key  :requestRange}
                      :request_put          {:name :put
                                             :fn   put-request->map
                                             :key  :requestPut}
                      :request_delete_range {:name :delete-range
                                             :fn   delete-range-request->map
                                             :key  :requestDeleteRange}
                      :request_txn          {:name :txn
                                             :fn   txn-request->map
                                             :key  :requestTxn}
                      :request_not_set      {:name :not-set}}))

(def request-op->map
  (transformer
   {:request :etcd.api.kv/request}
   {:etcd.api.kv/request request-oneof->map}
   (fn [x] (merge (:etcd.api.kv/request x) x))))

(def target-union->map
  (oneof-transformer :targetUnionCase
                     :etcd.api.kv/type
                     {:target_union_version
                      {:name :version
                       :key  :targetUnionVersion}
                      :target_union_create_revision
                      {:name :create-revision
                       :key  :targetUnionCreateRevision}
                      :target_union_mod_revision
                      {:name :mod-revision
                       :key  :targetUnionModRevision}
                      :target_union_value
                      {:name :value
                       :key :targetUnionValue}
                      :target_union_not_set
                      {:name :not-set}}))
(def compare->map
  (transformer {:result   :etcd.api.kv/result
                :target   :etcd.api.kv/target
                :key      :etcd.api.kv/key
                :info     :etcd.api.kv/info
                :rangeEnd :etcd.api.kv/range-end}
               {}
               (fn [x] (merge (:etcd.api.kv/info x) x))))

(def txn-request->map
  (transformer {:compare :etcd.api.kv/compare
                :success :etcd.api.kv/success
                :failure :etcd.api.kv/failure}
               {:etcd.api.kv/compare (partial mapv compare->map)
                :etcd.api.kv/success (partial mapv request-op->map)
                :etcd.api.kv/failure (partial mapv request-op->map)}))

(def compaction-request->map
  (transformer {:revision :etcd.api.kv/revision
                :physical :etcd.api.kv/physical}
               {}))

(def authenticate-request->map
  (transformer {:name     :etcd.api.kv/name
                :password :etcd.api.kv/password}
               {}))

;; Responses with specs
(s/def :etcd.api.kv/cluster-id nat-int?)
(s/def :etcd.api.kv/member-id nat-int?)
(s/def :etcd.api.kv/revision nat-int?)
(s/def :etcd.api.kv/raft-term nat-int?)
(s/def :etcd.api.kv/key bytes?)
(s/def :etcd.api.kv/previous bytes?)
(s/def :etcd.api.kv/range-end bytes?)
(s/def :etcd.api.kv/id nat-int?)
(s/def :etcd.api.kv/create-revision nat-int?)
(s/def :etcd.api.kv/mod-revision nat-int?)
(s/def :etcd.api.kv/version nat-int?)
(s/def :etcd.api.kv/lease nat-int?)
(s/def :etcd.api.kv/key-value (s/keys))
(s/def :etcd.api.kv/kv (s/keys :req [:etcd.api.kv/key]))
(s/def :etcd.api.kv/previous-keys (s/coll-of :etcd.api.kv/kv))
(s/def :etcd.api.kv/deleted? boolean?)
(s/def :etcd.api.kv/more? boolean?)
(s/def :etcd.api.kv/physical? boolean?)

;; All keys are namespaced so will be checked anyway
(s/def ::optional (s/keys))

(defn map->response-header
  [{:etcd.api.kv/keys [cluster-id member-id revision raft-term] :as m}]
  (ex/assert-spec-valid ::optional (or m {}))
  (cond-> (ResponseHeader/newBuilder)
    (some? cluster-id)
    (.setClusterId cluster-id)
    (some? member-id)
    (.setMemberId member-id)
    (some? revision)
    (.setRevision revision)
    (some? raft-term)
    (.setRaftTerm raft-term)
    true
    (.build)))

(defn map->key-value
  [{:etcd.api.kv/keys [key create-revision mod-revision version value lease]
    :as               m}]
  (ex/assert-spec-valid ::optional (or m {}))
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
(s/def :etcd.api.kv/event-type (into #{} (keys kw->event-type)))

(defn map->event
  [{:etcd.api.kv/keys [event-type kv previous] :as m}]
  (ex/assert-spec-valid ::optional (or m {}))
  (cond-> (Event/newBuilder)
    (some? kv)
    (.setKv (map->key-value kv))
    (some? previous)
    (.setPrevKv (map->key-value kv))
    (some? type)
    (.setType (kw->event-type event-type))))

(defn map->range-response
  [{:etcd.api.kv/keys [header kvs more?] :as m}]
  (ex/assert-spec-valid ::optional (or m {}))
  (cond-> (RangeResponse/newBuilder)
    (some? header)
    (.setHeader (map->response-header header))
    (some? kvs)
    (.addAllKvs (mapv map->key-value kvs))
    (some? more?)
    (.setMore more?)
    true
    (.setCount (count kvs))
    true
    (.build)))

(defn map->put-response
  [{:etcd.api.kv/keys [header previous] :as m}]
  (ex/assert-spec-valid ::optional (or m {}))
  (cond-> (PutResponse/newBuilder)
    (some? header)
    (.setHeader (map->response-header header))
    (some? previous)
    (.setPrevKv (map->key-value previous))
    true
    (.build)))

(defn map->delete-range-response
  [{:etcd.api.kv/keys [header deleted? previous-keys] :as m}]
  (ex/assert-spec-valid ::optional (or m {}))
  (cond-> (DeleteRangeResponse/newBuilder)
    (some? header)
    (.setHeader (map->response-header header))
    (some? deleted?)
    (.setDeleted deleted?)
    (some? previous-keys)
    (.addAllPrevKvs (mapv map->key-value previous-keys))
    true
    (.build)))

(declare map->txn-response)
(defmulti map->response-op :etcd.api.kv/response-type)

(defmethod map->response-op :range
  [x]
  (-> (ResponseOp/newBuilder)
      (.setResponseRange (map->range-response x))
      (.build)))

(defmethod map->response-op :put
  [x]
  (-> (ResponseOp/newBuilder)
      (.setResponsePut (map->put-response x))
      (.build)))

(defmethod map->response-op :delete-range
  [x]
  (-> (ResponseOp/newBuilder)
      (.setResponsePut (map->delete-range-response x))
      (.build)))

(defmethod map->response-op :txn
  [x]
  (-> (ResponseOp/newBuilder)
      (.setResponsePut (map->txn-response x))
      (.build)))

(defn map->txn-response
  [{:etcd.api.kv/keys [header succeeded? responses] :as m}]
  (ex/assert-spec-valid ::optional (or m {}))
  (cond-> (TxnResponse/newBuilder)
    (some? header)
    (.setHeader (map->response-header header))
    (some? succeeded?)
    (.setSucceeded succeeded?)
    (some? responses)
    (.addAllResponses (mapv map->response-op responses))
    true
    (.build)))

(defn map->compaction-response
  [{:etcd.api.kv/keys [header revision physical?] :as m}]
  (ex/assert-spec-valid ::optional (or m {}))
  (cond-> (CompactionResponse/newBuilder)
    (some? header)
    (.setHeader (map->response-header header))
    (some? revision)
    (.setRevision revision)
    (some? physical?)
    (.setPhysical physical?)
    true
    (.build)))

(defn map->authenticate-response
  [{:etcd.api.kv/keys [token] :as m}]
  (ex/assert-spec-valid ::optional (or m {}))
  (cond-> (AuthenticateResponse/newBuilder)
    (some? token)
    (.setToken token)
    true
    (.build)))
