(ns fetch.fdb.payload
  "FoundationdB implementation of all key and value serialization and
   deserialization functions."
  (:refer-clojure :exclude [key val])
  (:require [fetch.fdb.space :as space]
            [fetch.fdb.kv    :as kv]
            [fetch.fdb.tuple :as tuple]
            [fetch.fdb.dir   :as dir]
            [exoscale.ex     :as ex]))

(defprotocol Serializer
  (key [_ k revision])
  (key-range [_ k])
  (lease-key [_ lease-id])
  (lease-ref-key [_ lease-id])
  (watch-key [_ watch-id])
  (watch-range-key [_ watch-id])
  (watch-event-key [_ watch-id revision])
  (schema-key [_])
  (revision-key [_])
  (decode-keyval [_ kv]))

(defn space-serializer
  [db instance-id]
  (let [dirs @(space/create-or-open (:fetch.fdb/database db) (str instance-id))]
    (reify Serializer
      (key [_ k revision]
        (space/from dirs :keys k revision))
      (key-range [_ k]
        (space/range dirs :keys k))
      (lease-key [_ lease-id]
        (space/from dirs :leases lease-id))
      (lease-ref-key [_ lease-id]
        (space/from dirs :lease-refs lease-id))
      (watch-key [_ watch-id]
        (space/from dirs :watches watch-id))
      (watch-range-key [_ watch-id]
        (space/from dirs :watch-ranges watch-id))
      (watch-event-key [_ watch-id revision]
        (space/from dirs :watch-events watch-id revision))
      (schema-key [_]
        (space/from dirs :schema))
      (revision-key [_]
        (space/from dirs :revision))
      ;; Technically deserialization, but oh well.
      (decode-keyval [_ kv]
        (let [[kba vba]                (kv/as-tuple kv)
              [k rev]                  (some-> (space/by-name dirs :keys)
                                               (dir/unpack kba)
                                               (tuple/expand))
              [lease create-rev value] (some-> vba
                                               tuple/decode-and-expand)]
          #:fetch.api{:key             k
                      :revision        rev
                      :lease           lease
                      :create-revision create-rev
                      :value           value})))))

(defn encode-val
  [lease-id create-revision value]
  (tuple/pack-vals lease-id create-revision value))

(defn lease-ttl
  [lease-ttl]
  (tuple/pack-vals lease-ttl))

(defn lease-ref
  [key]
  key)

(defn watch-range
  [begin end]
  (tuple/pack-vals begin end))

(defn watch-event
  [index key]
  (tuple/pack-vals index key))

(defn schema
  [version]
  (tuple/pack-vals version))

(defn encode-revision
  [revision]
  (tuple/pack-vals revision))

(defn decode-revision
  [ba]
  (some-> ba tuple/decode tuple/get-long))
