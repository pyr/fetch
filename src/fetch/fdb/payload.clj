(ns fetch.fdb.payload
  "FoundationdB implementation of all key and value serialization and
   deserialization functions."
  (:refer-clojure :exclude [key val])
  (:require [fetch.fdb.space :as space]
            [fetch.fdb.kv    :as kv]
            [fetch.fdb.tuple :as tuple]
            [fetch.fdb.dir   :as dir]))

(defprotocol Serializer
  (key [_ k revision])
  (key-range [_ k])
  (watch-key [_ watch-id])
  (event-key [_ watch-id revision])
  (schema-key [_])
  (revision-key [_])
  (decode-keyval [_ kv]))

(def instance-id #uuid "4a7517f8-40f0-41ad-9e1d-cae1397c1b23")

(defn space-serializer
  [db instance-id]
  (let [dirs @(space/create-or-open (:fetch.fdb.db/database db)
                                    (str instance-id))]
    (reify Serializer
      (key [_ k revision]
        (space/from dirs :keys k revision))
      (key-range [_ k]
        (space/range dirs :keys k))
      (watch-key [_ watch-id]
        (space/from dirs :watches watch-id))
      (event-key [_ watch-id revision]
        (space/from dirs :events watch-id revision))
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
          {:key             k
           :mod-revision    rev
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
