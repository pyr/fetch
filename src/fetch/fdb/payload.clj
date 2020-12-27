(ns fetch.fdb.payload
  "FoundationdB implementation of all key and value serialization and
   deserialization functions."
  (:refer-clojure :exclude [key val])
  (:require [fetch.fdb.space :as space]
            [fetch.fdb.kv    :as kv]
            [fetch.fdb.tuple :as tuple]))

(defprotocol Serializer
  (key [_ k revision])
  (key-range [_ k])
  (key-prefix [_ k prefix])
  (event-key [_ watch-id revision])
  (schema-key [_])
  (revision-key [_])
  (decode-keyval [_ kv])
  (watch-range [_])
  (watch-key [_ prefix])
  (watch-instance-key [_ instance]))

(defn space-serializer
  [dirs]
  (reify Serializer
    (key [_ k revision]
      (space/from dirs :keys k revision))
    (key-range [_ k]
      (space/range dirs :keys k))
    (key-prefix [_ k prefix]
      (space/range dirs :keys k prefix))
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
                                             (space/unpack kba)
                                             (tuple/expand))
            [lease create-rev value] (some-> vba
                                             tuple/decode-and-expand)]
        {:key             k
         :mod-revision    rev
         :lease           lease
         :create-revision create-rev
         :value           value}))
    (watch-range [_]
      (space/range dirs :watches))
    (watch-key [_ prefix]
      (space/from dirs :watches prefix))
    (watch-instance-key [_ instance]
      (space/from dirs :instances instance))
    (events-key [_ instance]
      (space/from dirs :events instance))))

(defn encode-val
  [lease-id create-revision value]
  (tuple/pack-vals lease-id create-revision value))

(defn lease-ttl
  [lease-ttl]
  (tuple/pack-vals lease-ttl))

(defn lease-ref
  [key]
  key)

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

(defn decode-watch
  [^bytes ba]
  (some-> ba tuple/decode-and-expand))

(defn encode-watch
  [instance id revision]
  (tuple/pack-vals instance id revision))
