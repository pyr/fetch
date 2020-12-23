(ns fetch.fdb.kv
  (:import com.apple.foundationdb.KeyValue))

(defn k
  [^KeyValue kv]
  (.getKey kv))

(defn v
  [^KeyValue kv]
  (.getValue kv))

(defn as-tuple
  [^KeyValue kv]
  [(k kv) (v kv)])

(defn build
  [k v]
  (KeyValue. ^bytes k ^bytes v))
