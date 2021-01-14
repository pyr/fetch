(ns fetch.fdb.store.lookups
  (:require [fetch.fdb.store.common :as common]
            [fetch.fdb.op           :as op]
            [fetch.fdb.kv           :as kv]
            [fetch.fdb.payload      :as p]))

(defn count-keys
  [{:keys [tx dirs prefix]}]
  (let [kvs (op/reverse-range tx (p/key-range dirs prefix))]
    [(common/highest-revision tx dirs)
     (->> kvs
          (map kv/k)
          (map (partial p/decode-key dirs))
          (map :key)
          (distinct)
          (count))]))

(defn range-keys
  [{:keys [tx dirs revision limit prefix]}]
  (let [range (p/key-prefix dirs prefix)]
    (->> (op/reverse-range tx range limit)
         (map kv/k)
         (map (partial p/decode-key dirs))
         (partition-by :key)
         (map first)
         (filter #(>= (:mod-revision %) revision)))))

(defn get-at-revision
  [{:keys [tx dirs key revision]}]
  (some-> (op/get tx (p/key dirs key revision))
          p/decode-value
          (assoc :mod-revision revision :key key)))

(defn get-latest
  "Find the latest version, we rely on the lookup-previous interceptor
   which already did the work for us"
  [{:keys [previous previous?] :as ctx}]
  (assoc ctx :result previous :success? previous?))
