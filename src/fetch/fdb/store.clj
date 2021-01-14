(ns fetch.fdb.store
  "Implementations of the `fetch.store/StorageEngine` protocol against
   FoundationDB"
  (:require [fetch.store                  :as store]
            [fetch.fdb.store.common       :as common]
            [fetch.fdb.store.interceptors :as ix]
            [fetch.fdb.db                 :as db]
            [fetch.fdb.op                 :as op]
            [fetch.fdb.kv                 :as kv]
            [fetch.fdb.payload            :as p]
            [qbits.auspex                 :as a]))

(defmulti create-if-absent
  "Create a key unless a previous revision exists for it"
  (comp some? :previous?))

(defmethod create-if-absent true
  [{:keys [tx dirs] :as ctx}]
  (assoc ctx :success? false :revision (common/highest-revision tx dirs)))

(defmethod create-if-absent false
  [{:keys [tx dirs key value lease] :as ctx}]
  (let [lease-id (or lease 0)
        revision (common/increment-revision tx dirs)]
    (op/set tx (p/key dirs key revision) (p/encode-val lease-id 0 value))
    (assoc ctx :success? true :revision revision :lease lease-id)))

(defmulti update-at-revision
  "Essentially a compare and set operation"
  (comp some? :previous?))

(defmethod update-at-revision true
  [{:keys [tx dirs key revision value lease previous] :as ctx}]
  (if (= revision (:mod-revision previous))
    (let [new-rev (common/increment-revision tx dirs)]
      (op/set tx (p/key dirs key new-rev)
              (p/encode-val lease 0 value))
      (assoc ctx :revision new-rev :success? true))
    (assoc ctx :revision (common/highest-revision tx dirs) :success? false)))

(defmethod update-at-revision false
  [ctx]
  (assoc ctx :success? false))

(defmulti delete-key
  "A compare and set operation specific to deletes"
  (comp some? :previous?))

(defmethod delete-key true
  [{:keys [tx dirs key revision value lease previous] :as ctx}]
  (if (= revision (:mod-revision previous))
    (let [new-rev (common/increment-revision tx dirs)]
      (op/set tx (p/key dirs key new-rev)
              (p/encode-val lease 0 value))
      (assoc ctx :revision new-rev :success? true))
    (assoc ctx :revision (common/highest-revision tx dirs) :success? false)))

(defmethod delete-key false
  [ctx]
  (assoc ctx :success? false))

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


(defn create-watch-instance
  [tx dirs instance]
  (let [rev (common/increment-revision tx dirs)]
    (op/set tx (p/watch-instance-key dirs instance) (p/encode-revision rev))))

(defn delete-watch-instance
  [tx dirs instance]
  (op/clear tx (p/watch-instance-key dirs instance))
  (op/clear tx (p/events-range dirs instance)))

(defn register-key-watch
  [tx dirs instance id prefix revision]
  (op/set tx (p/watch-key dirs prefix)
          (p/encode-watch instance id revision)))

(defn cancel-key-watch
  [tx dirs instance id]
  (doseq [kv (op/range-with-range (p/watch-range dirs) tx
                                  op/range-no-limit false)
          :let [watch (p/decode-watch (kv/v kv))]
          :when (= [instance id] (take 2 watch))]
    (op/clear tx (kv/k kv))))

(defn register-listener
  "Watch the notification key for this watcher's instance, only one key watched
   per connection to etcd.

   When the key is notified, a new transaction is generated which fetches all
   events then compacts them."
  [db tx dirs instance]
  (let [instance-key (p/watch-instance-key dirs instance)
        events-key   (p/events-range dirs instance)]
    (a/chain (op/watch tx instance-key)
             (db/write-transaction
              db
              (fn [tx _]
                (let [results (op/range-with-range tx events-key
                                                   op/range-no-limit false)]
                  ;; Let's clear events we're now reporting so as to not
                  ;; report them again to this watcher
                  (op/clear-range tx events-key)
                  {:continue?       true
                   ;; XXX: need to better format here
                   :events-by-watch (group-by :watch-id results)}))))))


(defrecord FDBStoreEngine [db]
  store/StorageEngine
  (create-if-absent [_ key value lease]
    (ix/write! db :create create-if-absent {:key       key
                                            :lookup?   true
                                            :mutation? true
                                            :value     value
                                            :lease     lease}))
  (update-at-revision [_ key revision value lease]
    (ix/write! db :update update-at-revision {:key       key
                                              :mutation? true
                                              :value     value
                                              :revision  revision
                                              :lease     lease}))
    (delete-key [_ key revision]
      (ix/write! db :delete delete-key {:key       key
                                        :revision  revision
                                        :mutation? true}))

  (count-keys [_ prefix]
    (ix/read db :count (ix/out count-keys [:count]) {:prefix prefix}))
  (range-keys [_ revision limit prefix]
    (ix/read db :range (ix/out range-keys [:range]) {:revision revision
                                                     :limit    limit
                                                     :prefix   prefix}))
  (get-at-revision [_ key revision]
    (ix/read db :get (ix/out get-at-revision [:result]) {:key      key
                                                         :lookup?  true
                                                         :revision revision}))
  (get-latest [_ key]
    (ix/read db :get-latest get-latest {:key     key
                                        :lookup? true}))
  (create-watch-instance [_ instance]
    (ix/write! db :create-instance create-watch-instance {:instance instance}))
  (delete-watch-instance [_ instance]
    (ix/write! db :delete-instance delete-watch-instance {:instance instance}))
  (register-key-watch [_ instance id prefix revision]
    (ix/write! db :register-watch register-key-watch {:instance instance
                                                      :prefix   prefix
                                                      :revision revision
                                                      :id       id}))
  (cancel-key-watch [_ instance id]
    (ix/write! db :cancel-watch register-key-watch {:instance instance
                                                    :id       id}))
  (register-watch-listener [_ i]
    (ix/write! db :register-listener register-listener {:instance i})))
