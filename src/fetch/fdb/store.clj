(ns fetch.fdb.store
  (:require [fetch.store            :as store]
            [fetch.fdb.store.common :as common]
            [fetch.fdb.db           :as db]
            [fetch.fdb.op           :as op]
            [fetch.fdb.kv           :as kv]
            [fetch.fdb.payload      :as p]
            [qbits.auspex           :as a]))

(defn create-if-absent
  [tx dirs key value lease]
  (let [previous (common/previous tx dirs key)
        lease-id (if (some? lease) lease 0)]
    (if (some? previous)
      [(common/highest-revision tx dirs) false]
      (let [new-rev (common/increment-revision tx dirs)]
        @(a/chain (op/set tx (p/key dirs key new-rev)
                          (p/encode-val lease-id 0 value))
                  (constantly [new-rev true])))) ))

(defn update-at-revision
  [tx dirs key revision value lease]
  (let [previous (common/previous tx dirs key)]
    (if (= revision (:mod-revision previous))
      (let [new-rev (common/increment-revision tx dirs)]
        @(a/chain (op/set tx (p/key dirs key new-rev)
                          (p/encode-val lease 0 value))
                  (constantly [new-rev true])))
      [(common/highest-revision tx dirs) false])))

(defn count-keys
  [tx dirs prefix]
  @(a/chain (op/reverse-range tx (p/key-range dirs prefix))
            (fn [kvs]
              [(common/highest-revision tx dirs)
               (->> kvs
                    (map kv/k)
                    (map (partial p/decode-key dirs))
                    (map :mod-revision)
                    (distinct)
                    (count))])))

(defn range-keys
  [tx dirs revision limit prefix]
  (->> @(op/reverse-range tx (p/key-range dirs prefix) limit)
       (map kv/k)
       (map (partial p/decode-key dirs))
       (partition-by :mod-revision)
       (map first)
       (filter #(>= (:mod-revision %) revision))))

(defn get-at-revision
  [tx dirs key revision]
  (some->> @(op/get tx (p/key dirs key revision))
           (p/decode-keyval dirs)))

(defn get-latest
  [tx dirs key]
  (common/previous tx dirs key))

(defn delete-key
  [tx dirs key revision]
  (let [previous (common/previous tx dirs key)]
    (if (= revision (:mod-revision previous))
      (let [new-rev (common/increment-revision tx dirs)]
        @(a/chain (op/clear-range tx (p/key-range dirs key))
                  (constantly [new-rev true])))
      [revision false])))

(defn create-watch-instance
  [tx dirs instance]
  (let [rev (common/increment-revision tx dirs)]
    @(op/set tx (p/watch-instance-key dirs instance) (p/encode-revision rev))))

(defn delete-watch-instance
  [tx dirs instance]
  @(op/clear tx (p/watch-instance-key dirs instance))
  @(op/clear tx (p/events-range dirs instance)))

(defn register-key-watch
  [tx dirs instance id prefix revision]
  @(op/set tx (p/watch-key dirs prefix)
           (p/encode-watch instance id revision)))

(defn cancel-key-watch
  [tx dirs instance id]
  (doseq [kv (op/range-with-range (p/watch-range dirs) tx
                                  op/range-no-limit false)
          :let [watch (p/decode-watch (kv/v kv))]
          :when (= [instance id] (take 2 watch))]
    @(op/clear tx (kv/k kv))))

(defn register-watch-listener
  "Watch the notification key for this watcher's instance, only one key watched
   per connection to etcd.

   When the key is notified, a new transaction is generated which fetches all
   events then compacts them."
  [db tx dirs instance]
  (let [instance-key (p/watch-instance-key dirs instance)
        events-key   (p/events-range dirs instance)]
    (a/chain (op/watch tx instance-key)
             (db/run-in-transaction
              db
              (fn [tx _]
                (let [results @(op/range-with-range tx events-key
                                                    op/range-no-limit false)]
                  ;; Let's clear events we're now reporting so as to not
                  ;; report them again to this watcher
                  @(op/clear-range tx events-key)
                  {:continue?       true
                   ;; XXX: need to better format here
                   :events-by-watch (group-by :watch-id results)}))))))

(defrecord FDBStoreEngine [db]
  store/StorageEngine
  (create-if-absent [_ key value lease]
    (db/run-in-transaction db
                           (fn [tx dirs]
                             (create-if-absent tx dirs key value lease))))
  (update-at-revision [_ key revision value lease]
    (db/run-in-transaction db
                           (fn [tx dirs]
                             (update-at-revision tx dirs key revision
                                                 value lease))))
  (count-keys [_ prefix]
    (db/run-in-transaction db
                           (fn [tx dirs]
                             (count-keys tx dirs prefix))))
  (range-keys [_ revision limit prefix]
    (db/run-in-transaction db
                           (fn [tx dirs]
                             (range-keys tx dirs revision limit prefix))))
  (get-at-revision [_ key revision]
    (db/run-in-transaction db
                           (fn [tx dirs]
                             (get-at-revision tx dirs key revision))))
  (get-latest [_ key]
    (db/run-in-transaction db
                           (fn [tx dirs]
                             (get-latest tx dirs key))))
  (delete-key [_ key revision]
    (db/run-in-transaction db
                           (fn [tx dirs]
                             (delete-key tx dirs key revision))))
  (create-watch-instance [_ instance]
    (db/run-in-transaction db
                           (fn [tx dirs]
                             (create-watch-instance tx dirs instance))))
  (delete-watch-instance [_ instance]
    (db/run-in-transaction db
                           (fn [tx dirs]
                             (delete-watch-instance tx dirs instance))))
  (register-key-watch [_ instance id prefix revision]
    (db/run-in-transaction db
                           (fn [tx dirs]
                             (register-key-watch tx dirs instance id
                                                 prefix revision))))
  (cancel-key-watch [_ instance id]
    (db/run-in-transaction db
                           (fn [tx dirs]
                             (cancel-key-watch tx dirs instance id))))
  (register-watch-listener [_ instance]
    (db/run-in-transaction db
                           (fn [tx dirs]
                             (register-watch-listener db tx dirs instance)))))

(comment
  (require '[com.stuartsierra.component :as component]
           '[fetch.fdb.op :as op]
           '[fetch.fdb.db :as db]
           '[fetch.fdb.payload :as p]
           '[fetch.fdb.kv :as kv])

  (def handle
    (component/start
     (db/make-database {::db/cluster-file "/etc/foundationdb/fdb.cluster"})))

  (def store (component/start (map->FDBStoreEngine {:db handle})))

  handle

  store
  (store/create-if-absent store "foo" (.getBytes "bar") 2)
  (p/encode-revision 1)
  (db/run-in-transaction handle (fn [tx dirs] @(op/get tx (p/revision-key dirs))))
  )
