(ns fetch.fdb.store.watches
  (:require [fetch.fdb.store.common :as common]
            [fetch.fdb.op           :as op]
            [fetch.fdb.kv           :as kv]
            [fetch.fdb.transaction  :as tx]
            [fetch.fdb.payload      :as p]
            [qbits.auspex           :as a]))

(defn create-watch-instance
  [tx dirs instance]
  (let [rev (common/increment-revision tx dirs)]
    (op/set tx (p/watch-instance-key dirs instance) (p/encode-long rev))))

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
             (tx/write-transaction
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
