(ns fetch.fdb.store
  (:require [fetch.store            :as store]
            [fetch.fdb.store.common :as common]
            [fetch.fdb              :as fdb]
            [fetch.fdb.db           :as db]
            [fetch.fdb.op           :as op]
            [fetch.fdb.kv           :as kv]
            [fetch.fdb.payloads     :as p]
            [qbits.auspex           :as a]
            [exoscale.ex            :as ex]))

(defn create-if-absent
  [tx sz key value lease]
  (let [previous (common/previous tx sz key)
        lease-id (if (some? lease) lease 0)]
    (if (some? previous)
      [(common/highest-revision tx sz) false]
      (let [new-rev (common/increment-revision tx sz)]
        @(a/chain (op/set tx (p/key sz key new-rev)
                          (p/encode-val lease-id 0 value))
                  (constantly [new-rev true]))))))

(defn update-at-revision
  [tx sz key revision value lease]
  (let [previous (common/previous tx sz key)]
    (if (= revision (:mod-revision previous))
      (let [new-rev (common/increment-revision tx sz)]
        @(a/chain (op/set tx (p/key sz key new-rev)
                          (p/encode-val lease 0 value))
                  (constantly [new-rev true])))
      [(common/highest-revision tx sz) false])))

(defn count-keys
  [tx sz prefix]
  @(a/chain (op/reverse-range tx (p/key-prefix sz prefix))
            (fn [kvs]
              [(common/highest-revision tx sz) (count kvs)])))

(defn range-keys
  [tx sz revision limit prefix]
  (filter #(>= (:mod-revision %) revision)
          @(op/reverse-range tx (p/key-prefix sz prefix) limit)))

(defn get-at-revision
  [tx sz key revision]
  @(op/get tx (p/key sz key revision)))

(defn get-latest
  [tx sz key]
  (common/previous tx sz key))

(defn delete-key
  [tx sz key revision]
  (let [previous (common/previous tx sz key)]
    (if (= revision (:mod-revision previous))
      (let [new-rev (common/increment-revision tx sz)]
        @(a/chain (op/clear tx (p/key-range sz key))
                  (constantly [new-rev true])))
      [revision false])))

(defn create-watch-instance
  [tx sz instance]
  (let [rev (common/increment-revision tx sz)]
    @(op/set tx (p/watch-instance-key sz instance) (p/encode-revision rev))))

(defn delete-watch-instance
  [tx sz instance]
  @(op/clear tx (p/watch-instance-key sz instance)))

(defn register-key-watch
  [tx sz instance id prefix revision]
  @(op/set tx (p/watch-key sz prefix)
           (p/encode-watch instance id revision)))

(defn cancel-key-watch
  [tx sz instance id]
  (doseq [kv (op/range-with-range (p/watch-range sz) tx op/range-no-limit false)
          :let [watch (p/decode-watch (kv/v kv))]
          :when (= [instance id] (take 2 watch))]
    @(op/clear tx (kv/k kv))))

(defn register-watch-listener
  "Watch the notification key for this watcher's instance, only one key watched
   per connection to etcd.

   When the key is notified, a new transaction is generated which fetches all
   events then compacts them."
  [db tx sz instance]
  (let [instance-key (p/watch-instance-key sz instance)
        events-key   (p/events-key sz instance)]
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

(defrecord FDBStoreEngine [sz]
  store/StorageEngine
  (create-if-absent [this key value lease]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx sz]
                             (create-if-absent tx sz key value lease))))
  (update-at-revision [this key revision value lease]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx sz]
                             (update-at-revision tx sz key revision
                                                 value lease))))
  (count-keys [this prefix]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx sz]
                             (count-keys tx sz prefix))))
  (range-keys [this revision limit prefix]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx sz]
                             (range-keys tx sz revision limit prefix))))
  (get-at-revision [this key revision]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx sz]
                             (get-at-revision tx sz key revision))))
  (get-latest [this key]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx sz]
                             (get-latest tx sz key))))
  (delete-key [this key revision]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx sz]
                             (delete-key tx sz key revision))))
  (create-watch-instance [this instance]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx sz]
                             (create-watch-instance tx sz instance))))
  (delete-watch-instance [this instance]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx sz]
                             (delete-watch-instance tx sz instance))))
  (register-key-watch [this instance id prefix revision]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx sz]
                             (register-key-watch tx sz instance id
                                                 prefix revision))))
  (cancel-key-watch [this instance id]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx sz]
                             (cancel-key-watch tx sz instance id))))
  (register-watch-listener [this instance]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx sz]
                             (register-watch-listener db tx sz instance)))))
