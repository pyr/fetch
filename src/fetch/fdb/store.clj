(ns fetch.fdb.store
  (:require [fetch.store  :as store]
            [fetch.fdb.store.common :as common]
            [fetch.fdb    :as fdb]
            [fetch.fdb.db :as db]
            [exoscale.ex  :as ex]))


(defn create-if-absent
  [tx key value lease]
  (let [rev        (common/increment-revision tx sz)
        previous   (common/previous tx sz key)
        lease-id   (if (some? lease) lease 0)]
    (if (some? previous)
      [rev false]
      @(a/chain (op/set tx (p/key sz key rev)
                        (p/encode-val lease-id 0 value))
                (constantly [rev true])))))

(defn update-at-revision
  [tx key revision value lease]
  (ex/ex-unsupported! "update-at-revision"))

(defn count-keys
  [tx prefix]
  (ex/ex-unsupported! "count-keys"))

(defn range-keys
  [tx revision limit start prefix]
  (ex/ex-unsupported! "range-keys"))

(defn get-at-revision
  [tx key revision]
  (ex/ex-unsupported! "get-at-revision"))

(defn get-latest
  [tx key]
  (ex/ex-unsupported! "get-latest"))

(defn delete-key
  [tx key revision]
  (ex/ex-unsupported! "delete-key"))

(defn create-watch-instance
  [tx instance]
  (ex/ex-unsupported! "create-watch-instance"))

(defn delete-watch-instance
  [tx instance]
  (ex/ex-unsupported! "delete-watch-instance"))

(defn register-key-watch
  [tx instance id prefix revision]
  (ex/ex-unsupported! "register-key-watch"))

(defn cancel-key-watch
  [tx instance id prefix]
  (ex/ex-unsupported! "register-key-watch"))

(defn register-watch-listener
  [tx instance]
  (ex/ex-unsupported! "register-watch-listener"))

(defrecord FDBStoreEngine [sz]
  component/Lifecycle
  (start [this]
    (assoc this :sz (payload/space-serializer (space/create-or-open)))
    )
  store/StorageEngine
  (create-if-absent [this key value lease]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx]
                             (create-if-absent tx key value lease))))
  (update-at-revision [this key revision value lease]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx]
                             (update-at-revision tx key revision value lease))))
  (count-keys [this prefix]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx]
                             (count-keys tx prefix))))
  (range-keys [this revision limit start prefix]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx]
                             (range-keys tx revision limit start prefix))))
  (get-at-revision [this key revision]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx]
                             (get-at-revision tx key revision))))
  (get-latest [this key]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx]
                             (get-latest tx key))))
  (delete-key [this key revision]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx]
                             (delete-key tx key revision))))
  (create-watch-instance [this instance]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx]
                             (create-watch-instance tx instance))))
  (delete-watch-instance [this instance]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx]
                             (delete-watch-instance tx instance))))
  (register-key-watch [this instance id prefix revision]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx]
                             (register-key-watch tx instance id
                                                 prefix revision))))
  (cancel-key-watch [this instance id prefix]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx]
                             (cancel-key-watch tx instance id prefix))))
  (register-watch-listener [this instance]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx]
                             (register-watch-listener tx instance)))))
