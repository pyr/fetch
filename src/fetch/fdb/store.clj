(ns fetch.fdb.store
  (:require [fetch.store  :as store]
            [fetch.fdb    :as fdb]
            [fetch.fdb.db :as db]
            [exoscale.ex  :as ex]))

(defn create-if-absent
  [tx key value lease]
  (ex/ex-unsupported! "create-if-absent"))

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

(defn register-watch
  [tx id key observer]
  (ex/ex-unsupported! "register-watch"))

(defn cancel-watch
  [tx id]
  (ex/ex-unsupported! "cancel-watch"))

(defrecord FDBStoreEngine []
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
  (register-watch [this id key observer]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx]
                             (register-watch tx id key observer))))
  (cancel-watch [this id]
    (db/run-in-transaction (::fdb/fdb this)
                           (fn [tx]
                             (cancel-watch tx id)))))
