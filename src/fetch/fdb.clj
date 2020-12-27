(ns fetch.fdb
  (:require [fetch.fdb.db    :as db]
            [fetch.fdb.store :as store]))

(def handle
  "A component representing a handle to a FoundationDB database.
   Expects `::cluster-file` in its configuration, and an optional
   `::executor` argument."
  (db/make-database {}))

(def store
  "A component which implements the main storage engine abstraction
   from fetch"
  (store/map->FDBStoreEngine {}))
