(ns fetch.fdb
  (:require [fetch.fdb.db :as db]))

(def handle
  "A component representing a handle to a FoundationDB database.
   Expects `::cluster-file` in its configuration, and an optional
   `::executor` argument."
  (db/make-database {}))
