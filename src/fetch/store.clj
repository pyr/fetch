(ns fetch.store
  (:require [qbits.auspex :as a])
  (:import java.util.UUID))

(defprotocol StorageEngine
  (create-if-absent [this key value lease])
  (update-at-revision [this key revision value lease])
  (count-keys [this prefix])
  (range-keys [this revivision limit start prefix])
  (get-at-revision [this key revision])
  (get-latest [this key])
  (delete-key [this key revision])
  (create-watch-key [this id key value])
  (cancel-watch [this id])
  (get-revision [this]))

(defprotocol WatchNotifier
  ())

(defrecord Watcher [engine notifier])

(defn make-watcher
  [engine notifier pool]
  (let [watcher {::engine   engine
                 ::notifier notifier
                 ::id       (UUID/randomUUID)}
        rev     (get-revision engine)

        ]
    (create-watch-key engine id key value)


    ()

    ))
