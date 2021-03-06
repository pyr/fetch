(ns fetch.store)

(defprotocol StorageEngine
  ;; KV specific
  (create-if-absent [this key value lease])
  (update-at-revision [this key revision value lease])
  (count-keys [this prefix])
  (range-keys [this revision limit prefix])
  (get-at-revision [this key revision])
  (get-latest [this key])
  (delete-key [this key revision])
  ;; Watch specific
  (create-watch-instance [this instance])
  (delete-watch-instance [this instance])
  (register-key-watch [this instance id prefix revision])
  (cancel-key-watch [this instance id])
  (register-watch-listener [this instance]))

(defprotocol StorageEngineFactory ;; I could not resist :-)
  (namespaced [this prefix]))
