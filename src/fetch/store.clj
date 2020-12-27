(ns fetch.store)

(defprotocol StorageEngine
  (create-if-absent [this key value lease])
  (update-at-revision [this key revision value lease])
  (count-keys [this prefix])
  (range-keys [this revivision limit start prefix])
  (get-at-revision [this key revision])
  (get-latest [this key])
  (delete-key [this key revision])
  (register-watch [this id key observer])
  (cancel-watch [this id]))
