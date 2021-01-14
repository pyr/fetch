(ns fetch.fdb.store
  "Implementations of the `fetch.store/StorageEngine` protocol against
   FoundationDB"
  (:require [fetch.store                  :as store]
            [fetch.fdb.store.mutations    :as mutations]
            [fetch.fdb.store.lookups      :as lookups]
            [fetch.fdb.store.interceptors :as ix]
            [fetch.fdb.store.watches      :as watches]))

(defrecord FDBStoreEngine [db]
  store/StorageEngine
  ;; Mutations
  ;; =========
  (create-if-absent [_ key value lease]
    (ix/write! db :create mutations/create-if-absent {:key       key
                                                      :mutation? true
                                                      :value     value
                                                      :lease     lease}))
  (update-at-revision [_ key revision value lease]
    (ix/write! db :update mutations/update-at-revision {:key       key
                                                        :mutation? true
                                                        :value     value
                                                        :revision  revision
                                                        :lease     lease}))
  (delete-key [_ key revision]
    (ix/write! db :delete mutations/delete-key {:key       key
                                                :revision  revision
                                                :mutation? true}))

  ;; Lookups
  ;; =======
  (count-keys [_ prefix]
    (ix/read db :count lookups/count-keys {:prefix prefix}))
  (range-keys [_ revision limit prefix]
    (ix/read db :range (ix/out lookups/range-keys [:result]) {:revision revision
                                                              :limit    limit
                                                              :prefix   prefix}))
  (get-at-revision [_ key revision]
    (ix/read db :get (ix/out lookups/get-at-revision [:result])
             {:key      key
              :revision revision}))
  (get-latest [_ key]
    (ix/read db :get-latest lookups/get-latest {:key key}))

  ;; Watches
  ;; =======
  (create-watch-instance [_ instance]
    (ix/write! db :create-instance watches/create-watch-instance
               {:instance instance}))
  (delete-watch-instance [_ instance]
    (ix/write! db :delete-instance watches/delete-watch-instance
               {:instance instance}))
  (register-key-watch [_ instance id prefix revision]
    (ix/write! db :register-watch watches/register-key-watch
               {:instance instance
                :prefix   prefix
                :revision revision
                :id       id}))
  (cancel-key-watch [_ instance id]
    (ix/write! db :cancel-watch watches/register-key-watch
               {:instance instance
                :id       id}))
  (register-watch-listener [_ instance]
    (ix/write! db :register-listener watches/register-listener
               {:instance instance})))
