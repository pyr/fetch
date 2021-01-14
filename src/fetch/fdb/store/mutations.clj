(ns fetch.fdb.store.mutations
  "Implementations of the `fetch.store/StorageEngine` protocol against
   FoundationDB"
  (:require [fetch.fdb.store.common :as common]
            [fetch.fdb.op           :as op]
            [fetch.fdb.payload      :as p]))

;; All mutations rely on a look-up of a previous key. This lookup
;; happens in the `fetch.fdb.store.interceptors/lookup-previous`
;; interceptor.
;; To simplify read

(defmulti create-if-absent
  "Create a key unless a previous revision exists for it"
  :previous?)

(defmethod create-if-absent true
  [{:keys [tx dirs] :as ctx}]
  (assoc ctx :success? false :revision (common/highest-revision tx dirs)))

(defmethod create-if-absent false
  [{:keys [tx dirs key value lease] :as ctx}]
  (let [lease-id (or lease 0)
        revision (common/increment-revision tx dirs)]
    (op/set tx (p/key dirs key revision) (p/encode-val lease-id 0 value))
    (assoc ctx :success? true :revision revision :lease lease-id)))

(defmulti update-at-revision
  "Essentially a compare and set operation. Dispatches on the state of the
   previous key"
  (fn [{:keys [previous? revision previous]}]
    (let [old-revision (:mod-revision previous)]
      (cond
        (zero? revision)                                  :force-put
        (and (true? previous?) (= revision old-revision)) :force-put
        :else                                             :absent))))

(defmethod update-at-revision :force-put
  [{:keys [tx dirs key value lease] :as ctx}]
  (let [revision (common/increment-revision tx dirs)]
    (op/set tx (p/key dirs key revision) (p/encode-val lease 0 value))
    (assoc ctx :revision revision :success? true)))

(defmethod update-at-revision :bad-revision
  [{:keys [previous] :as ctx}]
  (assoc ctx :revision (:mod-revision previous) :success? false))

(defmulti delete-key
  "A compare and set operation specific to deletes. Dispatches
   on the state of the previous key"
  (fn [{:keys [previous? revision previous]}]
    (let [old-revision (:mod-revision previous)]
      (cond
        (zero? revision)                                  :force-delete
        (and (true? previous?) (= revision old-revision)) :force-delete
        (true? previous?)                                 :bad-revision
        :else                                             :absent))))

(defmethod delete-key :force-delete
  [{:keys [tx dirs key] :as ctx}]
  (op/clear-range tx (p/key-range dirs key))
  (assoc ctx :revision (common/increment-revision tx dirs) :success? true))

(defmethod delete-key :absent
  [{:keys [tx dirs] :as ctx}]
  (assoc ctx :revision (common/highest-revision tx dirs) :success? true))

(defmethod delete-key :bad-revision
  [{:keys [previous] :as ctx}]
  (assoc ctx :revision (:mod-revision previous) :success? false))
