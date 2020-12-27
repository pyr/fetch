(ns fetch.grpc.status
  "Protocols and facilities to transform a protocol implementation into
   a grpc service instance"
  (:require [exoscale.ex           :as ex]
            [clojure.tools.logging :as log])
  (:import java.util.concurrent.CompletionException
           io.grpc.Status))

(defmulti type->status identity :hierarchy ex/hierarchy)
(defmethod type->status ::ex/incorrect [_] Status/INVALID_ARGUMENT)
(defmethod type->status ::ex/not-found [_] Status/NOT_FOUND)
(defmethod type->status ::ex/forbidden [_] Status/PERMISSION_DENIED)
(defmethod type->status ::ex/unsupported [_] Status/UNIMPLEMENTED)
(defmethod type->status ::ex/conflict [_] Status/FAILED_PRECONDITION)
(defmethod type->status ::ex/unavailable [_] Status/UNAVAILABLE)
(defmethod type->status ::ex/busy [_] Status/RESOURCE_EXHAUSTED)
(defmethod type->status :default [_] Status/UNKNOWN)

(defmulti type->log identity :hierarchy ex/hierarchy)
(defmethod type->log ::ex/incorrect [_] false)
(defmethod type->log ::ex/not-found [_] false)
(defmethod type->log ::ex/forbidden [_] false)
(defmethod type->log ::ex/unsupported [_] false)
(defmethod type->log ::ex/conflict [_] false)
(defmethod type->log ::ex/unavailable [_] false)
(defmethod type->log ::ex/busy [_] false)
(defmethod type->log ::ex/invalid-spec [_] true)
(defmethod type->log :default [_] true)

(def status->status
  {:aborted             Status/ABORTED
   :already-exists      Status/ALREADY_EXISTS
   :cancelled           Status/CANCELLED
   :data-loss           Status/DATA_LOSS
   :deadline-exceeded   Status/DEADLINE_EXCEEDED
   :failed-precondition Status/FAILED_PRECONDITION
   :internal            Status/INTERNAL
   :invalid-argument    Status/INVALID_ARGUMENT
   :not-found           Status/NOT_FOUND
   :ok                  Status/OK
   :out-of-range        Status/OUT_OF_RANGE
   :permission-denied   Status/PERMISSION_DENIED
   :resource-exhausted  Status/RESOURCE_EXHAUSTED
   :unauthenticated     Status/UNAUTHENTICATED
   :unavailable         Status/UNAVAILABLE
   :unimplemented       Status/UNIMPLEMENTED
   :unknown             Status/UNKNOWN})

(defn ex->status
  "Coerce GRPC statuses into exoscale/ex errors"
  [e]
  (let [e                                 (cond-> e
                                            (instance? CompletionException e)
                                            .getCause)
        {:keys      [type]
         :grpc/keys [status description]} (ex-data e)
        description                       (or description (ex-message e))
        log?                              (or (:grpc/log? (ex-data e)
                                                          (type->log type)))
        gstatus                           (or (status->status status)
                                              (type->status type)
                                              Status/UNKNOWN)]
    (when log?
      (log/error e (ex-message e)))
    (.withDescription gstatus description)))

(defn as-exception
  [^Status s]
  (.asException s))
