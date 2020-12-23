(ns fetch.grpc.common
  "Protocols and facilities to transform a protocol implementation into
   a grpc service instance"
  (:require [qbits.auspex :as a]
            [exoscale.ex :as ex]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log])
  (:import java.util.concurrent.CompletionException
           io.grpc.BindableService
           io.grpc.Context
           io.grpc.Context$Key
           io.grpc.Contexts
           io.grpc.Metadata
           io.grpc.Metadata$Key
           io.grpc.Status
           io.grpc.ServerCall
           io.grpc.ServerCall$Listener
           io.grpc.ServerCallHandler
           io.grpc.ServerInterceptor
           io.grpc.ServerServiceDefinition))

(defn context-key
  [k]
  (Context/key (str k)))

(defn set-in-context!
  [k v]
  (->  (Context/current)
       (.withValue k v)
       (.attach)))

(defn get-from-context
  [^Context$Key k]
  (.get k))

(defn metadata-ascii-key
  [x]
  (Metadata$Key/of (str x) Metadata/ASCII_STRING_MARSHALLER))

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


(defn start-call
  [^ServerCallHandler next ^ServerCall call ^Metadata headers]
  (.startCall next call headers))

(defn start-with-context
  [values ^ServerCall call ^Metadata headers ^ServerCallHandler next]
  (let [ctx (reduce-kv #(.withValue %1 %2 %3) (Context/current) values)]
    (Contexts/interceptCall ctx call headers next)))

(defn wrap-interceptor
  [f]
  (reify ServerInterceptor
    (interceptCall [this call headers next]
      (try
        (f call headers next)
        (catch Exception e
          (.close call (.withCause (ex->status e) e))
          (proxy [ServerCall$Listener] []))))))

(defn wrap-unary-fn
  [f coax-in coax-out]
  (fn [req resp]
    (try
      (a/catch
          (a/chain req
                   (comp f coax-in)
                   coax-out
                   #(doto resp (.onNext %) (.onCompleted)))
          (fn [e]
            (.onError resp (.asException (ex->status (.getCause e))))))
      (catch Exception e
        (.onError resp (.asException (ex->status e)))))))

(defn header
  [^Metadata headers ^Metadata$Key k]
  (when (.containsKey headers k)
    (.get headers k)))

(defn clean-service
  [config]
  (dissoc config ::service-inst))

(defn clean-interceptor
  [config]
  (dissoc config ::intercetpor-inst))

(defn build-interceptor
  [{:exoscale.grpc.interceptor/keys [builder] :as config}]
  (assoc config :exoscale.grpc.interceptor/instance (builder config)))

(defn build-service
  [{:exoscale.grpc.service/keys [builder] :as config}]
  (assoc config :exoscale.grpc.service/instance (builder config)))

(defn make-service
  [service]
  (with-meta
    (ex/assert-spec-valid
     ::service
     {:exoscale.grpc.service/builder service})
    {'com.stuartsierra.component/start build-service
     'com.stuartsierra.component/stop  clean-service}))

(defn make-interceptor
  [interceptor]
  (with-meta
    (ex/assert-spec-valid
     ::interceptor
     {:exoscale.grpc.interceptor/builder interceptor})
    {'com.stuartsierra.component/start build-interceptor
     'com.stuartsierra.component/stop  clean-interceptor}))


(s/def :exoscale.grpc.service/builder ifn?)
(s/def :exoscale.grpc.interceptor/builder ifn?)
(s/def :exoscale.grpc.service/instance (some-fn
                                        (partial instance?
                                                 BindableService)
                                        (partial instance?
                                                 ServerServiceDefinition)))
(s/def :exoscale.grpc.interceptor/instance (partial instance?
                                                    ServerInterceptor))
(s/def ::service (s/keys :req [:exoscale.grpc.service/builder]
                         :opt [:exoscale.grpc.service/instance]))
(s/def ::interceptor (s/keys :req [:exoscale.grpc.interceptor/builder]
                             :opt [:exoscale.grpc.interceptor/instance
                                   ::interceptor]))
