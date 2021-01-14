(ns fetch.grpc.auth
  (:require [fetch.grpc.status          :as status]
            [fetch.grpc.context         :as ctx]
            [fetch.grpc.server          :as server]
            [fetch.store                :as store]
            [exoscale.ex                :as ex]
            [com.stuartsierra.component :as component])
  (:import io.grpc.BindableService
           io.grpc.Grpc
           io.grpc.ServerInterceptors
           io.grpc.ServerInterceptor
           io.grpc.ServerCall$Listener
           java.util.List))

(defn make-auth-interceptor
  "Build the auth interception function."
  [factory]
  (reify ServerInterceptor
    (interceptCall [this call headers next]
      (try
        (let [principal (some-> call
                                .getAttributes
                                (.get Grpc/TRANSPORT_ATTR_SSL_SESSION)
                                (.getPeerPrincipal)
                                (.getName)
                                (str))]
          (when (nil? principal)
            (ex/ex-forbidden! "invalid TLS session, cannot retrieve principal"))
          (ctx/intercept (ctx/with-engine (store/prefixed factory principal))
                         call
                         headers
                         next))
        (catch Exception e
          (.close call (.withCause (status/ex->status e) e))
          (proxy [ServerCall$Listener] []))))))

(defn make-service
  "Wrap a service in an interceptor"
  [{::store/keys [factory] ::server/keys [service]}]
  (ServerInterceptors/intercept ^BindableService service
                                ^List [(make-auth-interceptor factory)]))


;; The reason behind this one is that we want an interceptor to look up our
;; credentials in the TLS session to determine where in the cluster to store
;; things.
;;
;; Every call to services using this interceptor will have a storage engine
;; available at the `storage-engine` context key. A handy function `engine`
;; is available here to retrieve it.

(def service
  (with-meta {} {`component/start    make-service
                 `component/stop     #(dissoc % ::service)
                 `server/get-service ::service}))
