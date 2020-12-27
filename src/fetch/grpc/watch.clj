(ns fetch.grpc.watch
  (:require [fetch.api.proto   :as proto]
            [fetch.grpc.types  :as types]
            [fetch.grpc.stream :as stream]
            [exoscale.ex       :as ex])
  (:import exoscale.etcd.api.WatchGrpc$WatchImplBase))

(defn- make-watch-service
  [{::proto/keys [watchbackend]}]
  (proxy [WatchGrpc$WatchImplBase] []
    (watch [resp]
      (reify io.grpc.stub.StreamObserver
        (onNext [_ raw-req]
          (let [{:keys [type] :as req} (types/watch-request->map raw-req)]
            (if (= type :create)
              (wrap-stream-call (partial proto/create-watch watchbackend req)
                                resp)
              (wrap-stream call (partial proto/cancel-watch watchbackend req)
                           resp))))))))

(def watch
  (with-meta {}
    {'fetch.grpc.server/get-service make-watch-service}))
