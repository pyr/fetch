(ns fetch.grpc.watch
  (:require [fetch.grpc.types  :as types]
            [fetch.grpc.stream :as stream]
            [fetch.store       :as store]
            [exoscale.ex       :as ex])
  (:import exoscale.etcd.api.WatchGrpc$WatchImplBase))

(defmulti handle-request (fn [_ {:keys [type]}] type))

(defmethod handle-request :create
  [watcher req])

(defmethod handle-request :cancel
  [watcher req])

(defn- make-service
  [{::store/keys [engine]}]
  (proxy [WatchGrpc$WatchImplBase] []
    (watch [resp]
      (try
        (let [watcher (store/make-watcher engine (make-notifier resp))]
          (stream/make-observer
           (partial handle-request watcher)
           (partial handler-error watcher)))
        (catch Exception e
          (stream/error! e))))))

(def watch
  (with-meta {}
    {'fetch.grpc.server/get-service make-service}))
