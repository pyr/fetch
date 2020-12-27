(ns fetch.grpc.watch
  (:require [fetch.grpc.stream :as stream]
            [fetch.grpc.types  :as types]
            [fetch.store       :as store]
            [fetch.watcher     :as watcher])
  (:import exoscale.etcd.api.WatchGrpc$WatchImplBase))

(defmulti handle-request (fn [_ {:keys [type]}] type))

(defmethod handle-request :create
  [watcher notifier req]
  (let [id (watcher/create-watch watcher
                                 (:key req)
                                 (:start-revision req))]
    (notifier {:type     :created
               :created  true
               :watch-id id})))

(defmethod handle-request :cancel
  [watcher notifier req]
  (watcher/cancel-watch watcher (:watch-id req))
  (notifier {:type          :cancelled
             :cancelled     true
             :cancel-reason "watch closed"
             :watch-id      (:watch-id req)}))

(defn handle-error
  [watcher resp e]
  (watcher/stop-watcher watcher (ex-message e))
  (stream/error! resp e))

(defn- make-publisher
  [watcher notifier resp]
  (reify watcher/WatchPublisher
    (publish-events [_ watch-id revision events]
      (notifier {:type     :events
                 :revision revision
                 :watch-id watch-id
                 :events   events}))
    (on-error [_ e]
      (handle-error watcher resp e))))

(defn- make-observer
  [watcher notifier resp]
  (stream/make-observer
   (fn [req]
     (try
       (handle-request watcher notifier (types/watch-request->map req))
       (catch Exception e
         (handle-error watcher resp e))))
   (fn [e]
     (handle-error watcher resp e))
   (fn []
     (watcher/stop-watcher watcher "stream completed"))))

(defn- make-service
  [{::store/keys [engine]}]
  (proxy [WatchGrpc$WatchImplBase] []
    (watch [resp]
      (try
        (let [notifier  #(stream/on-next resp (types/map->watch-response %))
              publisher (make-publisher watcher notifier resp)
              watcher   (watcher/make-watcher engine publisher)]
          (make-observer watcher notifier resp))
        (catch Exception e
          (stream/error! resp e))))))

(def watch
  (with-meta {}
    {'fetch.grpc.server/get-service make-service}))
