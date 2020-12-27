(ns fetch.grpc.stream
  (:require [fetch.grpc.status :as status])
  (:import io.grpc.stub.StreamObserver))

(defn on-next
  [^StreamObserver resp val]
  (.onNext resp val))

(defn on-completed
  [^StreamObserver resp]
  (.onCompleted resp))

(defn on-error
  [^StreamObserver resp ^Throwable e]
  (.onError resp e))

(defn error!
  [resp e]
  (on-error resp
            (-> e
                (status/ex->status)
                (status/as-exception))))

(defn complete!
  [resp v]
  (on-next resp v)
  (on-completed resp))

(defn make-observer
  [new-message-fn error-fn completed-fn]
  (reify StreamObserver
    (onNext [_ msg]
      (new-message-fn msg))
    (onCompleted [_]
      (completed-fn))
    (onError [_ e]
      (error-fn msg))))
