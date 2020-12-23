(ns fetch.grpc.server
  (:require [exoscale.ex :as ex])
  (:import io.grpc.ServerBuilder
           java.util.concurrent.TimeUnit))

(defn service-not-found!
  [service-key]
  (throw (ex/ex-not-found! (str "service not found: " service-key))))

(defprotocol ServiceHolder
  :extend-via-metadata true
  (get-service [this]))

(defn fetch-service
  [this k]
  (if-let [location (get this k)]
    (cond
      (contains? (meta location) `get-service) (get-service location)
      (instance? ServiceHolder location)       (get-service location)
      :else                                    location)
    (throw (ex/ex-not-found! (str "service not found: " k)))))

(defn start-server
  [{::keys [port services] :as this}]
  (let [sb (ServerBuilder/forPort (int port))]
    (doseq [service (map (partial fetch-service this) services)]
      (.addService sb service))
    (let [server (.build sb)]
      (.start server)
      (assoc this ::server server))))

(defn stop-server
  [{::keys [server grace-period] :as this}]
  (when (some? server)
    (.shutdown server)
    (.awaitTermination server (or grace-period 2000) TimeUnit/MILLISECONDS))
  (dissoc this ::server))

(defn make-server
  [opts]
  (with-meta opts
    {'com.stuartsierra.component/start start-server
     'com.stuartsierra.component/stopt stop-server}))

(def server
  (make-server {}))
