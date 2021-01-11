(ns fetch.grpc.server
  (:require [exoscale.ex           :as ex]
            [clojure.spec.alpha    :as s]
            [clojure.tools.logging :as log])
  (:import io.grpc.BindableService
           io.grpc.Server
           io.grpc.netty.NettyServerBuilder
           java.net.InetSocketAddress
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

(defn- ^InetSocketAddress build-address
  [^String host port]
  (if (some? host)
    (InetSocketAddress. host (int port))
    (InetSocketAddress. (int port))))

(defn start-server
  [{::keys [host port services] :as this}]
  (ex/assert-spec-valid ::config this)
  (let [addr (build-address host port)
        sb   (NettyServerBuilder/forAddress addr)]
    (doseq [^BindableService service (map (partial fetch-service this) services)]
      (log/info "adding service:" service)
      (.addService ^NettyServerBuilder sb service))
    (let [server (-> sb .build .start)]
      (log/info "built and started server:" server)
      (assoc this ::server server))))

(defn stop-server
  [{::keys [server grace-period] :as this}]
  (log/info "shutting down server")
  (when (some? server)
    (.shutdown ^Server server)
    (.awaitTermination ^Server server (or grace-period 2000) TimeUnit/MILLISECONDS))
  (log/info "server shutdown completed")
  (dissoc this ::server))

(defn make-server
  [opts]
  (with-meta opts
    {'com.stuartsierra.component/start start-server
     'com.stuartsierra.component/stop  stop-server}))

(def server
  (make-server {}))

(s/def ::port pos-int?)
(s/def ::service qualified-keyword?)
(s/def ::services (s/coll-of ::service))
(s/def ::host string?)
(s/def ::config (s/keys :req [::port ::services] :opt [::host]))
