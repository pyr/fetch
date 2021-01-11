(ns fetch.pool
  "Thread pool management on standard JDK thread pools."
  (:require [clojure.tools.logging      :as log]
            [clojure.spec.alpha         :as s]
            [com.stuartsierra.component :as component])
  (:import java.util.concurrent.ArrayBlockingQueue
           java.util.concurrent.ExecutorService
           java.util.concurrent.Callable
           java.util.concurrent.TimeUnit
           java.util.concurrent.ThreadFactory
           java.util.concurrent.ThreadPoolExecutor))

(defprotocol Pool
  (submit [this task] "Submit a new task for execution on a pool"))

(defn factory
  "Yield a thread factory which sets all threads as daemon and uses
   the pool name to name threads.

   Ensuring the given threads are non daemons allows shutdown to wait on
   threads present in the related executor to finish before stopping the
   service."
  [pool-name]
  (reify ThreadFactory
    (newThread [this r]
      (let [thread (Thread. r)]
        (.setName thread (format "%s-%s-%s" pool-name
                                 (.getPriority thread) (.getId thread)))
        (.setDaemon thread false)
        thread))))

;; Some reading notes, since the following is a bit convoluted
;;
;; To support a full lifecycle for pools while still exposing
;; the actual `ThreadPoolExecutor` instance we start by
;; exposing a configuration record.
;;
;; `component/stop` is a noop on the record, while `component/start`
;; will create and yield a new `ThreadPoolExecutor` instance.
;;
;; `component/start` on the `ThreadPoolExecutor` instance is a noop,
;; while `component/stop` will yield back a `PoolConfig` record,
;; allowing for later restarts.
;;
;; `component/Lifecycle` is not implemented immediately on `PoolRecord`
;; to allow `instrumented-impl` to be defined.
;;
;;
(defrecord PoolConfig [config])

(defn- instrumented-impl
  "An instrumented thread pool.

   ILookup is implemented on the pool, allowing it to return
   its configuration for `(get pool ::config)`."
  [{:keys [pool-name capacity keepalive queue-size] :as config}]
  (proxy
   [ThreadPoolExecutor clojure.lang.ILookup]
   [(if (some? capacity) (int capacity) 10)
    (if (some? capacity) (int capacity) 10)
    (or keepalive 60)
    TimeUnit/SECONDS
    (ArrayBlockingQueue. (if (some? queue-size) (int queue-size) 10))
    (factory pool-name)]
    (valAt [x]
      (cond (= x ::config) (PoolConfig. config)))))

(defn forcibly-shutdown-pool
  [pool config]
  (let [grace-period (long (get-in config [:config :shutdown-grace-period] 2))
        pool         ^ExecutorService pool]
    (log/info "shutting pool down with grace period:" grace-period)
    (try
      (.shutdown pool)
      (when-not (.awaitTermination pool grace-period TimeUnit/SECONDS)
          ;; Failed to shut down gracefully, forcefully shut down now
        (.shutdownNow pool)
        (when-not (.awaitTermination pool grace-period TimeUnit/SECONDS)
          (log/error "could not terminate all threads")))
        ;; By yielding the pool config regardless of success or
        ;; failure, the result is guaranteed to honor `component/start`.
      (catch InterruptedException e
        (log/error e "interrupted while shutting down pool")))))

(extend-protocol component/Lifecycle
  PoolConfig
  (start [this]
    (instrumented-impl (:config this)))
  (stop [this]
    this)
  ThreadPoolExecutor
  (start [this]
    ;; Start on an existing pool is a noop
    this)
  (stop [this]
    (let [pool-config (::config this)]
      (forcibly-shutdown-pool this (::config this))
      pool-config)))

(extend-protocol Pool
  ThreadPoolExecutor
  (submit [pool task]
    (.submit ^ThreadPoolExecutor pool ^Callable task)))

(def operator
  "
An operator for a fixed thread pool.

Accepts configuration at its :config key, expecting the following keys:

|                option |                                description |
|-----------------------+--------------------------------------------|
|              capacity |                      pool maximum capacity |
|             pool-name |         string to prefix thread names with |
|            queue-size |             size of the waiting-list queue |
|             keepalive |               excess thread keepalive time |
| shutdown-grace-period | thread termination grace period in seconds |

An example wiring in a system component, assuming you have an EDN
configuration which looks like this and is present in your system map:

    {:system.pool1/config {:pool-name \"pool1\" :capacity 10}}

The operator can be added to the system map like this:

    (assoc system-map ::pool1 (using exoscale.operator.pool/operator
                               {:config :system.pool1/config}))

This operator honors start and stop behavior.
"
  (map->PoolConfig {}))

(s/def ::pool-name string?)
(s/def ::keepalive pos-int?)
(s/def ::capacity pos-int?)
(s/def ::queue-size pos-int?)
(s/def ::shutdown-grace-period pos-int?)
(s/def ::config (s/keys :req-un [::pool-name]
                        :opt-un [::keepalive
                                 ::capacity
                                 ::shutdown-grace-period
                                 ::queue-size]))
