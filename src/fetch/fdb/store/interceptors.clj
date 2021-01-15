(ns fetch.fdb.store.interceptors
  "Common interceptors to help serve requests"
  (:refer-clojure :exclude [read])
  (:require [fetch.fdb.store.common :as common]
            [fetch.fdb.transaction  :as tx]
            [fetch.fdb.op           :as op]
            [exoscale.interceptor   :as ix]
            [clojure.tools.logging  :as log]))

(def out
  "Forward exocale.interceptor/out"
  ix/out)

(def successful-mutation?
  "Check for successful mutation in a payload"
  (every-pred :mutation? :success?))

(def final
  {:name  :final
   :leave #(select-keys % [:result :success? :revision])})

(def lookup-previous
  "When `:lookup?` is set in the payload, fetch the last known version
   of `key`."
  {:name :lookup-previous
   :enter (-> (fn [{:keys [tx dirs key] :as ctx}]
                (assoc ctx :previous (common/previous tx dirs key)))
              (ix/when (comp some? :key)))})

(def byte-counter
  "When a mutation has successfully ran, adapt the current cluster's byte count"
  {:name :byte-counter
   :leave (-> (fn [{:keys [tx dirs op value]}]
                (let [bc (cond-> (count (seq value)) (= op :delete) (* -1))]
                  (common/update-usage tx dirs bc)))
              (ix/when :success?)
              (ix/discard))})

(def event-publisher
  "When a mutation has successfully ran, create an event"
  {:name :event-publisher
   :leave (-> (fn [{:keys [tx dirs key value op]}]
                (common/add-event tx dirs {:op op :key key :value value}))
              (ix/when :success?)
              (ix/discard))})

(def watch-update
  "When a mutation has successfully ran, check if the corresponding key
   is being watched and if so, signal the corresponding watch"
  {:name :watch-update
   :leave (-> (fn [{:keys [tx dirs key]}]
                (run! (partial common/signal-watch tx dirs)
                      (common/find-watches tx dirs key)))
              (ix/when :success?)
              (ix/discard))})

(def record-timing
  {:name :record-timing
   :leave (-> #(log/info "transaction size:" (op/approximate-size (:tx %)))
              (ix/discard)) })

(def error-report
  {:name :error-report})

(defn mutate!
  [db name handler params]
  (tx/write-transaction
   db
   (fn [tx dirs]
     (ix/execute (merge params {:op name :tx tx :dirs dirs})
                 [final record-timing error-report lookup-previous
                  byte-counter watch-update event-publisher
                  {:name name :enter handler}]))))

(defn write!
  [db name handler params]
  (tx/write-transaction
   db
   (fn [tx dirs]
     (ix/execute (merge params {:op name :tx tx :dirs dirs})
                 [final record-timing error-report
                  {:name name :enter handler}]))))

(defn read
  [db name handler params]
  (tx/read-transaction
   db
   (fn [tx dirs]
     (ix/execute (merge params {:op name :tx tx :dirs dirs})
                 [final error-report lookup-previous
                  {:name name :enter handler}]))))
