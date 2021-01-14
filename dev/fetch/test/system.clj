(ns fetch.test.system
  (:require [com.stuartsierra.component :as component :refer [using]]
            [fetch.store                :as store]
            [fetch.fdb                  :as fdb]
            [fetch.fdb.transaction      :as tx]
            [fetch.fdb.db               :as db]
            [fetch.fdb.op               :as op]
            [fetch.fdb.space            :as space]))

(def ^:dynamic *store* nil)
(def ^:dynamic *fdb* nil)
(def ^:dynamic *system* nil)

(defn cluster-name
  []
  (or (System/getenv "FETCH_TEST_CLUSTER_NAME")
      (System/getenv "fetch.test.cluster-name")
      (str (java.util.UUID/randomUUID))))

(defn purge-directories
  [{::keys [fdb cluster-name]}]
  ;; When things go wrong during local tests, it can be useful to have a way
  ;; to inspect the values generated in the database.
  (let [prevent-purge? (some?
                        (or (System/getenv "FETCH_TEST_PREVENT_PURGE")
                            (System/getProperty "fetch.test.prevent-purge")))
        store          (store/namespaced fdb cluster-name)]
    (when-not prevent-purge?
      ;; XXX: purge created directories
      :nothing
      )))

(def cleaner
  (with-meta {} {`component/stop purge-directories}))

(defn fdb-system
  []
  {::fdb/cluster-file "/etc/foundationdb/fdb.cluster"
   ::fdb/prefix       (name (gensym "test-etcd"))
   ::cluster-name     (cluster-name)
   ::fdb              (using fdb/handle [::fdb/cluster-file ::fdb/prefix])
   ::cleaner          (using cleaner [::fdb])})

(defn wrap-system-fn
  [sys]
  (fn [f]
    (let [started (component/start-system (into (component/system-map) (sys)))]
      (binding [*system* started
                *store*  (store/namespaced (::fdb started)
                                           (::cluster-name started))
                *fdb*    (::fdb started)]
        (try (f) (finally (component/stop-system started)))))))


(def test-systems
  {:fdb (wrap-system-fn fdb-system)})

(defn test-system
  [k]
  (or (get test-systems k)
      (throw (IllegalArgumentException. (str "unknown system: " k)))))
