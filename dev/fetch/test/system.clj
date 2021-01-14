(ns fetch.test.system
  (:require [com.stuartsierra.component :as component :refer [using]]
            [fetch.store                :as store]
            [fetch.fdb                  :as fdb]
            [fetch.fdb.db               :as db]
            [fetch.fdb.op               :as op]
            [fetch.fdb.space            :as space]))

(def ^:dynamic *store* nil)
(def ^:dynamic *fdb* nil)
(def ^:dynamic *lease* nil)
(def ^:dynamic *watch* nil)
(def ^:dynamic *kv* nil)
(def ^:dynamic *server* nil)
(def ^:dynamic *ports* nil)
(def ^:dynamic *system* nil)

(defn purge-directories
  [{::keys [fdb]}]
  ;; When things go wrong during local tests, it can be useful to have a way
  ;; to inspect the values generated in the database.
  (let [prevent-purge? (some? (or (System/getenv "FETCH_TEST_PREVENT_PURGE")
                                  (System/getProperty "fetch.prevent-purge")))]
    (when-not prevent-purge?
      (db/write-transaction
       fdb
       (fn [tx dirs]
         (doseq [[k d] dirs :when (simple-keyword? k)]
           (op/clear-range tx (space/subrange d))
           (db/remove-dir tx d))
         (op/clear-range tx (space/subrange (::db/instance dirs)))
         (db/remove-dir tx (::db/instance dirs))
         (op/clear-range tx (space/subrange (::db/top dirs)))
         (db/remove-dir tx (::db/top dirs))))
      (doseq [[k d] (-> fdb ::db/dirs)
              :when (simple-keyword? k)]
        (db/remove-dir (db/get-handle fdb) d))
      (db/remove-dir (db/get-handle fdb) (-> fdb ::db/dirs ::db/instance))
      (db/remove-dir (db/get-handle fdb) (-> fdb ::db/dirs ::db/top)))))

(def cleaner
  (with-meta {} {`component/stop purge-directories}))

(defn fdb-system
  []
  {::fdb/cluster-file "/etc/foundationdb/fdb.cluster"
   ::fdb/prefix       (name (gensym "test-etcd"))
   ::fdb              (using fdb/handle [::fdb/cluster-file ::fdb/prefix])
   ::store/engine     (using fdb/store {:db ::fdb})
   ::cleaner          (using cleaner [::fdb])})

(defn wrap-system-fn
  [sys]
  (fn [f]
    (let [started (component/start-system (into (component/system-map) (sys)))]
      (binding [*system* started
                *store*  (::store/engine started)
                *fdb*    (::fdb started)
                *lease*  (::lease started)
                *watch*  (::watch started)
                *kv*     (::kv started)
                *server* (::server started)
                *ports*  {}]
        (try (f) (finally (component/stop-system started)))))))


(def test-systems
  {:fdb (wrap-system-fn fdb-system)})

(defn test-system
  [k]
  (or (get test-systems k)
      (throw (IllegalArgumentException. (str "unknown system: " k)))))
