(ns fetch.fdb.transaction
  (:import java.util.function.Function
           com.apple.foundationdb.Database))

(defn ^Function make-fun
  [f dirs]
  (reify Function (apply [this arg] (f arg dirs))))

(defn write-transaction
  [{:fetch.fdb.db/keys [database dirs]} f]
  (.run ^Database database (make-fun f dirs)))

(defn read-transaction
  [{:fetch.fdb.db/keys [database dirs]} f]
  (.read ^Database database (make-fun f dirs)))
