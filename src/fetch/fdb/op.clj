(ns fetch.fdb.op
  (:refer-clojure :exclude [get set])
  (:import com.apple.foundationdb.Range
           com.apple.foundationdb.ReadTransaction
           com.apple.foundationdb.Transaction))

(defn get
  [^ReadTransaction tr ^bytes k]
  @(.get tr k))

(defn set
  [^Transaction tr ^bytes k ^bytes v]
  (.set tr k v))

(defn clear
  [^Transaction tr ^bytes k]
  (.clear tr k))

(defn clear-range
  [^Transaction tr ^Range r]
  (.clear tr r))

(def range-no-limit
  ReadTransaction/ROW_LIMIT_UNLIMITED)

(defn range-with-range
  ([^ReadTransaction tr ^Range rangedef limit reverse?]
   (-> (.getRange tr rangedef (int limit) (boolean reverse?))
       (.asList)
       (deref))))

#_(defn range-with-boundaries
  ([^ReadTransaction tr begin end reverse?]
   (.range tr begin end reverse?)))

(defn reverse-range
  ([tx range]
   (range-with-range tx range range-no-limit true))
  ([tx range limit]
   (range-with-range tx range (int limit) true)))

(defn watch
  [^Transaction tx ^bytes key]
  (.watch tx key))
