(ns fetch.fdb.store.common
  (:require [fetch.fdb.payload     :as p]
            [fetch.fdb.op          :as op]
            [clojure.tools.logging :as log]))

(defn previous
  [tx dirs k]
  (some->> (p/key-range dirs k)
           (op/reverse-range tx)
           (first)
           (p/decode-keyval dirs)))

(defn at-revision?
  [tx dirs k rev]
  (let [p (previous tx dirs k)]
    (when (or (and (zero? rev) (nil? p))
              (= rev (:mod-revision p)))
      p)))

(defn highest-revision
  [tx dirs]
  (some->> (p/revision-key dirs)
           (op/get tx)
           (p/decode-revision)))

(defn increment-revision
  [tx dirs]
  (let [rk     (p/revision-key dirs)
        value  (op/get tx rk)
        found? (some? value)
        rev    (inc (if found? (p/decode-revision value) 0))]
    (when-not found?
      (log/warn "revision is unset, will initialize to" rev))
    (op/set tx rk (p/encode-revision rev))
    rev))
