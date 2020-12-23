(ns fetch.api.common
  (:require [fetch.fdb.payload     :as p]
            [fetch.fdb.op          :as op]
            [clojure.tools.logging :as log]))

(defn previous
  [tx sz k]
  (some->> (p/key-range sz k)
           (op/reverse-range tx)
           (first)
           (p/decode-keyval sz)))

(defn increment-revision
  [tx sz]
  (let [rk     (p/revision-key sz)
        value  (op/get tx rk)
        found? (some? value)
        rev    (inc (if found? (p/decode-revision value) 0))]
    (when-not found?
      (log/warn "revision is unset, will initialize to 1"))
    (op/set tx rk (p/encode-revision rev))
    rev))
