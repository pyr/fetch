(ns fetch.api.put
  "Abstract handling of PUT operations for etcd.
   This does not rely on concrete operations provided by a database
   implementation such as FoundationDB but goes through the protocoled
   operations available from `fetch.api`."
  (:require [fetch.api.common   :as common]
            [fetch.fdb.payload  :as p]
            [fetch.fdb.op       :as op]
            [exoscale.ex        :as ex]
            [clojure.tools.logging :as log]))

(defn handle
  [tx sz {:etcd.api.kv/keys [key value lease previous?
                             ignore-value? ignore-lease?]
          :as               req}]

  (log/info "in put request for request:" (pr-str req))
  (when (true? previous?)
    (ex/ex-unsupported! "prevKv is unsupported in put requests"))
  (when (true? ignore-value?)
    (ex/ex-unsupported! "ignoreValue is unsupported in put requests"))
  (when (true? ignore-lease?)
    (ex/ex-unsupported! "ignoreLease is unsupported in put requests"))

  )
