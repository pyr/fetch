(ns fetch.api.txn
  (:require [exoscale.ex :as ex]))

(defn handle
  [_ _ _]
  (ex/ex-unsupported! "txn is not supported"))
