(ns fetch.api.delete-range
  (:require [exoscale.ex :as ex]))

(defn handle
  [_ _ _]
  (ex/ex-unsupported! "delete is not supported"))
