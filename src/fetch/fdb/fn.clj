(ns fetch.fdb.fn
  (:import java.util.function.Function))

(defn ^Function wrap
  [f]
  (reify Function (apply [this arg] (f arg))))

(defmacro defunction
  [sym & fntail]
  `(def ~sym (wrap (fn ~@fntail))))
