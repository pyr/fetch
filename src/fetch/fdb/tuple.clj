(ns fetch.fdb.tuple
  (:refer-clojure :exclude [range])
  (:import com.apple.foundationdb.tuple.Tuple))

(defprotocol Tuplable
  :extend-via-metadata true
  (as-tuple [this]))

(defn ^Tuple from-seq
  [objs]
  (Tuple/from (into-array Object objs)))

(defn ^Tuple from
  [& objs]
  (from-seq objs))

(defn ^bytes pack
  [^Tuple t]
  (.pack t))

(defn ^String get-string
  [^Tuple t index]
  (.getString t (int index)))

(defn ^Tuple decode
  [^bytes b]
  (Tuple/fromBytes b))

(defn expand
  [^Tuple t]
  (.getItems t))

(def decode-and-expand
  (comp expand decode))

(defn ^Tuple encode
  [x]
  (cond
    (string? x)                    (from x)
    (bytes? x)                     (from x)
    (int? x)                       (from x)
    (satisfies? Tuplable x)        (as-tuple x)
    (contains? (meta x) `as-tuple) (as-tuple x)
    (coll? x)                      (from-seq x)))

(defn ^bytes encode-and-pack
  [x]
  (-> (encode x) (pack)))

(defn get-long
  ([t]
   (get-long t 0))
  ([^Tuple t index]
   (.getLong t (long index))))

(defn unpack
  [^Tuple t]
  (.unpack t))

(defn range
  [^Tuple t]
  (.range t))

(defn pack-vals
  [& vals]
  (-> (vec vals) from-seq pack))
