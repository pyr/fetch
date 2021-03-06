(ns fetch.fdb.payload
  "FoundationdB implementation of all key and value serialization and
   deserialization functions."
  (:refer-clojure :exclude [key val])
  (:require [fetch.fdb.space :as space]
            [fetch.fdb.kv    :as kv]
            [fetch.fdb.tuple :as tuple]))

;; Revision handling
;; =================

(defn revision-key
  [dirs]
  (space/from dirs :metadata "r"))

(defn byte-count-key
  [dirs]
  (space/from dirs :metadata "b"))

(defn encode-long
  [revision]
  (tuple/pack-vals revision))

(defn decode-long

  [ba]
  (some-> ba tuple/decode tuple/get-long))

;; Key handling
;; ============

(defn key
  [dirs k revision]
  (space/from dirs :keys k revision))

(defn key-range [dirs k]
  (space/range dirs :keys k))

(defn- inc-prefix
  "Given an object path, yield the next semantic one."
  [^String p]
  (when (seq p)
    (let [[c & s] (reverse p)]
      (->> (-> c int inc char)
           (conj s)
           reverse
           (reduce str "")))))

(defn key-prefix
  [dirs prefix]
  (space/bounded-range (space/from dirs :keys prefix)
                       (space/from dirs :keys (inc-prefix prefix))))

(defn schema-key [dirs]
  (space/from dirs :schema))

(defn decode-key
  [dirs kba]
  (let [[k rev] (some-> (space/by-name dirs :keys)
                        (space/unpack kba)
                        (tuple/expand))]
    {:mod-revision rev
     :key          k}))

(defn decode-value
  [vba]
  (let [[lease create-rev value] (some-> vba tuple/decode-and-expand)]
    {:lease           lease
     :create-revision create-rev
     :value           value}))

(defn decode-keyval
  [dirs kv]
  (let [[kba vba] (kv/as-tuple kv)]
    (merge (decode-key dirs kba) (decode-value vba))))

(defn encode-val
  [lease-id create-revision value]
  (tuple/pack-vals lease-id create-revision value))

;; Watch handling
;; ==============

(defn watch-range [dirs]
  (space/range dirs :watches))

(defn watch-key [dirs prefix]
  (space/from dirs :watches prefix))

(defn watch-instance-key [dirs instance]
  (space/from dirs :instances instance))

(defn events-range [dirs instance]
  (space/range dirs :events instance))

(defn event-key [dirs instance revision]
  (space/from dirs :events instance revision))

(defn watch-event
  [index key]
  (tuple/pack-vals index key))

(defn schema
  [version]
  (tuple/pack-vals version))

(defn decode-watch
  [^bytes ba]
  (some-> ba tuple/decode-and-expand))

(defn encode-watch
  [instance id revision]
  (tuple/pack-vals instance id revision))
