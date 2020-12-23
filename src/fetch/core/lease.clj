(ns fetch.core.lease
  "Components needed to provide unique enough IDs.
   We use a flake algorithm (see below) which uses a 41bit
   clock."
  (:require [clojure.spec.alpha :as s]
            [exoscale.ex        :as ex]))

(def exoscale-epoch
  "By the time this overflows, we will all be dead."
  1479822304034)

(def time-shift
  "Bit shift position of timestamp"
  22)

(def host-shift
  "Bit shift position of host ID"
  10)

(def host-mask
  "Bit width of host ID (12 bits)"
  0xfff)

(def time-mask
  "Bit width of timestamp (41 bits)"
  0x1ffffffffff)

(def seq-mask
  "Bit widht of sequence ID (10 bits)"
  0x3ff)

;; blob id generation
;; ==================
;;
;; Blobs are identified by the tuple [partition-id, blob-id].
;; - Partitions have unsigned integer IDs spanning 64 bits.
;; - Blobs have unsigned integer IDs spanning 64 bits.
;;
;; Partition are allocated in blobd's configuration directly and
;; are never dynamically generated.
;;
;; Blobs are created on demand and need strong unicity guarantees.
;; We propose the following layout for blob IDs, as inspired by
;; the Twitter snowflake approach:
;;
;; - Host ID: 12 bit. 4096 maximum hosts.
;; - Sequence ID: 10 bit. 1024 before overflow.
;; - Time: 41 bit. Custom epoch with millisecond precision over ~ 70 years.
;;
;; +--------------+----------------+-----------------+
;; | 41-bit epoch | 12-bit host ID | 10-bit sequence |
;; +--------------+----------------+-----------------+
;;
;; This is a variation of the approach described here:
;;
;; https://github.com/twitter/snowflake/tree/snowflake-2010, and a
;; previous implementation in C by yours truly here:
;; https://github.com/antirez/redis/pull/610


(defn inc-sequence
  "Increment sequence ID, ensure that we wrap around at
   the 10 bit mark."
  [n]
  (bit-and 0x3ff (inc n)))

(defn generate
  [{::keys [host-id seq-id]}]
  (let [epoch (- (System/currentTimeMillis) exoscale-epoch)]
    (bit-or
     (bit-and (swap! seq-id inc-sequence) seq-mask)
     (bit-shift-left (bit-and host-id host-mask) host-shift)
     (bit-shift-left (bit-and epoch time-mask) time-shift))))

(defn make-generator
  [config]
  (with-meta config
    {'com.stuartsierra.component/start #(assoc (ex/assert-spec-valid ::config %)
                                               ::seq-id (atom 0))
     'com.stuartsierra.component/stop  #(dissoc % ::seq-id)}))

(def generator
  (make-generator {}))

(s/def ::seq-id (s/int-in 0 1024))
(s/def ::config (s/keys :req [::host-id]))
