(ns fetch.api.proto)

(defprotocol KVProto
  (-range [this req])
  (-put [this req])
  (-delete-range [this req])
  (-txn [this req])
  (-compact [this req]))

(defprotocol WatchProto
  (-watch [this stream]))

(defprotocol LeaseProto
  (-lease-grant [this req])
  (-lease-revoke [this req])
  (-lease-keep-alive [this stream])
  (-lease-time-to-live [this req]))

(defprotocol AuthProto
  (-authenticate [this req]))
