## FoundationDB implemenation of the storage engine

### `create-if-absent`

The previous revision of the key is fetched, if a value is found, the current
database revision is returned. Otherwise, the value will be set, the
revision is incremented and returned.


``` clojure
(defn create-if-absent
  [tx dirs key value lease]
  (let [previous (common/previous tx dirs key)
        lease-id (if (some? lease) lease 0)]
    (if (some? previous)
      [(common/highest-revision tx dirs) false]
      (let [new-rev (common/increment-revision tx dirs)]
        @(a/chain (op/set tx (p/key dirs key new-rev)
                          (p/encode-val lease-id 0 value))
                  (constantly [new-rev true]))))))
```

### `update-at-revision`

The previous revision of the key is fetched, if a different revision than
the one provided as an argument is found, the current database revision is
returned. Otherwise, the value will be set, the
revision is incremented and returned.
