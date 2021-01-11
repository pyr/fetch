(ns fetch.fdb.store-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [fetch.test.system :as sys :refer [test-system *store*]]
            [fetch.store :as store]))

(use-fixtures :once (test-system :fdb))

(defn str->ba [s] (.getBytes (str s)))
(defn ba->str [ba] (String. ^bytes ba "UTF-8"))

(deftest store-single-key-test
  (testing "empty directory is empty"
    (is (nil? (store/get-latest *store* "dummy"))))

  ;; Now we add a first key to the key space
  (let [k          "dummy"
        prefix     "dum"
        l1         0
        v1         "hello"
        l2         38
        v2         "bye"
        [rev1 ok?] (store/create-if-absent *store* k (str->ba v1) l1)]

    (testing "adding a key worked"
      (is (true? ok?))
      (is (nat-int? rev1)))
    (testing "can figure out latest revision of a key"
      (is (= {:mod-revision    rev1
              :lease           l1
              :key             k
              :value           v1
              :create-revision 0}
             (update (store/get-latest *store* k) :value ba->str))))
    (testing "can get key at specific revision"
      (is (= {:mod-revision    rev1
              :lease           l1
              :key             k
              :value           v1
              :create-revision 0}
             (update (store/get-at-revision *store* k rev1) :value ba->str))))

    ;; We update the same key
    (let [[rev2 ok?] (store/update-at-revision *store* k rev1 (str->ba v2) l2)]
      (testing "updating a key worked"
        (is (true? ok?))
        (is (nat-int? rev2)))
      (testing "revisions grow in time"
        (is (> rev2 rev1)))
      (testing "can retrieve old version"
        (is (= {:mod-revision    rev1
                :lease           l1
                :key             k
                :value           v1
                :create-revision 0}
               (update (store/get-at-revision *store* k rev1) :value ba->str))))
      (testing "can retrieve new version"
        (is (= {:mod-revision    rev2
                :lease           l2
                :key             k
                :value           v2
                :create-revision 0}
               (update (store/get-at-revision *store* k rev2) :value ba->str)))
        (is (= {:mod-revision    rev2
                :lease           l2
                :key             k
                :value           v2
                :create-revision 0}
               (update (store/get-latest *store* k) :value ba->str))))
      (testing "count only takes last revision into account"
        (is (= [rev2 1] (store/count-keys *store* k))))
      (testing "can retrieve the key with an exact range"
        (is (= [{:mod-revision rev2
                 :key          k}]
               (store/range-keys *store* rev1 500 k))))
      (testing "can retrieve the key with a prefix range"
        (is (= [{:mod-revision rev2
                 :key          k}]
               (store/range-keys *store* rev2 500 prefix))))
      (comment
        (testing "can delete key"
          (is (true? (second (store/delete-key *store* k rev2)))))
        (testing "cannot delete key a second time"
          (is (false? (second (store/delete-key *store* k rev2)))))))))
