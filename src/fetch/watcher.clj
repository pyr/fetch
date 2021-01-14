(ns fetch.watcher
  (:require [qbits.auspex :as a]
            [fetch.store  :as store])
  (:import java.util.UUID))

(defprotocol WatchPublisher
  (publish-events [this watch-id revision events]))

(defn watch-loop
  [engine publisher instance]
  (a/loop []
    (a/chain
     (store/register-watch-listener engine instance)
     (fn [{:keys [events-by-watch continue?]}]
       (doseq [{:keys [events watch-id revision]} events-by-watch
               :when                              (seq events)]
         ;; XXX: enough params here?
         (publish-events publisher watch-id revision events))
       (when continue?
         (a/recur))))))

(defn make-watcher
  [engine publisher]
  (let [instance (UUID/randomUUID)
        watcher  {::engine    engine
                  ::publisher publisher
                  ::next-id   (atom 0)
                  ::instance  instance}]
    (store/create-watch-instance engine instance)
    (assoc watcher ::loop (watch-loop engine publisher instance))))

(defn create-watch
  [watcher prefix revision]
  (let [id (swap! (::next-id watcher) inc)]
    (store/register-key-watch (::engine watcher) (::instance watcher)
                              id prefix revision)
    id))

(defn cancel-watch
  [watcher id]
  (store/cancel-key-watch (::engine watcher) (::instance watcher) id))

(defn stop-watcher
  [watcher]
  (a/cancel! (::loop watcher))
  (store/delete-watch-instance (::engine watcher) (::instance watcher)))
