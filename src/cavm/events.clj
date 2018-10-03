(ns cavm.events
  (:require [cavm.json :as json])
  (:require [com.keminglabs.jetty7-websockets-async.core :refer [configurator]]
            [clojure.core.async :refer [chan go >! <! >!! thread tap untap mult]]
            [ring.adapter.jetty :refer [run-jetty]]))

(defn handle-client [notify {in :in}]
  (let [ev (chan)]
    (tap notify ev)
    (go (loop []
          (let [msg (<! ev)]
            (if (>! in (json/write-str {:queue msg}))
              (recur)
              (untap notify ev)))))))

(defn listen [queue notify c]
  (go (loop []
        (let [ws-req (<! c)]
          (>! (:in ws-req) (json/write-str {:queue @queue}))
          (handle-client notify ws-req)
          (recur)))))

(defn jetty-config [queue]
  (let [c (chan)
        n (chan)]
    (add-watch queue :notify (fn [_ _ _ curr] (>!! n curr)))
    (thread (listen queue (mult n) c))
    (configurator c {:path "/load-events"})))
