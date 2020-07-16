(ns cavm.events
  (:require [cavm.json :as json])
  (:require [clojure.tools.logging :as log :refer [warn]])
  (:require [ring.adapter.jetty9 :refer [send! close!]]))

(def listeners (atom #{}))

(defn ws-handler [queue]
  {:on-connect (fn [ws]
                 (swap! listeners conj ws)
                 (send! ws (json/write-str {:queue @queue})))
   :on-error (fn [ws e]
               (swap! listeners disj ws))
   :on-close (fn [ws status-code reason]
               (swap! listeners disj ws))
   :on-text (fn [ws text-message]
              (warn "Unexpected text message"))
   :on-bytes (fn [ws bytes offset len]
              (warn "Unexpected text message"))})

(defn jetty-config [queue]
  (add-watch queue :notify (fn [_ _ _ curr]
                             (doseq [l @listeners]
                               (send! l (json/write-str {:queue curr})))))
  {"/load-events" (ws-handler queue)})

(defn jetty-config-dispose [queue]
  (doseq [l @listeners]
    (close! l))
  (reset! listeners #{}))
