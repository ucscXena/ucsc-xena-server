(ns
  ^{:author "Brian Craft"
    :doc "dev instrumentation for ring"}
  cavm.ring-instrument
  (:require liberator.dev)
  (:require ring.middleware.reload)
  (:require ring.middleware.stacktrace))


(def wrap-trace liberator.dev/wrap-trace)
(def wrap-reload ring.middleware.reload/wrap-reload)
(def wrap-stacktrace-web ring.middleware.stacktrace/wrap-stacktrace-web)
