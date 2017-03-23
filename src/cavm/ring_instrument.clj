(ns
  ^{:author "Brian Craft"
    :doc "default instrumentation for ring"}
  cavm.ring-instrument
  (:require ring.middleware.stacktrace))

(defn noop [handler & args] handler)

(def wrap-trace noop)
(def wrap-reload noop)
; XXX Should generate better error messages, instead of dumping stack, but
; this is better than nothing.
(def wrap-stacktrace-web ring.middleware.stacktrace/wrap-stacktrace-web)
