(ns 
  ^{:author "Brian Craft"
    :doc "c3p0 connection pool utilities."}
  cavm.conn-pool
  (:import (com.mchange.v2.c3p0 ComboPooledDataSource)))

(defn pool
  "Create a connection pool."
  [{:keys [classname subprotocol subname
           user password
           excess-timeout idle-timeout
           minimum-pool-size maximum-pool-size
           test-connection-query idle-connection-test-period
           test-connection-on-checkin test-connection-on-checkout
           conn-customizer]
    :or {excess-timeout (* 30 60)
         idle-timeout (* 3 60 60)
         minimum-pool-size 3
         maximum-pool-size 15
         test-connection-query nil
         idle-connection-test-period 0
         test-connection-on-checkin false
         test-connection-on-checkout false
         conn-customizer nil}}]
  {:datasource (doto (ComboPooledDataSource.)
                 (.setDriverClass classname)
                 (.setJdbcUrl (str "jdbc:" subprotocol ":" subname))
                 (.setUser user)
                 (.setPassword password)
                 (.setMaxIdleTimeExcessConnections excess-timeout)
                 (.setMaxIdleTime idle-timeout)
                 (.setMinPoolSize minimum-pool-size)
                 (.setMaxPoolSize maximum-pool-size)
                 (.setIdleConnectionTestPeriod idle-connection-test-period)
                 (.setTestConnectionOnCheckin test-connection-on-checkin)
                 (.setTestConnectionOnCheckout test-connection-on-checkout)
                 (.setPreferredTestQuery test-connection-query)
                 (.setConnectionCustomizerClassName conn-customizer))})
