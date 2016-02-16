(defproject com.smxemail/cassandra-component "1.0.0-SNAPSHOT"
  :description "SMX Cassandra / Component Integration"
  :min-lein-version "2.3.0"
  :source-paths ["src"]
  :javac-options ["-target" "1.7" "-source" "1.7"]
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [cc.qbits/alia "2.12.1"]
                 [prismatic/schema "1.0.3"]
                 [com.stuartsierra/component "0.3.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [ch.qos.logback/logback-classic "1.1.3"]])