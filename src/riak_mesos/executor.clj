(ns riak-mesos.executor
  (:require
   [clj-mesos.executor :as exec]
   clojure.java.shell)
  (:gen-class))

(defn riak-executor []
  (exec/executor (registered [driver executor framework slave]
                                              (println "Framework Registered"))
                 (launchTask [driver task-info]
                             (println "[launchTask] Sending status Update")
                             (exec/send-status-update driver {:task-id (:task-id task-info)
                                                              :task-state 1
                                                              :message "Riak Task is running" } ))
                 (frameworkMessage [driver bytes] (let [command-string (read-string (String. bytes "UTF-8"))]
                                                    (println "[frameworkMessage] Running command " command-string )
                                                    (future (apply clojure.java.shell/sh  command-string ))))))



(defn -main
  [master]
  (let [exec (riak-executor)
        driver (exec/driver exec)]
     (exec/start driver)
  (Thread/sleep 100000)))
