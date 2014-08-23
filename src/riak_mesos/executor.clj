(ns riak-mesos.executor
  (:require
    [clj-mesos.executor :as exec]
    clojure.java.shell)
  (:gen-class))

(defn riak-executor []
  (let [info (atom nil)]
    (exec/executor
      (registered [driver executor framework slave]
                  (reset! info {:executor (:executor-id executor) :slave (:slave-id slave)})
                  (println "Framework Registered"))
      (launchTask [driver task-info]
                  (println "[launchTask] Sending status Update")
                  (exec/send-status-update driver {:task-id (:task-id task-info)
                                                   :state :task-running
                                                   :data (.getBytes (pr-str @info))}))
      (frameworkMessage [driver bytes]
                        (try
                          (println "got framework message")
                          (let [command-string (read-string (String. bytes "UTF-8"))]
                            (println "[frameworkMessage] Running command " command-string )
                            (future (println (apply clojure.java.shell/sh  (clojure.string/split command-string #"\s+") ))))
                          (catch Exception e
                            (.printStackTrace e)))))))



(defn -main
  []
  (let [exec (riak-executor)
        driver (exec/driver exec)]
    (exec/start driver)
    (while true
      (Thread/sleep 100000))))
