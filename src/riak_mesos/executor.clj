(ns riak-mesos.executor
  (:require
            [clj-mesos.executor :as exec])
  (:gen-class))

;; (def riak-scheduler (sch/scheduler
;;                      (resourceOffers [driver offers]
;;                                       (let [[offer] offers]
;;                                         (sch/launch-tasks driver (:offer-id offer)
;;                                                          [{:name "riak-task"
;;                                                            :task-id "riak-01"
;;                                                            :slave-id (:slave-id offer)
;;                                                            :resources {:cpus 1.0
;;                                                                        :mem 100.0}
;;                                                            :command {:value "sleep 10"}}])))))

(defn riak-executor []
  (exec/executor (registered [driver executor framework slave]
                                              (println "Framework Registered"))
                                  (launchTask [driver task-info]
                                              (exec/send-status-update driver {:task-id (:task-id task-info)
                                                                               :task-state 1
                                                                               :message "Riak Task is running" } ))))



(defn -main
  [master]
  (let [exec (riak-executor)
        driver (exec/driver exec)])
  (executor/start driver))
