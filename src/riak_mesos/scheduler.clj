(ns riak-mesos.scheduler
  (:require [clj-mesos.scheduler])
  (:gen-class))

(def garbage-hack "I hate this")

(defn scheduler
  []
  (let [pending (atom #{1 2})
        used-hosts (atom #{})]
    (clj-mesos.scheduler/scheduler
      (statusUpdate [driver status]
                    (println "got status" status))
      (resourceOffers [driver offers]
                      (future
                        (locking garbage-hack
                          (try
                            (doseq [offer offers
                                    :let [{:keys [cpus mem]} (:resources offer)
                                          node (first @pending)]]
                              (if (and node
                                       (>= cpus 2.0)
                                       (>= mem 2000.0)
                                       (not (contains? @used-hosts (:hostname offer))))
                                (do (swap! pending disj node)
                                    (swap! used-hosts conj (:hostname offer))
                                    (println "launching task for node" node "(offer)" offer)
                                    (clj-mesos.scheduler/launch-tasks
                                      driver
                                      (:id offer)
                                      [{:name "Riak"
                                        :task-id (str "riak-node-" node)
                                        :slave-id (:slave-id offer)
                                        :resources {:cpus 2.0
                                                    :mem 2000.0}
                                        :container {:type :docker :docker "rtward/riak-mesos"}
                                        :command {:shell false}
                                        ;:executor-info {:executor-id (str "riak-node-executor-" node)
                                        ;                :container {:image "rtward/riak-mesos"}
                                        ;                :command {}}
                                        }]))
                                (clj-mesos.scheduler/decline-offer driver (:id offer))))
                            (catch Exception e
                              (.printStackTrace e)))))))))

(defn -main
  [master]
  (let [sched (scheduler)
        driver (clj-mesos.scheduler/driver sched
                                           {:user ""
                                            :name "riak"}
                                           master)]
    (clj-mesos.scheduler/start driver)
    (println "started")
    (Thread/sleep 1000000)))

(comment
  (try
    (clj-mesos.marshalling/map->proto org.apache.mesos.Protos$ContainerInfo {:type :docker :docker "rtward/riak-mesos"})
    (catch Exception e
      (println e)
      (println (.getFullName (:field (ex-data e))))
      )
    )
  (try
    (clj-mesos.marshalling/map->proto org.apache.mesos.Protos$ContainerInfo$DockerInfo {:image "rtward/riak-mesos"})
    (catch Exception e
      (println e)
      )
    )
(bean org.apache.mesos.Protos$ContainerInfo$DockerInfo)
  (let [node 1
        offer {:slave-id "aoeu"}
        ] (clj-mesos.marshalling/map->proto org.apache.mesos.Protos$TaskInfo {:name "Riak"
                                                                               :task-id (str "riak-node-" node)
                                                                               :slave-id (:slave-id offer)
                                                                               :resources {:cpus 1.0
                                                                                           :mem 200.0}
                                                                               :container {:type :docker :Docker {:image "rtward/riak-mesos"}}
                                                                               :command {}
                                                                               ;:executor-info {:executor-id (str "riak-node-executor-" node)
                                                                               ;                :container {:image "rtward/riak-mesos"}
                                                                      ;                :command {}}
                                                                      }))
  (-main)
  )
