(ns riak-mesos.scheduler
  (:require [clj-mesos.scheduler]))


(defn scheduler
  []
  (let [pending (atom #{1 2 3})]
    (clj-mesos.scheduler/scheduler
      (resourceOffers [driver offers]
                       (when (seq @pending)
                         (doseq [offer offers
                                 :let [{:keys [cpus mem]} (:resources offer)
                                       _ (println offer)
                                       _ (println "stuff" cpus mem)
                                       node (first @pending)
                                       _ (println "node " node)
                                       _ (println "truthy" (and (>= cpus 1.0)
                                            (>= mem 200.0)))
                                       ]
                                 :when (and (>= cpus 1.0)
                                            (>= mem 200.0))]
                           (swap! pending disj node)
                           (println "launching task!" offer)
                           (clj-mesos.scheduler/launch-tasks
                                   driver
                                   (:id offer)
                                   [{:name "Riak"
                                     :task-id (str "riak-node-" node)
                                     :slave-id (:slave-id offer)
                                     :resources {:cpus 1.0
                                                 :mem 200.0}
                                     :container {:type :docker :Docker {:image "rtward/riak-mesos"}}
                                     :command {}
                                     ;:executor-info {:executor-id (str "riak-node-executor-" node)
                                     ;                :container {:image "rtward/riak-mesos"}
                                     ;                :command {}}
                                     }])
                           (println "launched task")
                           ))))))

(defn -main
  [master]
  (let [sched (scheduler)
        driver (clj-mesos.scheduler/driver sched
                                           {:user ""
                                            :name "riak"}
                                           master)]
    (clj-mesos.scheduler/start driver)
    (Thread/sleep 1000000)))

(comment
  (try
    (clj-mesos.marshalling/map->proto org.apache.mesos.Protos$ContainerInfo {:type :docker :Docker {:image "rtward/riak-mesos"}})
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
