(ns riak-mesos.scheduler
  (:require [clj-mesos.scheduler]
            [riak-mesos.rest])
  (:gen-class))

(def garbage-hack "I hate this")

(defn scheduler
  []
  (let [pending (atom #{1 2})
        used-hosts (atom #{})
        ;;this contains pairs of [executor-id slave-id]
        active-executors (atom #{})
        executor-id->hostname (atom {})]
    (clj-mesos.scheduler/scheduler
      (statusUpdate [driver status]
                    (future
                      (locking garbage-hack
                        (println "got status" status)
                        (when-let [[executor-id slave-id] (first @active-executors)]
                          (future
                            (Thread/sleep 30000)
                            (let [command ["riak-admin" "join" "-f" (get executor-id->hostname executor-id)]]
                              (println "sending command" command)
                              (clj-mesos.scheduler/send-framework-message driver executor-id slave-id (.getBytes (pr-str command))))))
                        (when (= :task-running (:state status))
                          (swap! active-executors conj [(:executor-id status) (:slave-id status)])))))
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
                                    (swap! executor-id->hostname assoc (str "riak-node-executor-" node) (:hostname offer))
                                    (println "launching task for node" node "(offer)" offer)
                                    (clj-mesos.scheduler/launch-tasks
                                      driver
                                      (:id offer)
                                      [{:name "Riak"
                                        :task-id (str "riak-node-" node)
                                        :slave-id (:slave-id offer)
                                        :resources {:cpus 2.0
                                                    :mem 2000.0}
                                        ;:container {:type :docker :docker "rtward/riak-mesos"}
                                        ;:command {:shell false}
                                        :executor {:executor-id (str "riak-node-executor-" node)
                                                   :container {:type :docker
                                                               :docker "rtward/riak-mesos"
                                                               :volumes [{:container-path "/usr/local/lib/mesos"
                                                                          :host-path "/usr/local/lib"
                                                                          :mode :ro}]}
                                                   :command {:shell false}}
                                        }]))
                                (clj-mesos.scheduler/decline-offer driver (:id offer))))
                            (catch Exception e
                              (.printStackTrace e)))))))))
 

(defn -main
  [master]
  (let [pending (atom #{})
        running (atom #{})
        sched (scheduler pending running)
        driver (clj-mesos.scheduler/driver sched
                                           {:user ""
                                            :name "riak"}
                                           master)]
    (clj-mesos.scheduler/start driver)
    (riak-mesos.rest/start-server pending running 8081)
    (println "started")
    (Thread/sleep 1000000)))

(comment
  (try
    (clj-mesos.marshalling/map->proto org.apache.mesos.Protos$ContainerInfo {:type :docker
                                                                    :docker "rtward/riak-mesos"
                                                                    :volumes [{:container-path "/usr/local/lib/mesos"
                                                                               :host-path "/usr/local/lib"
                                                                               :mode :ro}]})
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
<<<<<<< HEAD
                                        :task-id (str "riak-node-" node)
                                        :slave-id (:slave-id offer)
                                        :resources {:cpus 2.0
                                                    :mem 2000.0}
                                        ;:container {:type :docker :docker "rtward/riak-mesos"}
                                        ;:command {:shell false}
                                        :executor {:executor-id (str "riak-node-executor-" node)
                                                        :container {:type :docker
                                                                    :docker "rtward/riak-mesos"
                                                                    :volumes [{:container-path "/usr/local/lib/mesos"
                                                                               :host-path "/usr/local/lib"
                                                                               :mode :ro}]}
                                                        :command {:shell false}}
                                        }))
=======
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
>>>>>>> daac222f5b2b6c3fd0b29abb9da7df084cc22eae
  (-main)
  )
