(ns riak-mesos.scheduler
  (:require [clj-mesos.scheduler]
            [riak-mesos.rest])
  (:gen-class))

(def garbage-hack "I hate this")

(defn scheduler
  [pending running]
  []
  (let [used-hosts (atom #{})
        ;;this contains pairs of [executor-id slave-id]
        active-executors (atom #{})
        slave-id->host+exec (atom {})]
    (clj-mesos.scheduler/scheduler
      (statusUpdate [driver status]
                    (future
                      (try
                        (locking garbage-hack
                          (println "got status" status)
                          (when-let [{:keys [executor-id slave-id]} (first @active-executors)]
                            (future
                              (let [{:keys [executor slave]} (read-string (String. (:data status) "UTF-8"))
                                    command ["riak-admin" "join" "-f" (str "riak@" (get-in @slave-id->host+exec [slave-id :hostname]))]]
                                (println "sending command" command "to" executor slave)
                                (Thread/sleep 30000)
                                (println "sent")
                                (clj-mesos.scheduler/send-framework-message driver executor slave (.getBytes (pr-str command))))))
                          (when (= :task-running (:state status))
                            (swap! active-executors conj (read-string (String. (:data status) "UTF-8"))))
                          (let [id-from-status (comp #(Integer/parseInt %) str last :task-id)] 
                            (cond 
                              (= (:task-state status) :task-staging) nil
                              (= (:task-state status) :task-starting) nil
                              (= (:task-state status) :task-running)
                              (swap! running conj (id-from-status status))
                              true 
                              (swap! pending conj (id-from-status status)))))
                        (catch Exception e
                          (.printStackTrace e)))))
      (resourceOffers [driver offers]
                      (future
                        (locking garbage-hack
                          (try
                            (doseq [offer offers
                                    :let [{:keys [cpus mem]} (:resources offer)
                                          node (first @pending)]]
                              (if (and node
                                       (>= cpus 1.0)
                                       (>= mem 200.0)
                                       (not (contains? @used-hosts (:hostname offer))))
                                (do (swap! pending disj node)
                                    (swap! used-hosts conj (:hostname offer))
                                    (swap! slave-id->host+exec assoc (:slave-id offer) {:hostname (:hostname offer) :executor (str "riak-node-executor-" node)})
                                    (println "launching task for node" node "(offer)" offer)
                                    (clj-mesos.scheduler/launch-tasks
                                      driver
                                      (:id offer)
                                      [{:name "Riak"
                                        :task-id (str "riak-node-" node)
                                        :slave-id (:slave-id offer)
                                        :resources {:cpus 1.0
                                                    :mem 200.0}
                                        ;:container {:type :docker :docker "rtward/riak-mesos"}
                                        ;:command {:shell false}
                                        :executor {:executor-id (str "riak-node-executor-" node)
                                                   :container {:type :docker
                                                               :docker "rtward/riak-mesos"
                                                               :volumes [{:container-path "/usr/local/lib/mesos"
                                                                          :host-path "/usr/local/lib"
                                                                          :mode :ro}
                                                                         {:container-path "/var/log/riak-executor"
                                                                          :host-path "/var/log/riak-executor"
                                                                          :mode :rw} ]}
                                                   :command {:shell false}}
                                        }]))
                                (clj-mesos.scheduler/decline-offer driver (:id offer))))
                            (catch Exception e
                              (.printStackTrace e)))))))))

(defn -main
  [master]
  (let [pending (atom #{0 1})
        running (atom #{})
        sched (scheduler pending running)
        driver (clj-mesos.scheduler/driver sched
                                           {:user ""
                                            :name "riak"}
                                           master)]
    (def the-magic-driver driver)
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
  (-main)
  )
