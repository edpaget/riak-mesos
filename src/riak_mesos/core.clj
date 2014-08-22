(ns riak-mesos.core
  (:require [clj-mesos.scheduler :as sch]))

(def riak-scheduler (sch/scheduler
                     (resourceOffers [driver offers]
                                      (let [[offer] offers]
                                        (sch/launch-tasks driver (:offer-id offer)
                                                         [{:name "riak-task"
                                                           :task-id "riak-01"
                                                           :slave-id (:slave-id offer)
                                                           :resources {:cpus 1.0
                                                                       :mem 100.0}
                                                           :command {:value "sleep 10"}}])))))

(defn -main
  [& master]
  #_(sch/run-scheduler master))
