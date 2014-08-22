(ns riak-mesos.state
  (:require [riak-mesos.curator]
            [cheshire.core :refer [parse-string generate-string]])
  (:import org.apache.zookeeper.KeeperException$NoNodeException))

(defn data->clj
  [data]
  (parse-string (apply str (map #(char (bit-and % 255)) data))
                true))

(defn clj->data
  [clj-map]
  (.getBytes (generate-string clj-map)))

(defn validate-data
  [json]
  (integer? (:nodes json)))

(defn path
  [id]
  (if id
    (str "/riak-mesos/" id)
    "/riak-mesos"))

(defn read-zk
  [curator & [id]]
  (try (->> (path id)
            (riak-mesos.curator/read-path curator)
            :data
            data->clj)
       (catch KeeperException$NoNodeException e nil)))

(defn all-clusters
  [curator]
  (read-zk curator))

(defn exists?
  [curator id ctx]
  (when-let [data (read-zk curator id)]
    (assoc ctx :entry data)))

(defn existed?
  [curator id]
  (nil? (read-zk curator id)))

(defn write!
  [curator id data ctx]
  (let [data (when data (assoc data :id id))] 
    (try (riak-mesos.curator/write-path curator 
                                        (path id) 
                                        (clj->data data))
         (catch Exception e (println e)))
    (assoc ctx :entry data)))

(defn delete!
  [curator id]
  (write! curator id nil {})) 
