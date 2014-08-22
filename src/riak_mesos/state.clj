(ns riak-mesos.state
  (:require [riak-mesos.curator]
            [cheshire.core :refer [parse-string]]))

(defn data->clj
  [data]
  (parse-string (.getString data)))

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
  (->> (path id)
       (riak-mesos.curator/read-data curator)
       data->clj))

(defn all-clusters
  [curator]
  (read-zk curator))

(defn exists?
  [curator id ctx]
  (if-let [data (read-zk curator id)]
    (assoc ctx :entry data)))

(defn existed?
  [ctx]
  (nil? (get-in ctx [:entry :sentital])))

(defn write!
  [curator id data]
  (riak-mesos.curator/write-data curator 
                                 (path id) 
                                 (clj-map data)))

(defn delete!
  [curator id]
  (write! curator id {:sentinel nil})) 
