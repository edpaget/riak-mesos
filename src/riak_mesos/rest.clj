(ns riak-mesos.rest
  (:require [compojure.core :refer [routes ANY]]
            [clojure.set :refer [difference union]]
            [ring.middleware.json :refer [wrap-json-response wrap-json-body]]
            [riak-mesos.curator :refer [make-curator]]
            [org.httpkit.server :refer [run-server]]
            [liberator.core :refer [resource]]
            [clojure.java.io :as io]))

(defn nodes
  [ctx]
  (get-in ctx [:request :body "nodes"]))

(comment (defn riak-clusters
           [curator]
           (resource 
             :allowed-methods [:get :post]
             :available-media-types ["application/json"]
             :handle-ok (fn [_] (state/all-clusters curator))
             :handle-created (fn [ctx] (:entry ctx))
             :post! (fn [ctx] (state/write! curator 1 (body ctx) ctx)))))

(defn riak-cluster 
  [pending running]
  (resource 
    :allowed-methods [:get :post]
    :allowed? {:post (fn [ctx] (> (nodes ctx) (count @running)))}
    :available-media-types ["application/json" "text/html"]
    :handle-ok (fn [ctx] (condp = (get-in ctx [:representation :media-type]) 
                           "application/json" {:nodes (count @running)}
                           "text/html" (slurp (io/resource "index.html"))))
    :post! (fn [ctx] 
             (let [pending-nodes (difference (set (range (nodes ctx))) 
                                             (union @running @pending))]
               (swap! pending union pending-nodes)))))

(defn app-routes
  [pending running]
  (-> (routes
        (ANY "/riak_cluster" [] (riak-cluster pending running))
        ;;(ANY "/riak_clusters/:id" [id] (riak-cluster curator id))
        )
      wrap-json-body))

(defn start-server
  [pending running port]
  (run-server (app-routes pending running) {:port port}))
