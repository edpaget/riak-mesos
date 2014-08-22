(ns riak-mesos.rest
  (:require [compojure.core :refer [routes ANY]]
            [ring.middleware.json :refer [wrap-json-response wrap-json-body]]
            [riak-mesos.state :as state]
            [riak-mesos.curator :refer [make-curator]]
            [org.httpkit.server :refer [run-server]]
            [liberator.core :refer [resource]]))

(defn body
  [ctx]
  (get-in ctx [:request :body]))

(defn riak-clusters
  [curator]
  (resource 
    :allowed-methods [:get :post]
    :available-media-types ["application/json"]
    :handle-ok (fn [_] (state/all-clusters curator))
    :post! (fn [ctx] (state/write! curator 1 (body ctx)))))

(defn riak-cluster 
  [curator id]
  (resource 
    :allowed-methods [:get :put :delete]
    :available-media-types ["application/json"]
    :handle-ok (fn [ctx] (:entry ctx))
    :put! (fn [ctx] (state/write! curator id (body ctx)))
    :delete! (fn [_] (state/write! curator id))))

(defn app-routes
  [curator]
  (routes
    (ANY "/riak_clusters" [] (riak-clusters curator))
    (ANY "/riak_clusters/:id" [id] (riak-cluster curator id))))

(defn start-server
  [zks port]
  (let [curator (make-curator zks)] 
    (run-server (app-routes curator) {:port port})))
