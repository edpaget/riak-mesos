(ns riak-mesos.curator
  (:import [org.apache.curator.framework.recipes.cache NodeCache NodeCacheListener]
           [org.apache.curator.framework CuratorFrameworkFactory]
           [org.apache.curator.retry RetryNTimes]
           [org.apache.curator.framework CuratorFramework]
           [org.apache.curator.framework.api.SetDataBuilder]
           org.apache.zookeeper.KeeperException$NodeExistsException
           org.apache.zookeeper.data.Stat))

(defn close
  [^java.io.Closeable c]
  (.close c))

(defn make-curator
  "Simple curator creation fn"
  [zkservers]
  (doto (CuratorFrameworkFactory/newClient zkservers 10000 60000 (RetryNTimes. 10 1000))
    (.start)))

(defn write-path
  ([^CuratorFramework curator path data]
   (.. (.newNamespaceAwareEnsurePath curator path)
       (ensure (.getZookeeperClient curator)))
   (try
     (.. curator
         (create)
         (forPath path data))
     (catch KeeperException$NodeExistsException e
       ;;TODO: this should probably be a probe
       (.. curator
           (setData)
           (forPath path data)))))
  ([^CuratorFramework curator path data version]
   (.. curator
       (setData)
       (withVersion version)
       (forPath path data))))

(defn read-path
  ([^CuratorFramework curator path]
   (let [stat (Stat.)]
     {:data (.. curator
                (getData)
                (storingStatIn)
                (forPath path))
      :stat (bean stat)})))

(defprotocol IListenable
  (listen [this f] "Invokes f on every event. Returns a no-arg function that will unregister it when invoked.")
  )

(defn node-cache
  [curator path]
  (let [cache (NodeCache. curator path)]
    (.start cache)
    (reify
      clojure.lang.IFn
      (invoke [_]
        (.close cache))
      clojure.lang.IDeref
      (deref [_]
        (.getCurrentData cache))
      IListenable
      (listen [_ f]
        (let [listener (reify NodeCacheListener
                            (nodeChanged [_]
                              (f)))
              listenable (.getListenable cache)]
          (.addListener listenable listener)
          (fn []
            (.removeListener listenable listener)))))))

(comment
  (def cache (node-cache (:curator disco.core-test/services) "/test/path"))

  (write-path  (:curator disco.core-test/services)
              "/test/path"
              (byte-array 1 [(byte 25)]))

  (def listener (listen cache (fn [] (println "value changed to" @cache))))

 (bean (:stat (bean (deref cache))))
  )
