(defproject riak-mesos "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [liberator "0.12.0"] 
                 [compojure "1.1.3"]
                 [http-kit "2.1.18"]
                 [clj-mesos "0.20.0"]
                 [org.apache.curator/curator-test "2.4.0"]
                 [org.apache.curator/curator-x-discovery "2.4.0"]])
