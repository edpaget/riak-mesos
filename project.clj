(defproject riak-mesos "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [liberator "0.12.0"]
                 [compojure "1.1.3"]
                 [org.clojure/tools.nrepl "0.2.4"]
                 [http-kit "2.1.18"]
                 [clj-mesos "0.20.4"]
                 [ring/ring-json "0.3.1"]
                 [cheshire "5.3.1"]
                 [org.apache.curator/curator-test "2.4.0"]
                 [org.apache.curator/curator-x-discovery "2.4.0"]]
  ;; :plugins
  ;; [[s3-wagon-private "1.1.2"]
  ;;  [theladders/lein-uberjar-deploy "1.0.0"]]
  ;; :repositories [["private" {:url "s3p://riak-mesos/releases/" :creds :gpg}]]
  ;; :main riak-mesos.core
)
