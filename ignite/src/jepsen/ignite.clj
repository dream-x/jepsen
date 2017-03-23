(ns jepsen.ignite
  (:gen-class)
  (:require [jepsen.cli :as cli]
            [jepsen.tests :as tests]
            [clojure [pprint :refer :all]
             [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
                    [db        :as db]
                    [util      :as util :refer [meh timeout]]
                    [control   :as c :refer [|]]
                    [client    :as client]
                    [checker   :as checker]
                    [model     :as model]
                    [generator :as gen]
                    [nemesis   :as nemesis]
                    [store     :as store]
                    [report    :as report]
                    [tests     :as tests]]
            [jepsen.control [net :as net]
                            [util :as net/util]]
            [jepsen.os.debian :as debian]
            [knossos.core :as knossos])
  (:import (org.apache.ignite Ignition
                              Ignite
                              IgniteCluster)
            (org.apache.ignite.cluster ClusterGroup)
            (org.apache.ignite.lang IgniteRunnable)
            (org.apache.ignite.configuration CacheConfiguration)
            (org.apache.ignite.cache CachePeekMode)
            (org.apache.ignite.configuration IgniteConfiguration)
            (java.lang Thread)))

(defn install!
  "Installs ignite."
  [node version]
  (when-not (= (str version "-1")
    (c/su
      (info node "download ignite" version)
      (c/cd "/tmp"
            (c/exec :wget (str "http://apache-mirror.rbc.ru/pub/apache//ignite/" version "/apache-ignite-fabric-" version "-bin.zip"))
            (c/exec :unzip (str "apache-ignite-fabric-" version "-bin.zip"))
      create startup script
      (c/exec :echo (str 
              "#!/bin/bash\nfaketime -m -f \"+$((RANDOM%100))s x1.${RANDOM}\" /tmp/apache-ignite-fabric-" version "-bin/bin/ignite.sh config/default-config.xml") :> "/usr/bin/ignited")
      (c/exec :chmod "0755" "/usr/bin/ignited")
      (c/exec :ignited)))))
)


(defn db [version]
  "Ignite for a particular version."
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (install! version)))
    (teardown! [_ test node])))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn start-client
  "Start client node."
  [node]
  (Ignition/setClientMode true)
  (let [gridname (str "Client" (clojure.string/replace node ":" ""))
        cfg (new IgniteConfiguration)]
        (.setGridName cfg gridname)
        (.setPeerClassLoadingEnabled cfg true)
        (Ignition/start cfg)))

(defn ignite-cas! [cache key value new-value]
  (let [existing-value (:value (.get cache key))]
    ; (println "++++++++")
    ; (println existing-value)
    ; (println value)
    ; (println "++++++++")
    (if (= existing-value value)
        (.putIfAbsent cache key new-value)
        false)))

(defrecord CasRegisterClient [ignite cache namecache key]
  client/Client
  (setup! [this test node]
    (let [ignite (start-client node)
          cache (.getOrCreateCache ignite (new CacheConfiguration namecache))]
      (assoc this :cache cache, :ignite ignite)))

  (invoke! [this test op]
      (case (:f op)
        :read (assoc op
                     :type :ok,
                     :value (.get cache key))

        :cas   (let [[value value'] (:value op)
                     ok?            (ignite-cas! cache
                                               key
                                               value
                                               value')]
               (assoc op :type (if ok? :ok :fail)))

        :write (do (.put cache key (:value op))
                   (assoc op :type :ok))))

  (teardown! [this test]
    (.close ignite)))

(defn cas-register-client
  "A basic CAS register on top of a single key."
  []
  (CasRegisterClient. nil nil "jepsen" "mew"))

(defn ignite-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test 
          { :name    "ignite"
            :os      debian/os
            :db      (db "1.9.0")
            :client  (cas-register-client)
            :nemesis (nemesis/partition-random-halves)
            :generator (->> (gen/mix [r w cas])
                            (gen/stagger 1/10)
                            (gen/nemesis
                              (gen/seq (cycle [(gen/sleep 5)
                                               {:type :info, :f :start}
                                               (gen/sleep 5)
                                               {:type :info, :f :stop}])))
                            (gen/time-limit 60))
            :model   (model/cas-register 0)
            :checker (checker/compose
                    {:perf   (checker/perf)
                     :linear checker/linearizable})}
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge  (cli/single-test-cmd {:test-fn ignite-test})
                    (cli/serve-cmd))
            args))
