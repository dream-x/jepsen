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
            [knossos.core :as knossos]))

(defn install!
  "Installs ignite."
  [node version]
  (when-not (= (str version "-1")
    (c/su
      (info node "download ignite" version)
      (c/cd "/tmp"
            (c/exec :wget (str "http://apache-mirror.rbc.ru/pub/apache//ignite/" version "/apache-ignite-fabric-" version "-bin.zip"))
            (c/exec :unzip "apache-ignite-fabric-1.8.0-bin.zip"))
      (c/cd (str "/tmp/apache-ignite-fabric-" version "-bin")
            (c/exec (str "./bin/ignite.sh config/default-config.xml")))))))

      ; ; Replace /usr/bin/asd with a wrapper that skews time a bit
      ; (c/exec :mv   "/usr/bin/asd" "/usr/local/bin/asd")
      ; (c/exec :echo
      ;         "#!/bin/bash\nfaketime -m -f \"+$((RANDOM%100))s x1.${RANDOM}\" /usr/local/bin/asd" :> "/usr/bin/asd")
      ; (c/exec :chmod "0755" "/usr/bin/asd"))))

(defn db [version]
  "Ignite for a particular version."
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (install! version)))
    (teardown! [_ test node])))

(defn ignite-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test {:db (db "1.8.0")}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge  (cli/single-test-cmd {:test-fn ignite-test})
                    (cli/serve-cmd))
            args))