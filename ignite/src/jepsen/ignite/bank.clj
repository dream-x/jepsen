; (ns jepsen.ignite.bank
;   "Simulates transfers between bank accounts"
;   (:refer-clojure :exclude [test])
;   (:require [jepsen [cockroach :as cockroach]
;                     [client :as client]
;                     [checker :as checker]
;                     [generator :as gen]
;                     [independent :as independent]
;                     [reconnect :as rc]
;                     [util :as util :refer [meh]]]
;             [jepsen.checker.timeline :as timeline]
;             [jepsen.cockroach [client :as c]
;                               [nemesis :as cln]]
;             [clojure.core.reducers :as r]
;             [clojure.java.jdbc :as j]
;             [clojure.tools.logging :refer :all]
;             [knossos.model :as model]
;             [knossos.op :as op]))

; (defrecord BankClient [cache-created? n starting-balance conn]
;   client/Client
;   (setup! [this test node]
;     (let [conn (c/client node)]
;       (locking tbl-created?
;         (when (compare-and-set! tbl-created? false true)
;           (rc/with-conn [c conn]
;             (Thread/sleep 1000)
;             (c/with-txn-retry
;               (j/execute! c ["drop table if exists accounts"]))
;             (Thread/sleep 1000)
;             (info "Creating table")
;             (c/with-txn-retry
;               (j/execute! c ["create table accounts
;                              (id      int not null primary key,
;                              balance bigint not null)"]))
;             (dotimes [i n]
;               (Thread/sleep 500)
;               (info "Creating account" i)
;               (c/with-txn-retry
;                 (c/insert! c :accounts {:id i :balance starting-balance}))))))

;       (assoc this :conn conn)))

;   (invoke! [this test op]
;     (c/with-exception->op op
;       (rc/with-conn [c conn]
;         (c/with-timeout
;           (c/with-txn-retry
;             (c/with-txn [c c]
;               (case (:f op)
;                 :read (->> (c/query c ["select balance from accounts"])
;                            (mapv :balance)
;                            (assoc op :type :ok, :value))

;                 :transfer
;                 (let [{:keys [from to amount]} (:value op)
;                       b1 (-> c
;                              (c/query
;                                ["select balance from accounts where id = ?"
;                                 from] {:row-fn :balance})
;                              first
;                              (- amount))
;                       b2 (-> c
;                              (c/query
;                                ["select balance from accounts where id = ?" to]
;                                {:row-fn :balance})
;                              first
;                              (+ amount))]
;                   (cond
;                     (neg? b1)
;                     (assoc op :type :fail, :error [:negative from b1])

;                     (neg? b2)
;                     (assoc op :type :fail, :error [:negative to b2])

;                     true
;                     (do (c/update! c :accounts {:balance b1} ["id = ?" from])
;                         (c/update! c :accounts {:balance b2} ["id = ?" to])
;                         (cockroach/update-keyrange! test :accounts from)
;                         (cockroach/update-keyrange! test :accounts to)
;                         (assoc op :type :ok)))))))))))

;   (teardown! [this test]
;     (try
;       (c/with-timeout
;         (rc/with-conn [c conn]
;           (j/execute! c ["drop table if exists accounts"])))
;       (finally
;         (rc/close! conn)))))