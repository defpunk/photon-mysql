(ns photon.db.mysql-test
  (:require
    [clojure.test :refer :all]
    [photon.db.mysql
             :refer
             :all] )
  )


(deftest can-create-config
  (testing "can create dbspec from conf"
           (is (= {:classname   "com.mysql.cj.jdbc.Driver"
                   :subprotocol "mysql"
                   :subname     "testhost:3306/db"
                   :user        "user"
                   :password "password"}

                  (conf->db-spec {:mysql.connection-url "testhost:3306/db"
                                  :mysql.user           "user"
                                  :mysql.password "password"})))
           )
  )

;The integration tests assume that a debase named eventstore
; is available on localhost:3306
; username photon - pwd - ph0t0n

(def my-sql-host "192.168.99.100")

(deftest db-functions-integration-test
  (let [db-spec  {:classname   "com.mysql.cj.jdbc.Driver"
                  :subprotocol "mysql"
                  :subname     (str "//" my-sql-host "/eventstore")
                  :user        "photon"
                  :password "ph0t0n" } ]

    (testing "can create schema" (setup-tables db-spec))

    (testing "should not be able to find an event thats yet to exist"
      (= nil (find-event db-spec "test-stream" 1)))

    (testing "create and look up an event"
      ()
      (= nil (find-event db-spec "test-stream" 1)))

    (testing "shoudld be able to create an event")

    ;(testing "can clean up"  (drop-tables db-spec))
    )
   )