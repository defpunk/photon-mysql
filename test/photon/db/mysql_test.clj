(ns photon.db.mysql-test
  (:require
    [clojure.test :refer :all]
    [photon.db.mysql
     :refer
     :all])
  )

(deftest can-create-config
  (testing "can create dbspec from conf"
    (is (= {:classname   "com.mysql.cj.jdbc.Driver"
            :subprotocol "mysql"
            :subname     "testhost:3306/db"
            :user        "user"
            :password    "password"}

           (conf->db-spec {:mysql.connection-url "testhost:3306/db"
                           :mysql.user           "user"
                           :mysql.password       "password"})))
    )
  )

;The integration tests assume that a debase named eventstore
; is available on localhost:3306
; username photon - pwd - ph0t0n

(def my-sql-host "192.168.99.100")

(deftest db-functions-integration-test
  (let [db-spec {:classname   "com.mysql.cj.jdbc.Driver"
                 :subprotocol "mysql"
                 :subname     (str "//" my-sql-host "/eventstore")
                 :user        "photon"
                 :password    "ph0t0n"}]

    (testing "clean up before testing" (drop-tables db-spec))
    (testing "can create schema" (setup-tables db-spec))

    (testing "should not be able to find an event thats yet to exist"
      (= nil (find-event db-spec "test-stream" 1)))

    (testing "create and look up an event"
      (save-event db-spec {:order-id    1
                           :stream-name "test-stream"
                           :event-type  "test"
                           :caused-by   "test-suite"
                           :payload     "{}"
                           :service-id  "testing"
                           :event-time  100})
      (is (= false (nil? (find-event db-spec "test-stream" 1))))
      (is (= false (nil? (find-event-by-id db-spec 1))))

      )

    (testing "saved event is mapped back correctly"
      (let [event (find-event db-spec "test-stream" 1)]
        (is (map? event))
        (is (= {:order-id           1
                :stream-name        "test-stream"
                :event-type         "test"
                :caused-by          "test-suite"
                :caused-by-relation nil
                :payload            "{}"
                :schema-url         nil
                :event-time         100
                :service-id         "testing"} event))

        (is (= event (find-event db-spec "__all__" 1)))
        (is (= event (find-event db-spec :__all__ 1)))
        )
      )


    (testing "look up and search should return same result when only one order id exists"
      (is (= 1 (count (find-event-by-id db-spec 1))))
      (is (= (find-event db-spec "test-stream" 1) (first (find-event-by-id db-spec 1)))
          )
      )

    (testing "can delete an event"
      (delete-event db-spec 1)
      (is (empty? (find-event db-spec "test-stream" 1)))
      )

    (testing "can delete all events"
      ;create alot of events
      (dotimes [n 50] (save-event db-spec {:order-id    n
                                           :stream-name "test-stream"
                                           :event-type  "test"
                                           :caused_by   "test-suite"
                                           :payload     "{}"
                                           :service-id  "testing"}))

      (is (= 50 (event-count db-spec)))                     ;check events created
      (delete-all-events db-spec)
      (is (= 0 (event-count db-spec)))                      ; check no events left

      )

    (testing "can page events"
      ;create alot of events
      (dotimes [n 50] (save-event db-spec {:order-id    (+ 1 n)
                                           :stream-name "test-stream"
                                           :event-type  "test"
                                           :caused_by   "test-suite"
                                           :payload     "{}"
                                           :service-id  "testing"}))

      (is (= 10 (count (get-events db-spec "test-stream" 1 1 10))))
      (is (= (range 1 11) (map #(:order_id %) (get-events db-spec "test-stream" 1 0 10))))
      (is (= (range 11 21) (map #(:order_id %) (get-events db-spec "test-stream" 1 1 10))))
      (is (= (range 26 51) (map #(:order_id %) (get-events db-spec "test-stream" 1 1 25))))

      )

    (testing "can page events across streams"
      (delete-all-events db-spec)
      ;create alot of events
      (dotimes [n 50]
        (save-event db-spec {:order-id    (+ 1 (* 2 n))
                             :stream-name "stream-two"
                             :event-type  "test"
                             :caused_by   "test-suite"
                             :payload     "{}"
                             :service-id  "testing"})

        (save-event db-spec {:order-id    (+ 2 (* 2 n))
                             :stream-name "stream-one"
                             :event-type  "test"
                             :caused_by   "test-suite"
                             :payload     "{}"
                             :service-id  "testing"})

        )

      (is (= 10 (count (get-events db-spec "stream-two" 1 1 10))))
      (is (= (filter odd? (range 1 21)) (map #(:order_id %) (get-events db-spec "stream-two" 1 0 10))))
      (is (= (filter even? (range 1 21)) (map #(:order_id %) (get-events db-spec "stream-one" 1 0 10))))
      (is (= (range 1 11) (map #(:order_id %) (get-events db-spec "__all__" 1 0 10))))
      (is (= (range 1 11) (map #(:order_id %) (get-events db-spec :__all__ 1 0 10))))
      )

    ;(testing "can clean up"  (drop-tables db-spec))
    )
  )