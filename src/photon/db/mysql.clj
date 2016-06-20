(ns photon.db.mysql
  (:require
    [photon.db :as db]
    [clojure.tools.logging :as log]
    [clojure.java.jdbc :as jdbc]
    )
  )

(def page-size 500)

(def create-table-sql "CREATE TABLE IF NOT EXISTS event (order_id BIGINT NOT NULL,stream_name VARCHAR(100) NOT NULL,event_type VARCHAR(100),caused_by VARCHAR(100), caused_by_relation VARCHAR(100), payload JSON, service_id  VARCHAR(100),schema_url  VARCHAR(100), event_time BIGINT, PRIMARY KEY (order_id, stream_name))")
(def drop-table-sql "DROP TABLE event")

(def trunc-event-sql "TRUNCATE TABLE event")

(def insert-event-sql "INSERT INTO event (order_id,stream_name ,event_type,
caused_by , caused_by_relation, payload, service_id ,schema_url, event_time  ) VALUES (?,?,?,?,?,?,?,?,?)")

(def find-event-by-id-sql  "SELECT order_id, stream_name, event_type, caused_by, caused_by_relation, payload, service_id, schema_url, event_time FROM event where order_id = ?")
(def find-event-by-id-and-stream-sql  "SELECT order_id, stream_name, event_type, caused_by, caused_by_relation, payload, service_id, schema_url , event_time FROM event where order_id = ? and stream_name = ?")

(def delete-event-by-id-sql "DELETE FROM event where order_id = ?")

(def select-all-stream-names-sql "SELECT DISTINCT stream_name from event")

(def get-paged-events-for-stream-sql "SELECT order_id, stream_name, event_type, caused_by, caused_by_relation, payload, service_id, schema_url , event_time FROM event where  stream_name = ? AND order_id >= ? LIMIT ? OFFSET ? ")
(def get-paged-events-sql "SELECT order_id, stream_name, event_type, caused_by, caused_by_relation, payload, service_id, schema_url , event_time FROM event where order_id >= ? LIMIT ? OFFSET ? ")

(defn conf->db-spec [conf]
  {:classname   "com.mysql.cj.jdbc.Driver"
   :subprotocol "mysql"
   :subname     (:mysql.connection-url conf)
   :user        (:mysql.user conf)
   :password    (:mysql.password conf)
   }
  )

(defn save-event [db-spec event]
  (jdbc/execute! db-spec [insert-event-sql
                          (:order-id event)
                          (:stream-name event)
                          (:event-type event)
                          (:caused-by event)
                          (:caused-by-relation event)
                          (:payload event)
                          (:service-id event)
                          (:schema-url event)
                          (:event-time event)
                          ])
  )

(defn setup-tables [db-spec]
  (jdbc/execute! db-spec [create-table-sql])
  )

(defn drop-tables [db-spec]
  (jdbc/execute! db-spec [drop-table-sql])
  )

(defn build-my-sql [conf]
  (let [db-spec (conf->db-spec conf)]
    (setup-tables db-spec)
    db-spec
    )
  )

(def setup-my-sql (memoize build-my-sql))

(defn db-event->map [e]
  {:order-id (:order_id e)
   :stream-name (:stream_name e)
   :event-type (:event_type e)
   :caused-by (:caused_by e)
   :caused-by-relation (:caused_by_relation e)
   :payload (:payload e)
   :service-id (:service_id e)
   :event-time (:event_time e)
   :schema-url (:schema_url e)}
  )

(defn find-event-in-any-stream [db-spec id]
  (let [event-result
        (jdbc/query db-spec [find-event-by-id-sql id])]

    (if (first event-result)
      (db-event->map (first event-result))
      nil
      )
    )
  )

(defn find-event-in-stream [db-spec stream-name id]
  (let [event-result
        (jdbc/query db-spec [find-event-by-id-and-stream-sql id stream-name])]

    (if (first event-result)
        (db-event->map (first event-result))
        nil
      )
    )
  )

(defn find-event [db-spec stream-name id]
  (if (or (= :__all__ stream-name)
          (= "__all__" stream-name))
      (find-event-in-any-stream db-spec id)
      (find-event-in-stream db-spec stream-name id)
    )
  )

(defn find-event-by-id [db-spec id]
  (map db-event->map (jdbc/query db-spec [find-event-by-id-sql id]))
  )

(defn delete-event [db-spec id]
  (jdbc/execute! db-spec [delete-event-by-id-sql id])
  )

(defn event-count [db-spec]
  (:ec (first (jdbc/query db-spec ["SELECT COUNT(*) as ec from event"])) ))


(defn delete-all-events [db-spec]
  (jdbc/execute! db-spec [trunc-event-sql])
  )

(defn all-stream-names [db-spec]
  (let [result
        (jdbc/query db-spec [select-all-stream-names-sql])]
     (map #({:stream-name (get % :stream_name)})  result)
    )
  )

(defn get-events [db-spec stream-name starting-order-id page page-size]
  (let [offset (* page-size page)]

    (if (or (= :__all__ stream-name)
            (= "__all__" stream-name))
      (jdbc/query db-spec [get-paged-events-sql starting-order-id page-size offset])
      (jdbc/query db-spec [get-paged-events-for-stream-sql stream-name starting-order-id page-size offset])

      ))
  )



(defrecord MySQLDB [conf]
  db/DB
  (driver-name [this] "mysql")
  (fetch [this stream-name id]
    (let [db-spec (setup-my-sql conf)] (find-event db-spec stream-name id))
    )
  (delete! [this id]
    (let [db-spec (setup-my-sql conf)] (delete-event db-spec id))
    )
  (delete-all! [this]
    (let [db-spec (setup-my-sql conf)] (delete-all-events db-spec))
    )
  (search [this id] (let [db-spec (setup-my-sql conf)] (find-event-by-id db-spec id)))

  (store [this payload]
    (log/trace "Payload" payload)
    (let [event-time (:event-time payload)
          new-payload (assoc (into {} payload) :event-time
                                               (if (nil? event-time)
                                                 (System/currentTimeMillis)
                                                 (long event-time)))]

      (let [db-spec (setup-my-sql conf)]
        (save-event db-spec new-payload)
        )
      )
    )


  (distinct-values [this k]
    (into #{}
          (map #(get % k)
               (let [db-spec (setup-my-sql conf)]
                 (all-stream-names db-spec)
                 )
               ))

    )

  (lazy-events [this stream-name date]
    (map db-event->map (db/lazy-events-page this stream-name date 0)))


  (lazy-events-page [this stream-name date page]
    (let [oid (if (nil? date) 0 (* 1000 date))
          results (let [db-spec (setup-my-sql conf)] (get-events db-spec stream-name oid page page-size))
          ]
      (when-let [r (seq results)]
        (lazy-cat r
                  (db/lazy-events-page this stream-name date
                                       (+ page page-size)))))

    )
  )