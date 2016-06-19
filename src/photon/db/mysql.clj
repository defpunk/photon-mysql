(ns photon.db.mysql
  (:require
    [photon.db :as db]
    [clojure.tools.logging :as log]
    [clojure.java.jdbc :as jdbc]
    )
  )

(def create-table-sql "CREATE TABLE IF NOT EXISTS event (order_id BIGINT NOT NULL,stream_name VARCHAR(100) NOT NULL,event_type VARCHAR(100),caused_by VARCHAR(100), caused_by_relation VARCHAR(100), payload  BLOB, service_id  VARCHAR(100),schema_url  VARCHAR(100), PRIMARY KEY (order_id, stream_name))")
(def drop-table-sql "DROP TABLE event")


(def insert-event-sql "INSERT INTO event (order_id BIGINT NOT NULL,stream_name VARCHAR(100) NOT NULL,event_type VARCHAR(100),caused_by VARCHAR(100), caused_by_relation VARCHAR(100), payload  BLOB, service_id  VARCHAR(100),schema_url  VARCHAR(100), PRIMARY KEY (order_id, stream_name))")



(defn conf->db-spec [conf]
  {:classname   "com.mysql.cj.jdbc.Driver"
   :subprotocol "mysql"
   :subname     (:mysql.connection-url conf)
   :user        (:mysql.user conf)
   :password    (:mysql.password conf)
   }
  )

(defn save-event [db-spec event]
  (jdbc/execute! db-spec [])
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

(defn find-event [dbspec stream-name id]
  nil
  )

(defn find-event-by-id [dbspec id]

  )

(defn delete-event [dbspec stream-name id]

  )

(defn delete-all-events [dbspec stream-name id]

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
  (distinct-values [this k] (println "finding distinct values for " k))
  (store [this payload]
    (log/trace "Payload" payload)
    (let [event-time (:event-time payload)
          new-payload (assoc (into {} payload) :event-time
                                               (if (nil? event-time)
                                                 (System/currentTimeMillis)
                                                 (long event-time)))]

      )
    )

  (lazy-events [this stream-name date] "no events yet")
  (lazy-events-page [this stream-name date page] "need to find all the events next")
  )

;(db/store [this payload]
;          (log/trace "Payload" payload)
;          (let [event-time (:event-time payload)
;                new-payload (assoc (into {} payload) :event-time
;                                                     (if (nil? event-time)
;                                                       (System/currentTimeMillis)
;                                                       (long event-time)))]
;            (with-open [w (clojure.java.io/writer (file-name conf) :append true)]
;              (.write w (str (json/generate-string new-payload) "\n")))))

;(lazy-events-page [this stream-name date page]
;                  (m/with-mongo (mongo conf)
;                                (let [l-date (if (string? date) (read-string date) date)
;                                      res (m/fetch collection :where {:stream-name stream-name}
;                                                   :skip (* page-size page) :limit page-size)]
;                                  (log/info "Calling mongo: " :where {:stream-name stream-name}
;                                            :skip (* page-size page) :limit page-size)
;                                  (if (< (count res) 1)
;                                    []
;                                    (concat res
;                                            (lazy-seq (db/lazy-events-page this stream-name l-date (inc page))))))))