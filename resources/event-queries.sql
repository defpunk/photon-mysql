-- name: setup-table
-- Sets up the event table for storage.
CREATE TABLE IF NOT EXISTS event (order_id BIGINT NOT NULL,stream_name VARCHAR(100) NOT NULL,event_type VARCHAR(100),caused_by VARCHAR(100), caused_by_relation VARCHAR(100), payload            BLOB, service_id  VARCHAR(100),schema_url  VARCHAR(100));

-- name: find-event
-- Finds an event by stream and id
SELECT select *
FROM event
WHERE stream_name = :stream_name
AND order_id = :order_id


-- name: delete-event
-- deletes events with teh specified id
DELETE
FROM event
WHERE order_id = :order_id

-- name: delete-all-events
-- deletes all events
TRUNCATE event

-- name: find-event-by-id
-- Finds an event by id
SELECT select *
FROM event
AND order_id = :order_id


