-- init.sql
\c train_delay_db

-- Create tables

-- GTFS RT tables
CREATE TABLE IF NOT EXISTS trips_gtfs_rt (
  id SERIAL PRIMARY KEY,
  trip_id VARCHAR,
  departure_date DATE,
  origin_departure_time TIME,
  stop_id VARCHAR,
  stop_arrival_time TIMESTAMP,
  stop_departure_time TIMESTAMP,
  stop_delay_arrival INT,
  stop_delay_departure INT,
  updated_at TIMESTAMP,
  CONSTRAINT unique_trip_stop UNIQUE (trip_id, departure_date, stop_id)
);


-- GTFS tables
CREATE TABLE IF NOT EXISTS routes_gtfs (
  route_id VARCHAR PRIMARY KEY,
  route_short_name VARCHAR,
  route_long_name VARCHAR,
  route_type INT,
  location_type INT
);

CREATE TABLE IF NOT EXISTS stops_gtfs (
  stop_id VARCHAR PRIMARY KEY,
  stop_name VARCHAR,
  stop_lat FLOAT,
  stop_lon FLOAT,
  location_type INT
);

CREATE TABLE IF NOT EXISTS stop_times_gtfs (
  trip_id VARCHAR PRIMARY KEY,
  arrival_time TIME,
  departure_time TIME,
  stop_id VARCHAR,
  stop_sequence INT,
  pickup_type INT,
  drop_off_type INT,
  CONSTRAINT fk_stop_id FOREIGN KEY (stop_id) REFERENCES stops_gtfs(stop_id)
);

CREATE TABLE IF NOT EXISTS trips_gtfs (
  trip_headline INT PRIMARY KEY,
  route_id VARCHAR,
  trip_id VARCHAR,
  direction_id INT,
  CONSTRAINT fk_route_id FOREIGN KEY (route_id) REFERENCES routes_gtfs(route_id),
  CONSTRAINT fk_trip_id FOREIGN KEY (trip_id) REFERENCES stop_times_gtfs(trip_id)
);