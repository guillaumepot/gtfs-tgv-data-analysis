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
CREATE TABLE IF NOT EXISTS calendar_dates (
  id SERIAL PRIMARY KEY,
  service_id INT,
  date DATE,
  exception_type INT,
  CONSTRAINT unique_service_date UNIQUE (service_id, date)
);


CREATE TABLE IF NOT EXISTS routes (
  route_id VARCHAR PRIMARY KEY,
  route_short_name VARCHAR,
  route_long_name VARCHAR,
  route_type INT
);


CREATE TABLE IF NOT EXISTS stops (
  stop_id VARCHAR PRIMARY KEY,
  stop_name VARCHAR,
  stop_lat FLOAT,
  stop_lon FLOAT,
  location_type INT,
  parent_station VARCHAR
);


CREATE TABLE IF NOT EXISTS stop_times (
  trip_id VARCHAR,
  arrival_time TIME,
  departure_time TIME,
  stop_id VARCHAR,
  stop_sequence INT,
  pickup_type INT,
  drop_off_type INT,
  PRIMARY KEY (trip_id, stop_sequence)
);


CREATE TABLE IF NOT EXISTS trips (
  route_id VARCHAR,
  service_id INT,
  trip_id VARCHAR PRIMARY KEY,
  trip_headsign INT,
  direction_id INT,
  block_id VARCHAR
);