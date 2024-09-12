"""
GTFS Data queries for the GTFS data ingestion DAG
- Used for the ingest_gtfs_data_to_database function (gtfs_data_ingestion_functions.py)
"""

# Queries (Depending on the file name)
queries = {
    'calendar_dates': (
        """
        INSERT INTO calendar_dates (service_id, date, exception_type)
        VALUES (%s, %s, %s)
        ON CONFLICT (service_id, date)
        DO UPDATE SET
        exception_type = EXCLUDED.exception_type
        """,
        (
            "service_id",
            "date",
            "exception_type"
        )
    ),
    'routes': (
        """
        INSERT INTO routes (route_id, route_short_name, route_long_name, route_type)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (route_id)
        DO UPDATE SET
        route_short_name = EXCLUDED.route_short_name,
        route_long_name = EXCLUDED.route_long_name,
        route_type = EXCLUDED.route_type
        """,
        (
            "route_id",
            "route_short_name",
            "route_long_name",
            "route_type"
        )
    ),
    'stops': (
        """
        INSERT INTO stops (stop_id, stop_name, stop_lat, stop_lon, location_type, parent_station)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (stop_id)
        DO UPDATE SET
        stop_name = EXCLUDED.stop_name,
        stop_lat = EXCLUDED.stop_lat,
        stop_lon = EXCLUDED.stop_lon,
        location_type = EXCLUDED.location_type,
        parent_station = EXCLUDED.parent_station
        """,
        (
            "stop_id",
            "stop_name",
            "stop_lat",
            "stop_lon",
            "location_type",
            "parent_station"
        )
    ),
    'stop_times': (
        """
        INSERT INTO stop_times (trip_id, arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (trip_id, stop_sequence)
        DO UPDATE SET
        arrival_time = EXCLUDED.arrival_time,
        departure_time = EXCLUDED.departure_time,
        stop_id = EXCLUDED.stop_id,
        pickup_type = EXCLUDED.pickup_type,
        drop_off_type = EXCLUDED.drop_off_type
        """,
        (
            "trip_id",
            "arrival_time",
            "departure_time",
            "stop_id",
            "stop_sequence",
            "pickup_type",
            "drop_off_type"
        )
    ),
    'trips': (
        """
        INSERT INTO trips (route_id, service_id, trip_id, trip_headsign, direction_id, block_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (trip_id)
        DO UPDATE SET
        route_id = EXCLUDED.route_id,
        service_id = EXCLUDED.service_id,
        trip_headsign = EXCLUDED.trip_headsign,
        direction_id = EXCLUDED.direction_id,
        block_id = EXCLUDED.block_id
        """,
        (
            "route_id",
            "service_id",
            "trip_id",
            "trip_headsign",
            "direction_id",
            "block_id"
        )
    )
}