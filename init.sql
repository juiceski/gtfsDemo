-- Core GTFS Tables
CREATE TABLE IF NOT EXISTS routes (
    route_id TEXT PRIMARY KEY,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type INTEGER
);

CREATE TABLE IF NOT EXISTS shapes (
    shape_id TEXT,
    shape_pt_lat FLOAT,
    shape_pt_lon FLOAT,
    shape_pt_sequence INTEGER,
    shape_dist_traveled FLOAT,
    PRIMARY KEY (shape_id, shape_pt_sequence)
);

CREATE TABLE IF NOT EXISTS stops (
    stop_id TEXT PRIMARY KEY,
    stop_name TEXT,
    stop_lat FLOAT,
    stop_lon FLOAT
);

CREATE TABLE IF NOT EXISTS trips (
    trip_id TEXT PRIMARY KEY,
    route_id TEXT REFERENCES routes(route_id),
    service_id TEXT,
    trip_headsign TEXT,
    trip_short_name TEXT,
    direction_id INTEGER,
    shape_id TEXT,
    wheelchair_accessible INTEGER,
    bikes_allowed INTEGER
);

CREATE TABLE IF NOT EXISTS stop_times (
    trip_id TEXT REFERENCES trips(trip_id),
    arrival_time TEXT,
    departure_time TEXT,
    stop_id TEXT REFERENCES stops(stop_id),
    stop_sequence INTEGER,
    pickup_type INTEGER,
    drop_off_type INTEGER
);

-- Indexes for Joins/Filtering
CREATE INDEX IF NOT EXISTS idx_trip_id ON stop_times(trip_id);
CREATE INDEX IF NOT EXISTS idx_stop_id ON stop_times(stop_id);
CREATE INDEX IF NOT EXISTS idx_route_id ON trips(route_id);

-- Materialized Views for Visualizations
-- Stop Usage Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS stop_usage_summary AS
SELECT
    stop_id,
    COUNT(DISTINCT trip_id) AS trip_count
FROM stop_times
GROUP BY stop_id;

-- Transfer Hotspot Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS stop_transfer_summary AS
SELECT
    st.stop_id,
    COUNT(DISTINCT t.route_id) AS route_count,
    ARRAY_AGG(DISTINCT t.route_id) AS routes
FROM stop_times st
JOIN trips t ON st.trip_id = t.trip_id
GROUP BY st.stop_id;

-- Indexes for Materialized Views
CREATE INDEX IF NOT EXISTS idx_transfer_hotspots ON stop_transfer_summary(route_count);
CREATE INDEX IF NOT EXISTS idx_usage_summary ON stop_usage_summary(trip_count);
