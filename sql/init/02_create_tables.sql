-- Transportation Zones (NYC boroughs and neighborhoods)
CREATE TABLE zones (
    zone_id SERIAL PRIMARY KEY,
    zone_name VARCHAR(100) NOT NULL,
    borough VARCHAR(50),
    zone_type VARCHAR(50),
    geometry GEOMETRY(MULTIPOLYGON, 4326),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trips table (unified for all transportation modes)
CREATE TABLE trips (
    trip_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    external_trip_id VARCHAR(255),
    mode transportation_mode NOT NULL,
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance DECIMAL(8,2),
    pickup_location GEOMETRY(POINT, 4326),
    dropoff_location GEOMETRY(POINT, 4326),
    pickup_zone_id INTEGER REFERENCES zones(zone_id),
    dropoff_zone_id INTEGER REFERENCES zones(zone_id),
    fare_amount DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    payment_type VARCHAR(50),
    trip_status trip_status DEFAULT 'completed',
    duration_minutes INTEGER GENERATED ALWAYS AS (
        EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime))/60
    ) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Transit Stations (subway, bus stops, bike stations)
CREATE TABLE stations (
    station_id SERIAL PRIMARY KEY,
    external_id VARCHAR(255),
    station_name VARCHAR(255) NOT NULL,
    station_type transportation_mode NOT NULL,
    location GEOMETRY(POINT, 4326) NOT NULL,
    zone_id INTEGER REFERENCES zones(zone_id),
    capacity INTEGER,
    accessibility_features TEXT[],
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Routes (bus routes, subway lines)
CREATE TABLE routes (
    route_id SERIAL PRIMARY KEY,
    external_route_id VARCHAR(255),
    route_name VARCHAR(255) NOT NULL,
    route_type transportation_mode NOT NULL,
    route_geometry GEOMETRY(LINESTRING, 4326),
    color VARCHAR(7), -- hex color code
    description TEXT,
    agency VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Route Stops (relationship between routes and stations)
CREATE TABLE route_stops (
    route_stop_id SERIAL PRIMARY KEY,
    route_id INTEGER REFERENCES routes(route_id),
    station_id INTEGER REFERENCES stations(station_id),
    stop_sequence INTEGER NOT NULL,
    distance_from_start DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(route_id, station_id)
);

-- Real-time Vehicle Positions
CREATE TABLE vehicle_positions (
    position_id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(255) NOT NULL,
    route_id INTEGER REFERENCES routes(route_id),
    timestamp TIMESTAMP NOT NULL,
    location GEOMETRY(POINT, 4326) NOT NULL,
    speed DECIMAL(5,2),
    heading INTEGER, -- degrees 0-359
    occupancy_status VARCHAR(50),
    delay_minutes INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Traffic Congestion Data
CREATE TABLE traffic_segments (
    segment_id SERIAL PRIMARY KEY,
    segment_name VARCHAR(255),
    geometry GEOMETRY(LINESTRING, 4326) NOT NULL,
    functional_class VARCHAR(50),
    speed_limit INTEGER,
    length_km DECIMAL(10,3),
    borough VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE traffic_conditions (
    condition_id SERIAL PRIMARY KEY,
    segment_id INTEGER REFERENCES traffic_segments(segment_id),
    timestamp TIMESTAMP NOT NULL,
    average_speed DECIMAL(5,2),
    travel_time_minutes DECIMAL(8,2),
    congestion_level VARCHAR(20), -- 'low', 'medium', 'high', 'severe'
    volume INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Weather Data (affects transportation patterns)
CREATE TABLE weather_data (
    weather_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    temperature_f DECIMAL(5,2),
    humidity_percent INTEGER,
    precipitation_in DECIMAL(5,3),
    wind_speed_mph DECIMAL(5,2),
    visibility_miles DECIMAL(5,2),
    weather_condition VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_trips_pickup_datetime ON trips(pickup_datetime);
CREATE INDEX idx_trips_mode ON trips(mode);
CREATE INDEX idx_trips_pickup_location ON trips USING GIST(pickup_location);
CREATE INDEX idx_trips_dropoff_location ON trips USING GIST(dropoff_location);
CREATE INDEX idx_trips_zones ON trips(pickup_zone_id, dropoff_zone_id);

CREATE INDEX idx_stations_location ON stations USING GIST(location);
CREATE INDEX idx_stations_type ON stations(station_type);

CREATE INDEX idx_routes_geometry ON routes USING GIST(route_geometry);
CREATE INDEX idx_routes_type ON routes(route_type);

CREATE INDEX idx_vehicle_positions_timestamp ON vehicle_positions(timestamp);
CREATE INDEX idx_vehicle_positions_location ON vehicle_positions USING GIST(location);
CREATE INDEX idx_vehicle_positions_vehicle ON vehicle_positions(vehicle_id, timestamp);

CREATE INDEX idx_traffic_conditions_timestamp ON traffic_conditions(timestamp);
CREATE INDEX idx_traffic_conditions_segment ON traffic_conditions(segment_id, timestamp);

CREATE INDEX idx_weather_timestamp ON weather_data(timestamp);

-- Spatial indexes on zones
CREATE INDEX idx_zones_geometry ON zones USING GIST(geometry); 