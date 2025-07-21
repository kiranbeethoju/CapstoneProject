-- Enable required extensions for spatial data processing
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;
CREATE EXTENSION IF NOT EXISTS uuid-ossp;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Create custom types
CREATE TYPE transportation_mode AS ENUM (
    'taxi', 'bus', 'subway', 'bikeshare', 'walking', 'rideshare'
);

CREATE TYPE trip_status AS ENUM (
    'completed', 'cancelled', 'in_progress'
);

-- Set timezone
SET timezone = 'America/New_York'; 