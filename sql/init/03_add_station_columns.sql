-- Add station_id columns to trips table for bike share support
ALTER TABLE trips 
ADD COLUMN pickup_station_id VARCHAR(255),
ADD COLUMN dropoff_station_id VARCHAR(255);

-- Add indexes for station lookups
CREATE INDEX idx_trips_pickup_station ON trips(pickup_station_id);
CREATE INDEX idx_trips_dropoff_station ON trips(dropoff_station_id);

-- Add comment explaining the columns
COMMENT ON COLUMN trips.pickup_station_id IS 'External station ID for bike share and transit trips';
COMMENT ON COLUMN trips.dropoff_station_id IS 'External station ID for bike share and transit trips'; 