-- Weather Data Warehouse Schema
-- PostgreSQL schema for transformed weather data

-- Dimension: Cities (Occitanie region)
CREATE TABLE dim_cities (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL UNIQUE,
    latitude DECIMAL(8, 5) NOT NULL,
    longitude DECIMAL(8, 5) NOT NULL,
    region VARCHAR(100) DEFAULT 'Occitanie'
);

-- Fact: Hourly weather observations (structured data from Spark transformation)
CREATE TABLE fact_weather_hourly (
    id BIGSERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES dim_cities(city_id),
    observation_time TIMESTAMP NOT NULL,
    ingestion_time TIMESTAMP NOT NULL,
    temperature_2m DECIMAL(5, 2),
    precipitation DECIMAL(6, 2),
    wind_speed_10m DECIMAL(5, 2),
    wind_direction_10m INTEGER,
    relative_humidity_2m INTEGER,
    pressure_msl DECIMAL(7, 2),
    cloud_cover INTEGER,
    source_doc_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(city_id, observation_time, ingestion_time)
);

-- Staging table for Spark writes (before city_id resolution)
CREATE TABLE fact_weather_hourly_staging (
    id BIGSERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    observation_time TIMESTAMP NOT NULL,
    ingestion_time TIMESTAMP NOT NULL,
    temperature_2m DECIMAL(5, 2),
    precipitation DECIMAL(6, 2),
    wind_speed_10m DECIMAL(5, 2),
    wind_direction_10m INTEGER,
    relative_humidity_2m INTEGER,
    pressure_msl DECIMAL(7, 2),
    cloud_cover INTEGER,
    source_doc_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE
);

-- Indexes for performance
CREATE INDEX idx_weather_city_time ON fact_weather_hourly(city_id, observation_time);
CREATE INDEX idx_weather_created ON fact_weather_hourly(created_at);
CREATE INDEX idx_staging_processed ON fact_weather_hourly_staging(processed);
CREATE INDEX idx_staging_city ON fact_weather_hourly_staging(city_name);

-- ETL watermark tracking for incremental processing
CREATE TABLE etl_watermark (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) UNIQUE,
    last_processed_timestamp TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Function to move data from staging to fact table
CREATE OR REPLACE FUNCTION process_staging_data()
RETURNS INTEGER AS $$
DECLARE
    rows_processed INTEGER;
BEGIN
    -- Insert into fact table with city_id lookup
    INSERT INTO fact_weather_hourly (
        city_id, observation_time, ingestion_time,
        temperature_2m, precipitation, wind_speed_10m, wind_direction_10m,
        relative_humidity_2m, pressure_msl, cloud_cover, source_doc_id
    )
    SELECT
        c.city_id, s.observation_time, s.ingestion_time,
        s.temperature_2m, s.precipitation, s.wind_speed_10m, s.wind_direction_10m,
        s.relative_humidity_2m, s.pressure_msl, s.cloud_cover, s.source_doc_id
    FROM fact_weather_hourly_staging s
    JOIN dim_cities c ON s.city_name = c.city_name
    WHERE s.processed = FALSE
    ON CONFLICT (city_id, observation_time, ingestion_time) DO NOTHING;

    GET DIAGNOSTICS rows_processed = ROW_COUNT;

    -- Mark staging rows as processed
    UPDATE fact_weather_hourly_staging SET processed = TRUE WHERE processed = FALSE;

    RETURN rows_processed;
END;
$$ LANGUAGE plpgsql;
