-- Seed data for Occitanie cities
-- These cities match the configuration in dags/config/occitanie_cities.json

INSERT INTO dim_cities (city_name, latitude, longitude, region) VALUES
    ('Toulouse', 43.60470, 1.44420, 'Occitanie'),
    ('Montpellier', 43.61080, 3.87670, 'Occitanie'),
    ('Perpignan', 42.68860, 2.89480, 'Occitanie'),
    ('Carcassonne', 43.21320, 2.35080, 'Occitanie'),
    ('Nîmes', 43.83670, 4.36010, 'Occitanie'),
    ('Albi', 43.92980, 2.14800, 'Occitanie')
ON CONFLICT (city_name) DO NOTHING;

-- Initialize ETL watermark
INSERT INTO etl_watermark (job_name, last_processed_timestamp)
VALUES ('spark_weather_transform', '2000-01-01 00:00:00')
ON CONFLICT (job_name) DO NOTHING;
