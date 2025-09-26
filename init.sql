-- Таблица для хранения почасовых данных
CREATE TABLE IF NOT EXISTS prefect_data.weather_hourly (
    city String,
    timestamp DateTime,
    temperature_2m Float32,
    precipitation Float32,
    wind_speed_10m Float32,
    wind_direction_10m Int16
) ENGINE = MergeTree()
ORDER BY (city, timestamp);

-- Таблица для хранения агрегированных дневных данных
CREATE TABLE IF NOT EXISTS prefect_data.weather_daily (
    city String,
    date Date,
    temp_min Float32,
    temp_max Float32,
    temp_avg Float32,
    precipitation_sum Float32
) ENGINE = MergeTree()
ORDER BY (city, date);