import httpx
import pandas as pd
import s3fs
import clickhouse_connect
from prefect import flow, task
from datetime import date, timedelta
import json

from prefect.blocks.system import JSON

CITIES = {
    "Москва": {"lat": 55.7558, "lon": 37.6176},
    "Самара": {"lat": 53.1959, "lon": 50.1002},
}


@task(retries=3, retry_delay_seconds=5)
async def fetch_weather(lat: float, lon: float) -> dict:
    """Извлечение данных о погоде из Open-Meteo API."""
    tomorrow = date.today() + timedelta(days=1)
    api_url = f"https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,wind_speed_10m,wind_direction_10m",
        "start_date": tomorrow.strftime("%Y-%m-%d"),
        "end_date": tomorrow.strftime("%Y-%m-%d"),
    }
    async with httpx.AsyncClient() as client:
        response = await client.get(api_url, params=params)
        response.raise_for_status()
        return response.json()


@task
async def save_to_minio(data: dict, city: str):
    """Сохранение сырых JSON данных в MinIO"""
    # Загружаем конфигурацию MinIO из секретов prefect (блока)
    minio_config = await JSON.load("minio-credentials")
    creds = minio_config.value

    tomorrow_str = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    file_path = f"s3://{creds['bucket_name']}/raw_weather_{city}_{tomorrow_str}.json"

    fs = s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": creds['endpoint_url']},
        key=creds['access_key'],
        secret=creds['secret_key'],
    )

    with fs.open(file_path, "w") as f:
        json.dump(data, f)
    print(f"Raw data for {city} saved to {file_path}")


@task
def transform_hourly_data(raw_data: dict, city: str) -> pd.DataFrame:
    """Трансформация данных для почасовой таблицы."""
    hourly_data = raw_data["hourly"]
    df = pd.DataFrame(hourly_data)
    df["city"] = city
    df.rename(columns={"time": "timestamp"}, inplace=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


@task
def transform_daily_data(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """Аггрегация данных для дневной таблицы."""
    df = hourly_df.copy()
    df["date"] = df["timestamp"].dt.date

    daily_agg = df.groupby(["city", "date"]).agg(
        temp_min=("temperature_2m", "min"),
        temp_max=("temperature_2m", "max"),
        temp_avg=("temperature_2m", "mean"),
        precipitation_sum=("precipitation", "sum"),
    ).reset_index()

    daily_agg['temp_avg'] = daily_agg['temp_avg'].round(2)
    return daily_agg


@task
async def load_to_clickhouse(df: pd.DataFrame, table_name: str):
    """Загрузка DataFrame в ClickHouse с использованием блока."""
    # Загружаем конфигурацию ClickHouse из блока
    ch_config = await JSON.load("clickhouse-connection")
    creds = ch_config.value

    client = clickhouse_connect.get_client(
        host=creds['host'],
        port=creds['port'],
        user=creds['user'],
        password=creds['password'],
        database=creds['database'],
        secure=False
    )
    try:
        client.insert_df(table=table_name, df=df)
        print(f"Successfully loaded {len(df)} rows into {table_name}.")
    finally:
        client.close()


@task
async def send_telegram_notification(daily_data: pd.DataFrame):
    """Отправка уведомления в Telegram с использованием блока."""
    # Загружаем конфигурацию Telegram из блока
    tg_config = await JSON.load("telegram-config")
    creds = tg_config.value

    for _, row in daily_data.iterrows():
        city, temp_min, temp_max, prec_sum = row['city'], row[
            'temp_min'], row['temp_max'], row['precipitation_sum']

        message = (
            f"🌦️ **Прогноз погоды на завтра для г. {city}**\n\n"
            f"🌡️ Температура: от {temp_min}°C до {temp_max}°C\n"
            f"💧 Осадки: {prec_sum:.1f} мм\n"
        )
        if prec_sum > 5.0:
            message += "\n⚠️ **Внимание:** Ожидаются сильные осадки!"

        api_url = f"https://api.telegram.org/bot{creds['bot_token']}/sendMessage"
        payload = {"chat_id": creds['chat_id'],
                   "text": message, "parse_mode": "Markdown"}

        async with httpx.AsyncClient() as client:
            response = await client.post(api_url, json=payload)
            if response.status_code == 200:
                print(f"Notification for {city} sent successfully.")
            else:
                print(
                    f"Failed to send notification for {city}: {response.text}")


@flow(name="Weather ETL Flow")
async def weather_etl_flow():
    """Основной flow, который оркестрирует весь ETL процесс для каждого города."""
    for city, coords in CITIES.items():
        raw_data = await fetch_weather(coords["lat"], coords["lon"])
        save_future = save_to_minio.submit(raw_data, city)

        hourly_df = transform_hourly_data(raw_data, city)
        daily_df = transform_daily_data(hourly_df)

        ch_hourly_future = load_to_clickhouse.submit(
            hourly_df, "weather_hourly")
        ch_daily_future = load_to_clickhouse.submit(daily_df, "weather_daily")

        # Дождемся завершения фоновых задач перед отправкой уведомления
        save_future
        ch_hourly_future
        ch_daily_future

        await send_telegram_notification(daily_df)

if __name__ == "__main__":
    weather_etl_flow.to_deployment(
        name="weather-etl-every-5-minutes",
        work_pool_name="local-pool",
        cron="*/5 * * * *"
    )
