from prometheus_client import start_http_server, Gauge, Counter, Info
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import psycopg2
from psycopg2 import OperationalError
import time
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://admin:admin@mongodb:27017/')
MONGO_DB = os.getenv('MONGO_DB', 'weather')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'raw_weather')

PG_HOST = os.getenv('PG_HOST', 'postgres-weather')
PG_PORT = int(os.getenv('PG_PORT', '5432'))
PG_USER = os.getenv('PG_USER', 'weather')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'weather123')
PG_DATABASE = os.getenv('PG_DATABASE', 'weather_db')

METRICS_PORT = int(os.getenv('METRICS_PORT', '9101'))
SCRAPE_INTERVAL = int(os.getenv('SCRAPE_INTERVAL', '15'))

raw_data_count = Gauge(
    'weather_raw_documents_total',
    'Total raw documents in MongoDB'
)

raw_data_by_city = Gauge(
    'weather_raw_documents_by_city',
    'Raw documents per city in MongoDB',
    ['city']
)

processed_data_count = Gauge(
    'weather_processed_rows_total',
    'Total processed rows in PostgreSQL fact table'
)

processed_data_by_city = Gauge(
    'weather_processed_rows_by_city',
    'Processed rows per city in PostgreSQL',
    ['city']
)

staging_data_count = Gauge(
    'weather_staging_rows_total',
    'Total rows in PostgreSQL staging table'
)

staging_unprocessed_count = Gauge(
    'weather_staging_unprocessed_total',
    'Unprocessed rows in PostgreSQL staging table'
)

collection_errors = Counter(
    'weather_metrics_collection_errors_total',
    'Total errors during metrics collection',
    ['source']
)

last_collection_timestamp = Gauge(
    'weather_metrics_last_collection_timestamp',
    'Timestamp of last successful metrics collection'
)

exporter_info = Info(
    'weather_metrics_exporter',
    'Information about the metrics exporter'
)


def collect_mongodb_metrics():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')

        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]

        total_raw = collection.count_documents({})
        raw_data_count.set(total_raw)
        logger.info(f"MongoDB raw documents: {total_raw}")

        pipeline = [
            {"$group": {"_id": "$city", "count": {"$sum": 1}}}
        ]
        for doc in collection.aggregate(pipeline):
            city = doc['_id']
            count = doc['count']
            raw_data_by_city.labels(city=city).set(count)
            logger.debug(f"MongoDB {city}: {count} documents")

        client.close()
        return True

    except ConnectionFailure as e:
        logger.error(f"MongoDB connection failed: {e}")
        collection_errors.labels(source='mongodb').inc()
        return False
    except Exception as e:
        logger.error(f"MongoDB metrics collection error: {e}")
        collection_errors.labels(source='mongodb').inc()
        return False


def collect_postgresql_metrics():
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            database=PG_DATABASE,
            connect_timeout=5
        )
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM fact_weather_hourly")
        total_processed = cursor.fetchone()[0]
        processed_data_count.set(total_processed)
        logger.info(f"PostgreSQL processed rows: {total_processed}")

        cursor.execute("""
            SELECT c.city_name, COUNT(*)
            FROM fact_weather_hourly f
            JOIN dim_cities c ON f.city_id = c.city_id
            GROUP BY c.city_name
        """)
        for row in cursor.fetchall():
            city_name, count = row
            processed_data_by_city.labels(city=city_name).set(count)
            logger.debug(f"PostgreSQL {city_name}: {count} rows")

        cursor.execute("SELECT COUNT(*) FROM fact_weather_hourly_staging")
        staging_total = cursor.fetchone()[0]
        staging_data_count.set(staging_total)

        cursor.execute(
            "SELECT COUNT(*) FROM fact_weather_hourly_staging WHERE processed = FALSE"
        )
        staging_unprocessed = cursor.fetchone()[0]
        staging_unprocessed_count.set(staging_unprocessed)

        logger.info(
            f"PostgreSQL staging: {staging_total} total, "
            f"{staging_unprocessed} unprocessed"
        )

        cursor.close()
        conn.close()
        return True

    except OperationalError as e:
        logger.error(f"PostgreSQL connection failed: {e}")
        collection_errors.labels(source='postgresql').inc()
        return False
    except Exception as e:
        logger.error(f"PostgreSQL metrics collection error: {e}")
        collection_errors.labels(source='postgresql').inc()
        return False


def collect_metrics():
    mongo_ok = collect_mongodb_metrics()
    pg_ok = collect_postgresql_metrics()

    if mongo_ok or pg_ok:
        last_collection_timestamp.set(time.time())

    logger.info(
        f"Metrics collection completed - MongoDB: {'OK' if mongo_ok else 'FAIL'}, "
        f"PostgreSQL: {'OK' if pg_ok else 'FAIL'}"
    )


def main():
    logger.info(f"Metrics port: {METRICS_PORT}")
    logger.info(f"Scrape interval: {SCRAPE_INTERVAL}s")
    logger.info(f"MongoDB: {MONGO_URI}")
    logger.info(f"PostgreSQL: {PG_HOST}:{PG_PORT}/{PG_DATABASE}")

    exporter_info.info({
        'version': '1.0.0',
        'mongo_db': MONGO_DB,
        'pg_database': PG_DATABASE
    })

    start_http_server(METRICS_PORT)
    logger.info(f"Metrics server started on port {METRICS_PORT}")

    while True:
        try:
            collect_metrics()
        except Exception as e:
            logger.error(f"Unexpected error in collection loop: {e}")

        time.sleep(SCRAPE_INTERVAL)


if __name__ == '__main__':
    main()
