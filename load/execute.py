import os
import sys
import time
import psycopg2
from pyspark.sql import SparkSession

# Import custom logger and time formatter
sys.path.append('/home/bishnu/ETL')  # ✅ Replace with your absolute ETL path
from utility.utility import setup_logger, format_seconds

logger = setup_logger("load.log")  # ✅ Add a name for the log file

# Check for required arguments
if len(sys.argv) < 7:
    logger.error("Usage: python execute.py <parquet_output_dir> <db_name> <db_user> <db_host> <db_port> <db_password>")
    sys.exit(1)

# Parse arguments
parquet_dir = sys.argv[1]
DB_NAME = sys.argv[2]
DB_USER = sys.argv[3]
DB_HOST = sys.argv[4]
DB_PORT = sys.argv[5]
DB_PASSWORD = sys.argv[6]

# Function to create tables
def create_tables(db_name, db_user, db_password, db_host, db_port):
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cur = conn.cursor()
        logger.info("✅ Connected to PostgreSQL.")

        create_statements = [
            """
            CREATE TABLE IF NOT EXISTS artists (
                id TEXT PRIMARY KEY,
                name TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tracks (
                id TEXT PRIMARY KEY,
                name TEXT,
                id_artists TEXT[],
                release_date TEXT,
                duration_ms TEXT,
                explicit TEXT,
                popularity TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS artist_metadata (
                id TEXT PRIMARY KEY,
                name TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS track_metadata (
                id TEXT PRIMARY KEY,
                name TEXT,
                release_date TEXT,
                popularity TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS track_artist_map (
                id TEXT,
                artist_id TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS recommendations (
                track_id TEXT,
                recommendations TEXT[]
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS exploded_recommendations (
                track_id TEXT,
                recommendations TEXT[],
                recommendation TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS master (
                id TEXT,
                name TEXT,
                id_artists TEXT[],
                release_date TEXT,
                duration_ms TEXT,
                explicit TEXT,
                popularity TEXT,
                artist_id TEXT,
                artist_name TEXT,
                track_id TEXT,
                recommendations TEXT[]
            )
            """
        ]

        for stmt in create_statements:
            cur.execute(stmt)

        conn.commit()
        cur.close()
        conn.close()
        logger.info("✅ All tables created successfully.")
    except Exception as e:
        logger.error("❌ Error creating tables", exc_info=True)

# Function to load data
def load_parquet_to_postgres(parquet_path, db_name, db_user, db_password, db_host, db_port):
    try:
        spark = SparkSession.builder \
            .appName("LoadParquetToPostgres") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
            .getOrCreate()

        for folder in os.listdir(parquet_path):
            full_path = os.path.join(parquet_path, folder)
            if os.path.isdir(full_path):
                logger.info(f"📦 Loading dataset: {folder}")
                df = spark.read.parquet(full_path)

                df.write \
                    .format("jdbc") \
                    .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}") \
                    .option("dbtable", folder) \
                    .option("user", db_user) \
                    .option("password", db_password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("overwrite") \
                    .save()

                logger.info(f"✅ Loaded: {folder}")
    except Exception as e:
        logger.error("❌ Error loading Parquet to PostgreSQL", exc_info=True)

# Run main
if __name__ == "__main__":
    start_time = time.time()

    create_tables(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)
    load_parquet_to_postgres(parquet_dir, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)

    end_time = time.time()
    logger.info(f"🕒 Execution time: {format_seconds(end_time - start_time)}")
