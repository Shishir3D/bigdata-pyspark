import time
import sys
import os
import psycopg2
from psycopg2 import sql  # type : ignore
from pyspark.sql import SparkSession

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utility.utility import setup_logging


def create_spark_session(spark_config):
    """Initialize Spark session."""
    logger.debug("Initializing Spark Session with default parameters")
    return (
        SparkSession.builder.master(f"spark://{spark_config['master_ip']}:7077")
        .appName("SpotifyDataLoad")
        .config("spark.driver.memory", spark_config["driver_memory"])
        .config("spark.executor.memory", spark_config["executor_memory"])
        .config("spark.executor.cores", spark_config["executor_cores"])
        .config("spark.executor.instances", spark_config["executor_instances"])
        .getOrCreate()
    )


def create_postgres_tables(logger, pg_un, pg_pw):
    conn = None
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=pg_un,
            password=pg_pw,
            host="localhost",
            port="5432",
        )
        cursor = conn.cursor()

        create_table_queries = [
            """
                CREATE TABLE IF NOT EXISTS master_table (
                    track_id VARCHAR(50),
                    track_name TEXT,
                    track_popularity INTEGER,
                    artist_id VARCHAR(50),
                    artist_name TEXT,
                    followers FLOAT,
                    genres TEXT,
                    artist_popularity INTEGER,
                    danceability FLOAT,
                    energy FLOAT,
                    tempo FLOAT,
                    related_ids TEXT[]
                );
                """,
            """
                CREATE TABLE IF NOT EXISTS recommendations_exploded (
                    id VARCHAR(50),
                    related_id VARCHAR(50)
                );
                """,
            """
                CREATE TABLE IF NOT EXISTS artist_track (
                    id VARCHAR (50),
                    artist_id VARCHAR(50)
                );
                """,
            """
                CREATE TABLE IF NOT EXISTS track_metadata (
                    id VARCHAR(50) PRIMARY KEY,
                    name TEXT,
                    popularity INTEGER,
                    duration_ms INTEGER,
                    danceability FLOAT,
                    energy FLOAT,
                    tempo FLOAT
                );
                """,
            """
                CREATE TABLE IF NOT EXISTS artist_metadata (
                    id VARCHAR(50) PRIMARY KEY,
                    name TEXT,
                    followers FLOAT,
                    popularity INTEGER
                );
                """,
        ]

        for query in create_table_queries:
            cursor.execute(query)
        conn.commit()
        logger.info("PostgreSQL tables created successfully")

    except Exception as e:
        logger.info(f"Error creating tables: {e}")
    finally:
        if cursor:  # type:ignore
            cursor.close()
        if conn:
            conn.close()


def load_to_postgres(logger, spark, input_dir, pg_un, pg_pw):
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    connection_properties = {
        "user": pg_un,
        "password": pg_pw,
        "driver": "org.postgresql.Driver",
    }

    tables = [
        ("stage2/master_table", "master_table"),
        ("stage3/recommendations_exploded", "recommendations_exploded"),
        ("stage3/artist_track", "artist_track"),
        ("stage3/track_metadata", "track_metadata"),
        ("stage3/artist_metadata", "artist_metadata"),
    ]

    for parquet_path, table_name in tables:
        try:
            df = spark.read.parquet(os.path.join(input_dir, parquet_path))
            mode = "append" if "master" in parquet_path else "overwrite"
            df.write.mode(mode).jdbc(
                url=jdbc_url, table=table_name, properties=connection_properties
            )
            logger.info(f"Loaded {table_name} to PostgreSQL")
        except Exception as e:
            logger.info(f"Error loading {table_name}: {e}")


if __name__ == "__main__":

    logger = setup_logging("load.log")

    if len(sys.argv) != 9:
        logger.critical(
            "Usage: python load/execute.py <input_dir> <pg_un> <pg_pw>  pg_host  d_mem e_mem e_core e_inst"
        )
        sys.exit(1)

    logger.info("load stage started")
    start = time.time()

    input_dir = sys.argv[1]
    pg_un = sys.argv[2]
    pg_pw = sys.argv[3]
    spark_config = {}
    spark_config["master_ip"] = sys.argv[3]
    spark_config["driver_memory"] = sys.argv[4]
    spark_config["executor_memory"] = sys.argv[4]
    spark_config["executor_cores"] = sys.argv[6]
    spark_config["executor_instances"] = sys.argv[7]

    if not os.path.exists(input_dir):
        logger.error(f"Error: Input directory {input_dir} does not exist")
        sys.exit(1)

    spark = create_spark_session(logger)
    create_postgres_tables(logger, pg_un, pg_pw)
    load_to_postgres(logger, spark, input_dir, pg_un, pg_pw)

    end = time.time()
    logger.info("Load stage completed")
