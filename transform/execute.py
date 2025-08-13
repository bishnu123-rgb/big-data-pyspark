import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, regexp_replace
sys.path.append('/home/bishnu/ETL')
from utility.utility import setup_logger, format_seconds

logger = setup_logger("transform.log")

def create_spark_session():
    return SparkSession.builder \
        .appName("ETLTransform") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

def clean_and_write_parquet(spark, input_path, output_path):
    artist_schema = "id STRING, name STRING"
    track_schema = """
        id STRING, name STRING, id_artists STRING,
        release_date STRING, duration_ms STRING,
        explicit STRING, popularity STRING
    """
    rec_schema = "track_id STRING, recommendations ARRAY<STRING>"

    artists_df = spark.read.csv(os.path.join(input_path, "artists.csv"), header=True, schema=artist_schema)
    tracks_df = spark.read.csv(os.path.join(input_path, "tracks.csv"), header=True, schema=track_schema)
    rec_df = spark.read.json(os.path.join(input_path, "dict_artists_fixed.json"), schema=rec_schema)

    artists_df = artists_df.dropDuplicates(["id"]).filter(col("id").isNotNull())
    tracks_df = tracks_df.dropDuplicates(["id"]).filter(col("id").isNotNull())
    rec_df = rec_df.dropDuplicates(["track_id"]).filter(col("track_id").isNotNull())

    tracks_df = tracks_df.withColumn("id_artists", split(regexp_replace("id_artists", r"[\[\]']", ""), r",\s*"))

    artists_df.write.mode("overwrite").parquet(os.path.join(output_path, "artists"))
    tracks_df.write.mode("overwrite").parquet(os.path.join(output_path, "tracks"))
    rec_df.write.mode("overwrite").parquet(os.path.join(output_path, "recommendations"))

    return artists_df, tracks_df, rec_df

def create_master_table(artists_df, tracks_df, rec_df, output_path):
    artists_df = artists_df.withColumnRenamed("name", "artist_name")
    exploded_tracks = tracks_df.withColumn("artist_id", explode(col("id_artists")))
    joined = exploded_tracks.join(artists_df, exploded_tracks.artist_id == artists_df.id, "left").drop(artists_df.id)
    joined = joined.join(rec_df, joined.id == rec_df.track_id, "left")
    joined.write.mode("overwrite").parquet(os.path.join(output_path, "master"))

def create_analytics_datasets(output_path, rec_df, tracks_df, artists_df):
    exploded_recs = rec_df.withColumn("recommendation", explode(col("recommendations")))
    exploded_recs.write.mode("overwrite").parquet(os.path.join(output_path, "exploded_recommendations"))

    track_artist = tracks_df.select("id", explode(col("id_artists")).alias("artist_id"))
    track_artist.write.mode("overwrite").parquet(os.path.join(output_path, "track_artist_map"))

    track_meta = tracks_df.select("id", "name", "release_date", "popularity")
    track_meta.write.mode("overwrite").parquet(os.path.join(output_path, "track_metadata"))

    artist_meta = artists_df.select("id", "name")
    artist_meta.write.mode("overwrite").parquet(os.path.join(output_path, "artist_metadata"))

if __name__ == "__main__":
    start_time = time.time()

    if len(sys.argv) < 3:
        logger.error("Usage: python execute.py <input_path> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    try:
        spark = create_spark_session()
        artists_df, tracks_df, rec_df = clean_and_write_parquet(spark, input_path, output_path)
        create_master_table(artists_df, tracks_df, rec_df, output_path)
        create_analytics_datasets(output_path, rec_df, tracks_df, artists_df)
        logger.info("Transform completed successfully.")
    except Exception as e:
        logger.error("Transform failed", exc_info=True)

    end_time = time.time()
    logger.info(f"🕒 Execution time: {format_seconds(end_time - start_time)}")

