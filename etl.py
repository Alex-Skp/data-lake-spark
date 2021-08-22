import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp, dayofweek
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, FloatType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "data/song_data/*/*/*/*.json"
     
    # read song data file
    song_data_schema = StructType([
    StructField("num_songs",         IntegerType()),
    StructField("artist_id",         StringType()),
    StructField("artist_latitude",   FloatType()),
    StructField("artist_longitude",  FloatType()),
    StructField("artist_location",   StringType()),
    StructField("artist_name",       StringType()),
    StructField("song_id",           StringType()),
    StructField("title",             StringType()),
    StructField("duration",          FloatType()),
    StructField("year",              IntegerType()),
    ])

    df = spark.read.json(song_data, schema=song_data_schema)

    # extract columns to create songs table
    songs_table = df.select("song_id", 
                            "title", 
                            "artist_id",
                            "year",
                            "duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite')\
        .partitionBy("year","artist_id")\
        .parquet("./output/songs_table.parquet")

    # extract columns to create artists table
    artists_table = df.select("artist_id",
                              col("artist_name").alias("name"),
                              col("artist_location").alias("location"),
                              col("artist_latitude").alias("lattitude"),
                              col("artist_longitude").alias("longitude")).distinct()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite')\
        .partitionBy("artist_id")\
        .parquet(output_data + "output/artists_table.parquet")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "data/log_data"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page == 'NextSong'")

    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"),
                            col("firstName").alias("first_name"),
                            col("lastName").alias("last_name"),
                            col("gender"),
                            col("level")).distinct()
    
    # write users table to parquet files
    artists_table.write.mode('overwrite')\
        .partitionBy("user_id")\
        .parquet(output_data + "output/users_table.parquet")

    # create timestamp column from original timestamp column
    df = df.na.drop(subset=["ts"])
    df = df.withcolumn("timestamp", from_unixtime(col("ts")/1000))
    
    # extract columns to create time table
    time_table = df.select(col("timestamp").alias("start_time"),
                           hour(col("timestamp")).alias("hour"),
                           dayofmonth(col("timestamp")).alias("day"),
                           weekofyear(col("timestamp")).alias("week"),
                           month(col("timestamp")).alias("month"),
                           year(col("timestamp")).alias("year"),
                           dayofweek(col("timestamp")).alias("weekday")
                           ).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.mode('overwrite')\
        .partitionBy("year", "month")\
        .parquet(output_data + "output/time_table.parquet")

    # read in song data to use for songplays table

    song_data_url = input_data + "data/song_data/*/*/*/*.json"
    song_data_schema = StructType([
    StructField("num_songs",         IntegerType()),
    StructField("artist_id",         StringType()),
    StructField("artist_latitude",   FloatType()),
    StructField("artist_longitude",  FloatType()),
    StructField("artist_location",   StringType()),
    StructField("artist_name",       StringType()),
    StructField("song_id",           StringType()),
    StructField("title",             StringType()),
    StructField("duration",          FloatType()),
    StructField("year",              IntegerType()),
    ])
    song_data = spark.read.json(song_data_url, schema=song_data_schema) 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_data, (song_data.title == df.song)\
                                        &(song_data.artist_name == df.artist)\
                                        &(song_data.duration == df.length), how='left')\
                        .select(col("timestamp").alias("start_time"),
                                col("userId").alias("user_id"),
                                col("level"),
                                col("song_id"),
                                col("artist_id"),
                                col("sessionId").alias("session_id"),
                                col("location"),
                                col("userAgent").alias("user_agent")).distinct()


    # write songplays table to parquet files partitioned by year and month??
    songplays_table.mode('overwrite')\
        .partitionBy("year", "month")\
        .parquet(output_data + "output/songplays_table.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "" #INSERT BUCKET URL FOR THE OUTPUT 
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
