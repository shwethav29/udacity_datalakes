import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql import types as T

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    SONG_FILE_PATH = "song_data/A/*/*/*.json"
    input_song_data_file = os.path.join(input_data, SONG_FILE_PATH)

    # song schemae
    song_schema = T.StructType(
        [
            T.StructField("num_songs", T.IntegerType(), True),
            T.StructField("artist_id", T.StringType(), True),
            T.StructField("artist_latitude", T.DoubleType(), True),
            T.StructField("artist_longitude", T.DoubleType(), True),
            T.StructField("artist_location", T.StringType(), True),
            T.StructField("artist_name", T.StringType(), True),
            T.StructField("song_id", T.StringType(), True),
            T.StructField("title", T.StringType(), True),
            T.StructField("duration", T.DoubleType(), True),
            T.StructField("year", T.IntegerType(), True)
        ])

    # read song data file
    df = spark.read.json(input_song_data_file, song_schema)

    # extract columns to create songs table
    songs_table = df.selectExpr('song_id', 'title', 'artist_id', 'year', 'duration')

    # write songs table to parquet files partitioned by year and artist. Also using repartition to partition the data in memory before writing to disk
    songs_table.repartition("year", "artist_id").write.partitionBy('year', 'artist_id').mode("overwrite").parquet(
        output_data + 'songs/')

    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    LOG_DATA_PATH = "log_data/*/*/*.json"
    input_log_data_file = os.path.join(input_data, LOG_DATA_PATH)

    # log data schema
    log_schema = T.StructType(
        [
            T.StructField("artist", T.StringType(), True),
            T.StructField("auth", T.StringType(), True),
            T.StructField("firstName", T.StringType(), True),
            T.StructField("gender", T.StringType(), True),
            T.StructField("itemInSession", T.IntegerType(), True),
            T.StructField("lastName", T.StringType(), True),
            T.StructField("length", T.DoubleType(), True),
            T.StructField("level", T.StringType(), True),
            T.StructField("location", T.StringType(), True),
            T.StructField("method", T.StringType(), True),
            T.StructField("page", T.StringType(), True),
            T.StructField("registration", T.StringType(), True),
            T.StructField("sessionId", T.StringType(), True),
            T.StructField("song", T.StringType(), True),
            T.StructField("status", T.IntegerType(), True),
            T.StructField("ts", T.LongType(), True),
            T.StructField("userAgent", T.StringType(), True),
            T.StructField("userid", T.StringType(), True)
        ])

    # read log data file
    df = spark.read.json(input_log_data_file, log_schema)

    # filter by actions for song plays
    df = df.filter("page=='NextSong'")

    # extract columns for users table
    users_table = df.selectExpr('userId', 'firstName', 'lastName', 'gender', 'level')
    # get unique not null users
    users_table = users_table.dropDuplicates(subset=['userId'])
    users_table = users_table.where(col('userId').isNotNull())
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users/')

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp((x / 1000)), T.TimestampType())
    df = df.withColumn("timestamp", get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select(
        df.ts.alias('start_time'),
        hour("timestamp").alias('hour'),
        dayofmonth("timestamp").alias('day'),
        weekofyear("timestamp").alias('week'),
        month("timestamp").alias('month'),
        year("timestamp").alias('year'),
        dayofweek("timestamp").alias('weekday'))

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode("overwrite").parquet(output_data + 'time/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs/')
    # read artist data
    artist_df = spark.read.parquet(output_data + "artists/")

    # song_df = song_df.alias('song_df')
    # artist_df = artist_df.alias('artist_df')

    song_artist_df = song_df.join(artist_df, ['artist_id'], "inner").alias("sa")

    # extract columns from joined song and log datasets to create songplays table

    df = df.alias("se")
    songplays_table = df.join(
        song_artist_df.withColumnRenamed('title', 'song').withColumnRenamed('artist_name', 'artist').withColumnRenamed(
            'duration', 'length'), ['song', 'artist', 'length']).selectExpr('se.ts', 'se.userid', 'se.level',
                                                                            'sa.artist_id', 'se.sessionid',
                                                                            'se.location', 'se.userAgent')
    songplays_table = songplays_table.dropDuplicates(
        subset=['ts', 'userid', 'level', 'artist_id', 'sessionid', 'location', 'userAgent'])

    songplays_table = songplays_table.join(time_table.withColumnRenamed('start_time', 'ts'), 'ts', 'inner').selectExpr(
        'ts', 'userid', 'level', 'artist_id', 'sessionid', 'location', 'useragent', 'year', 'month')
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode("overwrite").parquet(output_data + 'songplays/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://shwe-uda-dl-spark-project/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
