import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):   
    # get filepath to song data file
    song_data = os.path.join('{}song_data/*/*/*/*.json'.format(input_data))
    
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()

    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year').parquet(os.path.join(output_data, 'songs'), mode='overwrite')
    
    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id', 'artist_name as name', 'artist_location as location', 
                                  'artist_latitude as latitude', 'artist_longitude as longitude').dropDuplicates()
    
    # write artists table to parquet file
    artists_table.write.parquet(os.path.join(output_data, 'artists'), mode='overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join('{}log_data/*/*/*.json'.format(input_data))

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr('userId as user_id', 'firstName as first_name', 'lastName as last_name', 
                                'gender', 'level').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ms: datetime.fromtimestamp(ms/1000.0),TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df.selectExpr('timestamp as start_time', 'hour(timestamp) as hour', 'dayofmonth(timestamp) as day', 
                               'weekofyear(timestamp) as week', 'month(timestamp) as month', 'year(timestamp) as year', 
                               'dayofweek(timestamp) as weekday')
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'times'), mode='overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join('{}log_data/*/*/*.json'.format(input_data)))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (song_df.artist_name == df.artist) & (song_df.title == df.song) & (df.length ==song_df.duration), 'left_outer')
    songplays_table = songplays_table.selectExpr('timestamp as start_time', 'userId as user_id', 'level', 'song_id', 'artist_id', 
                                                 'sessionId as session_id', 'location', 'userAgent as user_agent').withColumn('songplay_id', monotonically_increasing_id())
    songplays_table = songplays_table.withColumn('year', year('start_time')).withColumn('month', month('start_time'))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays'), mode='overwrite')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://nd027/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
