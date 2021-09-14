import os

import configparser
from datetime import datetime

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
import pyspark.sql.types as T



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark



def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(
        input_data,
        'song_data/*/*/*/*.json'
    )
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
      'song_id',
      'title',
      'artist_id',
      'year',
      'duration'
    ).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write\
        .mode('overwrite')\
            .partitionBy('year', 'artist_id')\
                .parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = df.selectExpr(
      'artist_id', 
      'artist_name as name', 
      'artist_location as location', 
      'artist_latitude as latitude', 
      'artist_longitude as longitude'
    ).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write\
        .mode('overwrite')\
            .parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(
        input_data,
        'log_data/*/*/*.json'
    )

    # read log data file
    df = spark.read\
          .option("multiline",True)\
          .option("mode", "FAILFAST")\
          .json(log_data)
    
    # filter by actions for song plays
    df = df.filter(col('page') == 'NextSong')
    
    # extract columns for users table    
    users_table = df.selectExpr(
      'userId as user_id',
      'firstName as first_name',
      'lastName as last_name',
      'gender',
      'level'
    )

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(
      os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda t: t/1000, T.TimestampType())
    df = df.withColumn('timestamp', get_timestamp(col('ts')))
    
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda t: datetime.fromtimestamp(t), T.DateType())
    df = df.withColumn('start_time', get_datetime(col('timestamp')))

    # extract columns to create time table
    df.createOrReplaceTempView('events')
    
    time_table = spark.sql('''
    select
      start_time,
      hour(start_time) as hour,
      day(start_time) as day,
      weekofyear(start_time) as week,
      month(start_time) as month,
      year(start_time) as year,
      weekday(start_time) as weekday
    from events
    '''
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.dropDuplicates().write.mode('overwrite').partitionBy('year', 'month').parquet(
      os.path.join(output_data, 'time')
    )

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs'))
    
    # read in song data to use for songplays table
    artists_df = spark.read.parquet(os.path.join(output_data, 'artists'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(
      artists_df,
      'artist_id',
      'outer'
    ).join(
      df.withColumnRenamed('location', 'location_event'),
      (song_df['title'] == df['song']) &
      (song_df['duration'] == df['length']) &
      (artists_df['name'] == df['artist']),
      'rightouter'
    ).withColumn('songplay_id', F.monotonically_increasing_id()).selectExpr(
      'songplay_id', 
      'start_time', 
      'userId as user_id',
      'level',
      'song_id', 
      'artist_id', 
      'sessionId as session_id',
      'location_event as location',
      'userAgent as user_agent'
    ).join(time_table, 'start_time', 'inner').repartition('year', 'month')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(
      os.path.join(output_data, 'songplays')
    )


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
