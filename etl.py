import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col, rank, desc, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format,monotonically_increasing_id
from time import time
from pyspark.sql.types import IntegerType,TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    #song_data = input_data + 'song_data/*/*/*/*.json'
    song_data = input_data + 'song_data/A/A/A/*.json'
    
    # read song data file
    print('Reading song data from S3 location: {}'.format(song_data))
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration'])
    
    # write songs table to parquet files partitioned by year and artist
    print('Writing SONGS parquet file at S3 location : {}'.format(output_data + 'songs'))
    t0 = time()
    songs_table.write.partitionBy('year','artist_id').mode('overwrite').parquet(output_data + 'songs')
    t1 = time() - t0
    print("=== DONE IN: {0:.2f} sec\n".format(t1))
    print('{} records added in the SONGS file'.format(songs_table.count()))

    # extract columns to create artists table
    artists_table = df.select(['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']).dropDuplicates()
    
    # write artists table to parquet files
    print('Writing ARTISTS parquet file at S3 location : {}'.format(output_data + 'artists'))
    t0 = time()
    artists_table.write.mode('overwrite').parquet(output_data + 'artists')
    t1 = time() - t0
    print("=== DONE IN: {0:.2f} sec\n".format(t1))
    print('{} records added in the ARTISTS file'.format(artists_table.count()))

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/2018/11'

    # read log data file
    print('reading songplay log file from S3 location: {}'.format(log_data))
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    # extract columns for users table    
    user_rnk = df.select(['userId','firstName','lastName','gender','level','ts']) \
                 .withColumn('rnk',rank().over(Window.partitionBy('userId').orderBy(desc('ts'))))
    
    users_table = user_rnk.select(['userId','firstName','lastName','gender','level']).filter(user_rnk['rnk'] == 1)

    # write users table to parquet files
    print('Writing USERS parquet file at S3 location : {}'.format(output_data + 'users'))
    t0 = time()
    #users_table.write.mode('overwrite').parquet(output_data + 'users')
    t1 = time() - t0
    print("=== DONE IN: {0:.2f} sec\n".format(t1))
    print('{} records added in the USERS file'.format(users_table.count()))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x/1000),IntegerType())
    df = df.withColumn('ts_int', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    df = df.withColumn('start_time', from_unixtime('ts_int').cast(TimestampType()))
    
    # extract columns to create time table
    time_table = df.select('start_time') \
                   .withColumn('hour',hour('start_time')) \
                   .withColumn('day',dayofmonth('start_time')) \
                   .withColumn('week',weekofyear('start_time')) \
                   .withColumn('month',month('start_time')) \
                   .withColumn('year',year('start_time')) \
                   .withColumn('weekday',dayofweek('start_time')) \
                   .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    print('Writing TIME parquet file at S3 location : {}'.format(output_data + 'time'))
    t0 = time()
    time_table.write.mode('overwrite').parquet(output_data + 'time')
    t1 = time() - t0
    print("=== DONE IN: {0:.2f} sec\n".format(t1))
    print('{} records added in the TIME file'.format(time_table.count()))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(input_data + 'songs')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.withColumn("songplay_id", monotonically_increasing_id()) \
                        .join(song_df, song_df.title == df.song, how = 'left') \
                        .select( "songplay_id" \
                                , col("start_time") \
                                , col("userId").alias("user_id") \
                                , "level" \
                                , "song_id" \
                                , "artist_id" \
                                , col("sessionId").alias("session_id") \
                                , "location" \
                                , col("userAgent").alias("user_agent") )

    # write songplays table to parquet files partitioned by year and month
    print('Writing SONGPLAYS parquet file at S3 location : {}'.format(output_data + 'songplays'))
    t0 = time()
    songplays_table.write.mode('overwrite').parquet(output_data + 'songplays')
    t1 = time() - t0
    print("=== DONE IN: {0:.2f} sec\n".format(t1))
    print('{} records added in the SONGPLAYS file'.format(songplays_table.count()))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-bhavik/sparkify/"
    
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
