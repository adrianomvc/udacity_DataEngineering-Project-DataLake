
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import  to_timestamp, from_unixtime, unix_timestamp

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
     #Create a spark session using AWS hadoop packages
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    function: use spark to process song json files from S3 source
    input = spark - spark session
            input_data - root AWS S3 bucket
            output_data - AWS S3 bucket and directory to store file
    '''
    
    print("== START: process_song_data ==") 
    
    # Get the song data set from the input_data directory path 's3a://udacity-dend/'     
    # get filepath to song data file
    song_path = input_data + config['JSON']['JSON_SONG_DATA']
    print("==== Getting filepath (song_data): {} ".format(song_path))
      
    
    print("==== Reading SONG_DATA file ")
    # read song data file
    song_data = os.path.join(song_path)  
    df_songs = spark.read.json(song_data)
    
    print("SONG_DATA schema:")
    df_songs.printSchema()
   
   
    #----------------------------------------------------------------------------    
    # extract columns to create songs table using spark SQL
    # get fields: song_id, title, artist_id, year, duration from:
    print("==== Extract columns to create SONGS_TABLE ")
    df_songs.createOrReplaceTempView("song_data")

    songs_table = spark.sql("""
    SELECT DISTINCT
        song_id, 
        title, 
        artist_id, 
        year, 
        duration 
    FROM song_data """)
     
    print("SONGS_TABLE schema:")
    songs_table.printSchema()
    print("SONGS_TABLE examples:")
    songs_table.show(5, truncate=False)

    

    # write songs table to parquet files partitioned by year and artist
    songs_table_path = output_data + "songs_table"
    print("==== Writing SONGS_TABLE parquet files (partitioned by year and artist_id): {}".format(songs_table_path))
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(os.path.join(songs_table_path), "overwrite")

    #----------------------------------------------------------------------------
    # extract columns to create artists table using spark SQL
    # get fields: artist_id, name, location, lattitude, longitude
    print("==== Extract columns to create ARTISTS_TABLE")
    artists_table = spark.sql("""
            SELECT DISTINCT
                artist_id,
                artist_name,
                artist_location,
                artist_latitude, 
                artist_longitude 
            FROM song_data """)   
    
    
    print("ARTISTS_TABLE schema:")
    artists_table.printSchema()
    print("ARTISTS_TABLE examples:")
    artists_table.show(5, truncate=False)
    
    
    # write artists table to parquet files
    artists_table_path = output_data + "artists_table"
    print("==== Writing ARTISTS_TABLE parquet files: {}".format(artists_table_path))
    artists_table.write.parquet(os.path.join(artists_table_path), "overwrite")
    
    print("== END: process_song_data ==")
    
    return songs_table, artists_table


def process_log_data(spark, input_data, output_data):
    
    '''
    function: use spark to process log json files from S3 source
    input = spark - spark session
            input_data - root AWS S3 bucket
            output_data - AWS S3 bucket and directory to store file
    '''
    print("== START: process_log_data ==") 
    
    # get filepath to log data file
    log_path = input_data + config['JSON']['JSON_LOG_DATA']
    print("==== Getting filepath (log_data): {} ".format(log_path))

    # read log data file
    print("==== Reading LOG_DATA file ")
    df_log = spark.read.json(log_path)
    
    
    
    #----------------------------------------------------------------------------
    # filter by actions for song plays
    # get all df_log files associated when page='NextSong'   
    print("==== Filter PAGE: page='NextSong' ")
    df_log_filter = df_log.filter(df_log.page == 'NextSong')
    df_log_filter.createOrReplaceTempView("log_data")
    
    #----------------------------------------------------------------------------
    # extract columns for users table
    # get fields: user_id, first_name, last_name, gender, level
    print("==== Extract columns to create USERS_TABLE'  ")
    users_table = spark.sql("""
                        SELECT DISTINCT 
                            userid As user_id, 
                            firstName,
                            lastName, 
                            gender, 
                            level 
                        FROM log_data
                        """)
    print("USERS_TABLE schema:")
    users_table.printSchema()
    print("USERS_TABLE examples:")
    users_table.show(5, truncate=False)  
    
    # write users table to parquet files
    users_table_path = output_data + "users_table"
    print("==== Writing USERS_TABLE parquet files: {}".format(users_table_path))
    users_table.write.parquet(os.path.join(users_table_path), "overwrite")
    

        
    #----------------------------------------------------------------------------
    # create timestamp column from original timestamp column
    #    get_timestamp = udf(lambda x : datetime.datetime.fromtimestamp(x / 1000.0).date)
    #    get_timestamp = udf(lambda x : datetime.datetime(x/1000).strftime('YYYY-mm-dd %H:%M:%S'))
    #    df = log_data.withColumn("ts" , get_timestamp(log_data.ts))
    #    df = df.withColumn("timestamp" , get_timestamp(df.ts))        
    print("==== Creating timestamp column...  ")
    
    df_log_filter = df_log_filter.withColumn("timestamp", from_unixtime(col("ts")/1000))
    
    
    print("Log_data + timestamp + datetime columns schema:")
    df_log_filter.printSchema()
    print("Log_data + timestamp + datetime columns examples:")
    df_log_filter.show(5)
    
    df_log_filter.createOrReplaceTempView("log_data")

    # extract columns to create time table
    # get fields: start_time, hour, day, week, month, year, weekday
    print("==== Extract columns to create TIME_TABLE")
    time_table = spark.sql("""
    SELECT DISTINCT 
        ts as start_time,
        timestamp,
        cast(date_format(timestamp, "HH") as INTEGER) as hour,
        cast(date_format(timestamp, "dd") as INTEGER) as day,
        weekofyear(timestamp) as week, 
        month(timestamp) as month,
        year(timestamp) as year,
        dayofweek(timestamp) as weekday
    FROM log_data  """)
    
    print("TIME_TABLE schema:")
    time_table.printSchema()
    print("TIME_TABLE examples:")
    time_table.show(5, truncate=False)  

    
    # write time table to parquet files partitioned by year and month
    time_table_path = output_data + "time_table"
    print("==== Writing TIME_TABLE parquet files (partitioned by year and month): {}".format(time_table_path))
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(os.path.join(time_table_path), "overwrite")

    #----------------------------------------------------------------------------
    # extract columns from joined song and log datasets to create songplays table 
    # get fields: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
        
    print("==== Extract columns from joined SONG and LOG datasets to create SONGPLAYS_TABLE ")

    songplays_table = spark.sql("""
                        SELECT DISTINCT 
                            monotonically_increasing_id() As  songplay_id,
                            ts as start_time,
                            month(timestamp) as month,
                            year(timestamp) as year,
                            log_data.userId as user_id,
                            log_data.level,
                            song_data.song_id,
                            song_data.artist_id,
                            log_data.sessionId as session_id,
                            log_data.location,
                            log_data.userAgent as user_agent
                        FROM log_data  
                        JOIN song_data ON 
                            log_data.artist = song_data.artist_name
                            and log_data.song = song_data.title
                            and log_data.length = song_data.duration
                        """)

    print("SONGPLAYS_TABLE schema:")
    songplays_table.printSchema()
    print("SONGPLAYS_TABLE examples:")
    songplays_table.show(5, truncate=False)  
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table_path = output_data + "songplays_table"
    print("==== Writing SONGPLAYS_TABLE parquet files (partitioned by year and month): {}".format(songplays_table_path))
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(os.path.join(songplays_table_path), "overwrite")
    
    print("== END: process_log_data ==")
    
    return users_table, time_table, songplays_table

    
def query_table_count(spark, table):
    """Query example returning row count of the given table.
    Keyword arguments:
    * spark            -- spark session
    * table            -- table to count
    Output:
    * count            -- count of rows in given table
    """
    return table.count()

def query_songplays_table(  spark, \
                            songs_table, \
                            artists_table, \
                            users_table, \
                            time_table, \
                            songplays_table):
    """Query example using all the created tables.
        Provides example set of songplays and who listened them.
    Keyword arguments:
    * spark            -- spark session
    * songs_table      -- songs_table dataframe
    * artists_table    -- artists_table dataframe
    * users_table      -- users_table dataframe
    * time_table       -- time_table dataframe
    * songplays_table  -- songplays_table dataframe
    Output:
    * schema           -- schema of the created dataframe
    * songplays        -- songplays by user (if any)
    """
    df_all_tables_joined = songplays_table.alias('sp')\
        .join(users_table.alias('u'), col('u.user_id') \
                                    == col('sp.user_id'))\
        .join(songs_table.alias('s'), col('s.song_id') \
                                    == col('sp.song_id'))\
        .join(artists_table.alias('a'), col('a.artist_id') \
                                    == col('sp.artist_id'))\
        .join(time_table.alias('t'), col('t.start_time') \
                                    == col('sp.start_time'))\
        .select('sp.songplay_id', 'u.user_id', 's.song_id', 'u.lastName', \
                'sp.start_time', 'a.artist_name', 's.title')\
        .sort('sp.start_time')\
        .limit(100)

    print("\nJoined dataframe schema:")
    df_all_tables_joined.printSchema()
    print("Songplays by users:")
    df_all_tables_joined.show()
    return

def query_examples( spark, \
                    songs_table, \
                    artists_table, \
                    users_table, \
                    time_table, \
                    songplays_table):
    """Query example using all the created tables.
    Keyword arguments:
    * spark            -- spark session
    * songs_table      -- songs_table dataframe
    * artists_table    -- artists_table dataframe
    * users_table      -- users_table dataframe
    * time_table       -- time_table dataframe
    * songplays_table  -- songplays_table dataframe
    Output:
    * schema           -- schema of the created dataframe
    * songplays        -- songplays by user (if any)
    """
    # Query count of rows in the table
    print("=== SONGS_TABLE count: " \
            + str(query_table_count(spark, songs_table)))
    print("=== ARTISTS_TABLE count: " \
            + str(query_table_count(spark, artists_table)))
    print("=== USERS_TABLE count: " \
            + str(query_table_count(spark, users_table)))
    print("=== TIME_TABLE count: " + \
            str(query_table_count(spark, time_table)))
    print("=== SONGPLAYS_TABLE count: " \
            + str(query_table_count(spark, songplays_table)))
    query_songplays_table(  spark, \
                            songs_table, \
                            artists_table, \
                            users_table, \
                            time_table, \
                            songplays_table)
    
    
def main():
    spark = create_spark_session()
    input_data = config['S3']['INPUT_S3']
    output_data =config['S3']['OUTPUT_S3']
    
    songs_table, artists_table               = process_song_data(spark, input_data, output_data)   
    
    users_table, time_table, songplays_table = process_log_data(spark, input_data, output_data)

    print("== Running example queries...")
    query_examples( spark, \
                    songs_table, \
                    artists_table, \
                    users_table, \
                    time_table, \
                    songplays_table)
if __name__ == "__main__":
    main()
