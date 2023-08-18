
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (countDistinct, 
                                   count, 
                                   col, 
                                   desc, 
                                   unix_timestamp,
                                   lead,
                                   when,
                                   lit,
                                   collect_list)

from pyspark.sql import functions as f


DEFAULT_POPULAR_SONG_TRESHOLD =100
DEFAULT_MAX_SESSION_TIME = 20 * 60 # seconds
DEFAULT_TOP_NUMBER_OF_SESSIONS = 100

def load_tsv_file(spark, path, header, schema = None):
    # Load the file
    df_user_musics = spark \
        .read \
        .option("delimiter", "\t") \
        .csv(path, header=header) \
        .toDF(*schema)
    return df_user_musics


# PART A
def user_distinct_songs(df_user_musics):

    # track_id is mostly blank so have taken track-name and artist_id as well
    user_distinct_songs = df_user_musics.groupBy("userid") \
                        .agg(countDistinct("musicbrainz-track-id", "track-name", "musicbrainz-artist-id") \
                        .alias("number_of_distinct_songs")) \
                        .orderBy("userid")
    print(user_distinct_songs.collect())
    output_path = r"C:\playstation\partA"
    write_data_in_tsv_files(user_distinct_songs,output_path)


# PART B
def most_popular_songs(df_user_musics):

    most_popular_songs = df_user_musics \
                        .groupBy("artist-name", "track-name") \
                        .agg(count('*') \
                            .alias("song_freequency")) \
                        .sort(desc("song_freequency")) \
                        .limit(DEFAULT_POPULAR_SONG_TRESHOLD)
    print(most_popular_songs.collect())
    output_path = r"C:\playstation\partB"
    write_data_in_tsv_files(most_popular_songs,output_path)


# PART C
def longest_sessions(df_user_musics):
    

    # convert timestamp feild to datetime
    df_user_musics = df_user_musics.withColumn("timestamp", 
                                            unix_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'")\
                                            .cast("timestamp")
                                            )

    # prepare partitioning
    windowspec = Window().partitionBy("userid").orderBy(col("timestamp").desc())

    # shift time of song by 1 record to get previous song start time
    df_user_musics = df_user_musics.withColumn("previous_song_timestamp", 
                                            lead("timestamp").over(windowspec)
                                            )

    # get the time difference between current song and the previous song
    df_user_musics = df_user_musics.withColumn("time_diff", 
                                            col("timestamp").cast("long") - 
                                            col("previous_song_timestamp").cast("long")
                                            )

    # if time diff is grater than 20 minutes than consider that as a start of new session
    df_user_musics = df_user_musics.withColumn("is_new_session?", col("time_diff") > DEFAULT_MAX_SESSION_TIME)

    # assign session ids to each partition
    sum_func = when(col("is_new_session?"), lit(1)).otherwise(lit(0))
    df_user_musics = df_user_musics.withColumn("session_id", f.sum(sum_func).over(windowspec))

    # get first and last song times and list of all songs within the partition (session)
    df_user_musics = df_user_musics.groupBy("userid", "session_id") \
        .agg(
            f.min("timestamp").alias("first-song-timestamp"),
            f.max("timestamp").alias("last-song-timestamp"),
            collect_list("track-name").alias("list-of-songs"),
        )

    # get duration of a session
    df_user_musics = df_user_musics.withColumn("session-time", 
                                            col("last-song-timestamp").cast("long") - 
                                            col("first-song-timestamp").cast("long")
                                            )

    # filtering top 100 longest sessions
    longest_sessions = df_user_musics \
            .orderBy(col("session-time").desc()) \
            .limit(DEFAULT_TOP_NUMBER_OF_SESSIONS)
    print(longest_sessions.collect())
    output_path = r"C:\playstation\partC"
    write_data_in_tsv_files(longest_sessions,output_path)

def write_data_in_tsv_files(data, output_path):
    data\
    .write \
    .mode("overwrite") \
    .option("delimiter", "\t") \
    .csv(output_path, header=True)


if __name__ == "__main__":

    # Create spark session and assign a name
    spark = SparkSession \
        .builder \
        .appName("Last.fm analysis") \
        .master("local[*]") \
        .config("spark.memory.offHeap.enabled","true") \
        .config("spark.memory.offHeap.size","5g")\
        .getOrCreate()
    

    # Load user music tab seperated file. It appeares this file doesnt have a header. So lets define headers.
    schema = ["userid", "timestamp", "musicbrainz-artist-id", "artist-name", "musicbrainz-track-id", "track-name"]
    usermusicfilepath = r"C:\playstation\data\userid-timestamp-artid-artname-traid-traname.tsv"
    data = load_tsv_file(spark, 
                         path = usermusicfilepath, 
                         header = False, 
                         schema = schema
                         )
    
    # get output result for task A and write data to files
    user_distinct_songs(data)
    

    # get output result for task B and write data to files
    most_popular_songs(data)
    

    # get output result for task C and write data to files
    longest_sessions(data)
    
