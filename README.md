# Project 4: Sparkify Data Lake
by **Adriano Vilela**

---


## Overview Project Sparkify Data Lake
---

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake.
building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables


## Data Schema
---

The fact and dimension tables have been defined for a schema that optimizes queries and analysis of music playback.

#### Fact Table

- **songplays** - records in log data associated with song plays i.e. records with page NextSong (columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

#### Dimension Tables
- **users** - users in the app (columns: user_id, first_name, last_name, gender, level)

- **songs** - songs in music database (columns: song_id, title, artist_id, year, duration)

- **artists** - artists in music database (columns: artist_id, name, location, lattitude, longitude)

- **time** - timestamps of records in songplays broken down into specific units (columns: start_time, hour, day, week, month, year, weekday)


## AWS S3
---

Buckets were created on AWS S3, to receive files from the Sparkify.



## Python scripts

---
Script execution sequence:
1. **etl.py:** This script uses data in s3:/udacity-dend/song_data and s3:/udacity-dend/log_data, processes it, and inserts the processed data into Data Lake.




### RUN Sequence


**etl.py:**
- Script reads files available on an AWS S3 bucket.
- Generating tables facts and dimensions in another AWS S3 bucket.
- First processes the **process_song_data** script that reads songs_data files and writes the dimension tables `song` and `artists` in the bucket S3.
- Second processes the **process_log_data** script that reads logs_data files and writes the dimension tables `users` and `time` and fact table  `songplays`




## Example queries
---
NOTE: There are some example queries implemented in etl.py and executed in the end of the script run.

1. Get users and songs they listened at particular time. Limit query to 1000 hits:

        SELECT  sp.songplay_id,
                u.user_id,
                s.song_id,
                u.lastName,
                sp.start_time,
                a.artist_name,
                s.title
        FROM songplays AS sp
                JOIN users   AS u ON (u.user_id = sp.user_id)
                JOIN songs   AS s ON (s.song_id = sp.song_id)
                JOIN artists AS a ON (a.artist_id = sp.artist_id)
                JOIN time    AS t ON (t.start_time = sp.start_time)
        ORDER BY (sp.start_time)
        LIMIT 1000;

+-----------+-------+------------------+--------+-------------+-----------+--------------+
|songplay_id|user_id|           song_id|lastName|   start_time|artist_name|         title|
+-----------+-------+------------------+--------+-------------+-----------+--------------+
|          0|     15|SOZCTXZ12AB0182364|    Koch|1542837407796|      Elena|Setanta matins|
|          0|     15|SOZCTXZ12AB0182364|    Koch|1542837407796|      Elena|Setanta matins|
+-----------+-------+------------------+--------+-------------+-----------+--------------+

2. Get count of rows in each Dimension table:

        SELECT COUNT(*)
        FROM songs_table;
=== SONGS_TABLE count: 71

        SELECT COUNT(*)
        FROM artists_table;
=== ARTISTS_TABLE count: 69

        SELECT COUNT(*)
        FROM users_table;
=== USERS_TABLE count: 104

        SELECT COUNT(*)
        FROM time_table;
=== TIME_TABLE count: 6813



3. Get count of rows in Fact table:

        SELECT COUNT(*)
        FROM songplays_table;
=== SONGPLAYS_TABLE count: 1