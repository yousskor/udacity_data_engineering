import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# Retrieve configuration values from the config file:

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Create staging_events table: Stores raw log data from user activity.
staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist              VARCHAR,
        auth                VARCHAR,
        first_name           VARCHAR,
        gender              CHAR(1),
        itemInSession       INT,
        last_name            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            TEXT,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INT,
        song                VARCHAR,
        status              INT,
        ts                  BIGINT,
        userAgent           TEXT,
        user_id              VARCHAR
    )
""")

# Create staging_songs table: Stores raw song metadata from the music database.
staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_location     TEXT,
        artist_longitude    FLOAT,
        artist_name         VARCHAR,
        duration            FLOAT,
        num_songs           INT,
        song_id             VARCHAR,
        title               VARCHAR,
        year                INT
    )
""")

# Create songplays table: Fact table for song play events, linking users, songs, and artists.
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id         INT         IDENTITY(0,1)   PRIMARY KEY,
        start_time          TIMESTAMP   NOT NULL,
        user_id             VARCHAR     NOT NULL,
        level               VARCHAR,
        song_id             VARCHAR     NOT NULL,
        artist_id           VARCHAR     NOT NULL,
        session_id          INT,
        location            TEXT,
        user_agent          TEXT
    )
""")

# Create users table: Dimension table for user information.
users_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id             VARCHAR     PRIMARY KEY,
        first_name          VARCHAR,
        last_name           VARCHAR,
        gender              CHAR(1),
        level               VARCHAR
    )
""")

# Create songs table: Dimension table for song information.
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id             VARCHAR     PRIMARY KEY,
        title               VARCHAR,
        artist_id           VARCHAR     NOT NULL,
        year                INT,
        duration            FLOAT
    )
""")

# Create artists table: Dimension table for artist information.
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id           VARCHAR     PRIMARY KEY,
        name                VARCHAR,
        location            TEXT,
        latitude            FLOAT,
        longitude           FLOAT
    )    
""")

# Create time table: Dimension table for timestamps of song plays.
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time          TIMESTAMP   PRIMARY KEY,
        hour                INT,
        day                 INT,
        week                INT,
        month               INT,
        year                INT,
        weekday             INT
    )     
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}  
CREDENTIALS 'aws_iam_role={}'  
REGION 'us-west-2'  
FORMAT AS JSON {}  
COMPUPDATE OFF;
""").format(LOG_DATA, ARN, LOG_JSONPATH)


staging_songs_copy = ("""
COPY staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2' 
FORMAT AS JSON 'auto'
COMPUPDATE OFF;
""").format(SONG_DATA, ARN) 

 

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
        se.user_id       AS user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId    AS session_id,
        se.location,
        se.userAgent    AS user_agent
    FROM staging_events se
    JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name
    WHERE se.page = 'NextSong'
""")





user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
            user_id, 
            first_name, 
            last_name, 
            gender, 
            level
    FROM staging_events
    WHERE page = 'NextSong' AND user_id IS NOT NULL

""")






song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT
                song_id, 
                title, 
                artist_id, 
                year, 
                duration
          FROM staging_songs
          WHERE song_id IS NOT NULL
""")





artist_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT
                artist_id,
                artist_name         AS name,
                artist_location     AS location,
                artist_latitude     AS latitude,
                artist_longitude    AS longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL
""")



time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT
                TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
                EXTRACT(hour FROM start_time)       AS hour,
                EXTRACT(day FROM start_time)        AS day,
                EXTRACT(week FROM start_time)       AS week,
                EXTRACT(month FROM start_time)      AS month,
                EXTRACT(year FROM start_time)       AS year,
                EXTRACT(weekday FROM start_time)    AS weekday
        FROM staging_events
        WHERE ts IS NOT NULL
""")




# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, users_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
