import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
SONG_DATA = config.get('S3', 'SONG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE staging_events(
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INTEGER,
        lastName VARCHAR,
        length DOUBLE PRECISION,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration DOUBLE PRECISION,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts DOUBLE PRECISION,
        userAgent VARCHAR,
        userId INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs INTEGER,
        artist_id  VARCHAR,
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title  VARCHAR,
        duration DOUBLE PRECISION,
        year INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id INTEGER IDENTITY (0,1) PRIMARY KEY,
        start_time TIMESTAMP references time(start_time),
        user_id INTEGER NOT NULL references users(user_id),
        level VARCHAR,
        song_id VARCHAR NOT NULL references songs(song_id),
        artist_id VARCHAR NOT NULL references artists(artist_id),
        session_id INTEGER NOT NULL,
        location VARCHAR,
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id INTEGER PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR references artists(artist_id),
        year INTEGER,
        duration DOUBLE PRECISION
    );
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        location VARCHAR,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION
    );
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    json {} region 'us-west-2';
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto' region 'us-west-2';
""").format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT
        DATEADD('ms', CAST(staging_events.ts as BIGINT),'1970-01-01'),
        staging_events.userId,
        staging_events.level,
        staging_songs.song_id,
        staging_songs.artist_id,
        staging_events.sessionId,
        staging_events.location,
        staging_events.userAgent
    FROM staging_events, staging_songs
    WHERE staging_events.page = 'NextSong'
    AND staging_events.song = staging_songs.title
    AND staging_events.artist = staging_songs.artist_name
    AND staging_events.length = staging_songs.duration;
""")

user_table_insert = ("""
    INSERT INTO users(
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT
        userId,
        firstName,
        lastName,
        gender,
        level
    FROM staging_events
    WHERE userId is NOT null;
""")

song_table_insert = ("""
    INSERT INTO songs(
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT DISTINCT
        DATEADD(
            'ms',
             CAST(staging_events.ts as BIGINT),
             '1970-01-01'
             ) as start_time,
        extract(hour from start_time),
        extract(day from start_time),
        extract(week from start_time),
        extract(month from start_time),
        extract(year from start_time),
        extract(dayofweek from start_time)
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create, user_table_create,
                        artist_table_create, time_table_create,
                        song_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop, songplay_table_drop,
                      user_table_drop, song_table_drop, artist_table_drop,
                      time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, artist_table_insert,
                        song_table_insert, time_table_insert,
                        songplay_table_insert]
