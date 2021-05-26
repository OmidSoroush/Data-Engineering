import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']


# DROP SCHEMAS
fact_schema_drop= ("DROP SCHEMA IF EXISTS fact_tables CASCADE")
dimension_schema_drop= ("DROP SCHEMA IF EXISTS dimension_tables CASCADE")
staging_schema_drop= ("DROP SCHEMA IF EXISTS staging_tables CASCADE")



# CREATE SCHEMAS
fact_schema= ("CREATE SCHEMA IF NOT EXISTS fact_tables")
dimension_schema= ("CREATE SCHEMA IF NOT EXISTS dimension_tables")
staging_schema= ("CREATE SCHEMA IF NOT EXISTS staging_tables")



# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_tables.events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_tables.songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_tables.songplays"
user_table_drop = "DROP TABLE IF EXISTS dimension_tables.users"
song_table_drop = "DROP TABLE IF EXISTS dimension_tables.songs"
artist_table_drop = "DROP TABLE IF EXISTS dimension_tables.artists"
time_table_drop = "DROP TABLE IF EXISTS dimension_tables.time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_tables.events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR(100),
        gender VARCHAR,
        itemInSession INTEGER,
        lastName VARCHAR(100),
        length NUMERIC,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        sessionId INTEGER SORTKEY DISTKEY,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR,
        userId INTEGER 
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_tables.songs (
        num_songs           INTEGER,
        artist_id           VARCHAR SORTKEY DISTKEY,
        artist_latitude     VARCHAR,
        artist_longitude    VARCHAR,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
        );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS fact_tables.songplays
        (songplay_id    INTEGER IDENTITY(0,1) SORTKEY NOT NULL,
        start_time      TIMESTAMP,
        user_id         INTEGER,
        level           VARCHAR,
        song_id         VARCHAR,
        artist_id       VARCHAR,
        session_id      INTEGER,
        location        VARCHAR,
        user_agent      VARCHAR
        );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimension_tables.users
        (user_id        INTEGER PRIMARY KEY SORTKEY NOT NULL,
        first_name      VARCHAR,
        last_name       VARCHAR,
        gender          VARCHAR,
        level           VARCHAR
        );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimension_tables.songs
        (song_id        VARCHAR PRIMARY KEY SORTKEY NOT NULL,
        title           VARCHAR,
        artist_id       VARCHAR,
        year            INTEGER,
        duration        FLOAT
        );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimension_tables.artists
        (artist_id      VARCHAR PRIMARY KEY SORTKEY NOT NULL,
        name            VARCHAR,
        location        VARCHAR,
        lattitude       VARCHAR,
        longitude       VARCHAR
        );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimension_tables.time
        (start_time     TIMESTAMP PRIMARY KEY SORTKEY NOT NULL,
        hour            INTEGER,
        day             INTEGER,
        week            INTEGER,
        month           INTEGER,
        year            INTEGER,
        weekday         VARCHAR
        );
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_tables.events FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    compupdate off statupdate off
    format as json {};
""").format(LOG_DATA,IAM_ROLE,LOG_JSONPATH)


staging_songs_copy = ("""COPY staging_tables.songs FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    compupdate off statupdate off
    json 'auto'
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO fact_tables.songplays
        (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + e.ts / 1000 * INTERVAL '1 second' AS start_time,
        e.userId,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId,
        e.location,
        e.userAgent
    FROM staging_tables.events AS e
    JOIN staging_tables.songs AS s ON (e.song = s.title AND e.artist = s.artist_name)
    WHERE e.page = 'NextSong'; 
""")

user_table_insert = ("""
    INSERT INTO dimension_tables.users
        (user_id, first_name, last_name, gender, level)
    SELECT e.userId,
        e.firstName,
        e.lastName,
        e.gender,
        e.level
    FROM staging_tables.events AS e
    WHERE e.page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO dimension_tables.songs
        (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_tables.songs;
""")

artist_table_insert = ("""
    INSERT INTO dimension_tables.artists
        (artist_id, name, location, lattitude, longitude)
    SELECT DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    From staging_tables.songs;
""")

time_table_insert = ("""
    INSERT INTO dimension_tables.time
        (start_time, hour, day, week, month, year, weekday)
    SELECT start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(week FROM start_time) AS weekday
    FROM fact_tables.songplays
""")

# QUERY LISTS

drop_schemas_queries = [fact_schema_drop, dimension_schema_drop, staging_schema_drop]
create_schemas_queries = [fact_schema, dimension_schema, staging_schema]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
