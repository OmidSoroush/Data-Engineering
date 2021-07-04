
# Sparkify Database ETL Project

The purpose of this project ist to create a PostgreSQL database for the company called Sparkify. Specifically, the company wants to put the collected data regarding songs and log-fiels in a SQL database. The project consists data modeling, creating schema and ETL pipeline for the data. 

The data is modeled based on the star schema model which includes a fact table and multiple dimension tables. A star schema modeling technique is used becaused the data is small and structured. 

## Fact Table

### Songplay - includes information about the song play 

> - songplay_id (INT) PRIMARY KEY: ID of song play
> - start_time (DATE): Start time for the user activity
> - user_id (INT): ID of user
> - level (TEXT): User level (free or paid)
> - song_id (TEXT): ID of Song
> - artist_id (TEXT): ID of Artist of the song played
> - session_id (INT): ID of the Session
> - location (TEXT): User location
> - user_agent (TEXT): Agent 

## Dimension Tables

### users - users of the application

> - user_id (INT) PRIMARY KEY: ID of user
> - first_name (TEXT) : Name of user
> - last_name (TEXT) : Last Name of user
> - gender (TEXT): Gender (Female or Male)
> - level (TEXT): User level (free or paid)
> - songs - songs

### songs - includes songs in the database

> - song_id (TEXT) PRIMARY KEY: ID of Song
> - title (TEXT): Title of Song
> - artist_id (TEXT): ID of song Artist
> - year (INT): Year of song release
> - duration (FLOAT): Song duration in milliseconds
> - artists - artists in music database

### artists - information about the artists

> - artist_id (TEXT) PRIMARY KEY: ID of Artist
> - name (TEXT): Name of Artist
> - location (TEXT): Name of Artist city
> - lattitude (FLOAT): Lattitude location of artist
> - longitude (FLOAT): Longitude location of artist
> - time (DATE): information on the date


### time - start of the user activity

> - start_time (DATE) PRIMARY KEY: Timestamp
> - hour (INT): Hour of start_time
> - day (INT): Day of start_time
> - week (INT): Week of year of start_time
> - month (INT): Month of start_time
> - year (INT): Year of start_time
> - weekday (TEXT): Name of week day 


## Steps followed to create the database

1. I first dropped, created and inserted SQL queries 
2. Created the tables in the database
3. used test file to check if the creation of tables was successful
4. used the etl.ipynb file to insert one row of the data in the tables in order to check if everything runs correctly
5. lastly, I used the etl.py file to insert all the data into the tables