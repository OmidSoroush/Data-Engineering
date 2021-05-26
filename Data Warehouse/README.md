# Introduction 

A startup called Sparkify which is focused on music streaming wants to move to a cloud database. Specifically, the company has data regarding songs details and users' activities which are stored JSON format in Amazon S3 storage. The company wants to be able to perform queries on these data using a cloud data warehousing concept. 

# Project description

Sparkify has currently difficulties querying their data and use them for analytsis to derive insights from the data. In order to make Sparkify able to easily query their data, we are going to proceed as follows: first, we build etl data pipelines to load the data from S3 to staging tables in the redshift. Then, we are going to make a star schema to make fact and dimension tables out of these staging tables and store them in a database in the redshift.

# Datasets

We will be working with two datasets which are stored in S3. The first dataset contains information about each song and the artist. Here is a glimpse of what each JSON file looks like:

**{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}**


The second dataset contains log files in JSON format which stores information about the users' activities. Here is a glimpse of what each log file look like:

**{"artist":"Pavement", "auth":"Logged In", "firstName":"Sylvie", "gender", "F", "itemInSession":0, "lastName":"Cruz", "length":99.16036, "level":"free", "location":"Klamath Falls, OR", "method":"PUT", "page":"NextSong", "registration":"1.541078e+12", "sessionId":345, "song":"Mercy:The Laundromat", "status":200, "ts":1541990258796, "userAgent":"Mozilla/5.0(Macintosh; Intel Mac OS X 10_9_4...)", "userId":10}**

# The schema for the project

A star schema with a fact and four dimension tables will be built to store the data for this project. 

## Fact table

**songplays** - records data associated with song plays with the following columns:

*songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agen*

## Dimension tables

> **users** - user_id, first_name, last_name, gender, level

> **songs** - song_id, title, artist_id, year, duration

> **artists** - artist_id, name, location, lattitude, longitude

> **time** - start_time, hour, day, week, month, year, weekday


# Steps to follow to realize the project

1- Use the sql_queries.py to write SQL queries to drop tables if they already exist and create the tables discussed above plus the two staging tables.
2- Launch a redshift cluster and create an IAM role that has read access to S3.
3- Add redshift database and IAM role info to dwh.cfg.
4- Run create_tables.py - to drop and create tables
5- Run etl.py - Run this python file to create the etl pipeline to insert data into the created tables.
6- Run some basic analytical queries either in jupyter notebook or in redshift query tool to find out whether the data insertion was successful. 

