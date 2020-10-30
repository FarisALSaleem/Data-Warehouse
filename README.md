# About 

This repository was made for the "Data Warehouse" project by Udacity for their Data Engineer program.

------------------------
# Purpose 

In this project, we will be building a database hosted on Redshift for the startup Sparkify.
Sparkifys has a growing user/song base and wants to move their storage solution to the cloud.
Sparkifys user, song and metadata data resides in a S3 bucket in a JSON format.
Our goals are to:
	- Create a redshift cluster
	- Build ETL pipeline that:
		- Extracts Sparkifys data from S3
		- Stages them in Redshift
		- Transforms data into a set of dimensional tables

------------------------

# Repository Structure

 - dwh.cfg: a configuration that stores DB credentials.
 - create_tables.py: Python code that connects to Redshift and create all the need tables for this project.
 - etl.py: Python code that connects to the Redshift, extracts the data from Sparkifys S3 bucket, stages it redshift and transforms it into dimensionals.
 - sql_queries.py: Python code that contains all the SQL statements used within this project
 - README.md: this file


------------------------

## Table schema 

![Database Diagram](/Database_Diagram.png "Database Diagram")

### Songplays table
- songplay_id INTEGER IDENTITY (0,1) PRIMARY KEY
- start_time TIMESTAMP 
- user_id INTEGER 
- level VARCHAR
- song_id VARCHAR 
- artist_id VARCHAR
- session_id INTEGER
- location VARCHAR
- user_agent VARCHAR

### Users table
- user_id INTEGER PRIMARY KEY
- first_name VARCHAR
- last_name VARCHAR
- gender VARCHAR
- level VARCHAR

### Songs table
- song_id VARCHAR PRIMARY KEY
- title VARCHAR
- artist_id VARCHAR
- year INTEGER
- duration DOUBLE PRECISION

### Artists table
- artist_id VARCHAR PRIMARY KEY
- name VARCHAR
- location VARCHAR
- latitude DOUBLE PRECISION
- longitude DOUBLE PRECISION

### Time table
- start_time TIMESTAMP PRIMARY KEY
- hour INTEGER
- day INTEGER
- week INTEGER
- month INTEGER
- year INTEGER
- weekday INTEGER

------------------------

# Requirements
- A Redshift cluster
- Python3 with the following packages:
	- Psycopg2
	- Configparser

------------------------

# How to Run
The Redshift cluster should be up before running the project.
Enter DB credentials inside of dwh.cfg

```
python create_tables.py
python etl.py
```
