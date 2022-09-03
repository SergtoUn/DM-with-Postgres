# Project: Data Modeling with Postgres  
## 1. General description
A project Spartkify is interested in perfroming the analysis of the data collected from the clients that listen to the songs.  
Currently all the metadata is distributed among several files in JSON format in multiple directories. Also numerous logs with the data about the users exploiting the services of Sparkify are available.  
This current project addresses the neccessity of the analytics team to possess one single data source for further processing. All the data from the files in data directory can be uploaded into the database according to the schema selected. 

## 2. Schema  
According to the task the structure (schema) of the database has been created.   
The database has been created as per the star schema, fact-dimension design with the following tables:  
- **songs** table contains the data about the songs and has the columns: **song_id** (PK), title, artist_id (FK references artists table on artist_id field), year, duration;
- **artists** table contains the data about the artists and has the following columns: **artist_id** (PK), name, location, latitude, longitude;
- **users** contains the data about the users and has the following columns: **user_id** (PK),  first_name, last_name, gender, level;
- **time** table contains timestamps from the logs with the extended data about the hours, days, weeks, months, years and weekdays of the timestamps, and has the following columns: **start_time** (PK), hour, day, week, month, year, weekday;
- songplay table is the fact table and contains the columns: **songplays** (PK), songplay_id, start_time (FK references time table on start_time field), user_id (FK references users table on user_id field), level, song_id (FK references songs table on songs_id field), artist_id (FK references artists table on artist_id field), session_id, location, user_agent.

The following datatypes were selected upon the examination of the input files (the fields that are not PK accept null values if it is not specially expressed otherwise):
  
**songs:**  
song_id: text,   
title: text,  
artist_id: text (as per the PK in artists table),  
year: int,  
duration: float, not null;  

**artists:**  
artist_id: text,  
name: text, not null,  
location: text,  
latitude: float,  
longitude: float;  

**users:**
user_id: int, 
first_name: text, not null,   
last_name: text, not null,   
gender: text,   
level: text;

**time:**
start_time: timestamp,   
hour: int,  
day: int,  
week: int,  
month: int,  
year: int,  
weekday: text;  
   
**songplays:**
songplay_id: int,   
start_time: timestamp,   
user_id: int,   
level: text,
song_id: text,   
artist_id: text,   
session_id: text, not null,  
location: text,  
user_agent: text.  

NB: songplays table allows nullable values to be insert into artist_id and song_id columns because the logs may (and do) possess the records about the songs and the artists that are not available in the metadata files presented. These records may be beneficial during the analysis. For the 
  
## 3. Updating the Project Template  
In order to warn the client about the exact songs and artists that are unavailable in metadata files additional file "warnings.txt" has been created.  
Also due to the bug in test.ipynb the output for the test on nullables in {songplay_table} has been altered underlining the original problem, please find the changes in the picture file   
![Bug](image6.PNG "Bug")

## 4. Processing the scripts
In order to create the schema please run in Linux terminal:
`python3 create_tables.py`  
Please kindly note that the database created is empty.
The data is processed and loaded with the following command run from the terminal:
`python3 etl.py`
Alternatively both commands can be run in Linux terminal with
`./start.sh`



