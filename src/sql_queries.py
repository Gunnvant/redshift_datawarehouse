import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "drop table if exists log_data_staging;"
staging_songs_table_drop = "drop table if exists song_data_staging;"
songplay_table_drop = "drop table if exists songplays_fact;"
user_table_drop = "drop table if exists user_dim;"
song_table_drop = "drop table if exists songs_dim;"
artist_table_drop = "drop table if exists artist_dim;"
time_table_drop = "drop table if exists time_dim;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE log_data_staging (    
id int identity(1,1),
artist varchar,
auth varchar,
firstName varchar,
gender varchar,
itemInSession int,
lastName varchar,
length float,
level varchar,
location varchar,
method varchar,
page varchar,
registration varchar,
sessionId int,
song varchar,
status int,
ts bigint,
userAgent varchar,
userId int,
PRIMARY KEY (id));
""")

staging_songs_table_create = ("""
CREATE TABLE song_data_staging (
id int identity(1,1),
artist_id varchar,
artist_latitude double precision,
artist_location varchar,
artist_longitude double precision,
artist_name varchar,
duration float,
num_songs int,
song_id varchar,
title varchar,
year int,
PRIMARY KEY (id)
);
""")

songplay_table_create = ("""
create table songplays_fact(
songplay_id int identity(1,1),
start_time timestamp,
user_id int,
level varchar,
song_id varchar,
artist_id varchar,
session_id int,
location varchar,
user_agent varchar,
primary key (songplay_id),
FOREIGN KEY (user_id) REFERENCES user_dim (user_id),
FOREIGN KEY (artist_id) REFERENCES artist_dim (artist_id), 
FOREIGN KEY (song_id) REFERENCES songs_dim (song_id),
FOREIGN KEY (start_time) REFERENCES time_dim (start_time)
);
""")

user_table_create = ("""
create table user_dim (
user_id int,
first_name varchar,
last_name varchar,
gender varchar,
level varchar,
primary key (user_id)
);
""")

song_table_create = ("""
create table songs_dim (
song_id varchar,
title varchar,
artist_id varchar,
year int,
duration float,
primary key (song_id)
);
""")

artist_table_create = ("""
create table artist_dim (
artist_id varchar,
name varchar,
location varchar,
latitude double precision,
longitude double precision,
primary key (artist_id)
);
""")

time_table_create = ("""
create table time_dim(
start_time timestamp,
hour int,
day int,
week int,
month int,
year int,
weekday int,
primary key (start_time)
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy log_data_staging 
from {}
iam_role {}
json {};
""").format(LOG_DATA,
            ARN,
            LOG_JSONPATH)

staging_songs_copy = ("""
copy song_data_staging 
from {}
iam_role {}
json 'auto';
""").format(SONG_DATA,
            ARN)

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays_fact (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
select timestamp 'epoch'+ts/1000*interval '1 second',
log_data_staging.userid,
log_data_staging.level,
song_data_staging.song_id,
song_data_staging.artist_id,
log_data_staging.sessionid,
log_data_staging."location",
log_data_staging.useragent 
from log_data_staging 
left join song_data_staging  
on log_data_staging.song=song_data_staging.title and log_data_staging.artist = song_data_staging.artist_name 
where log_data_staging.page = 'NextSong';
""")

user_table_insert = ("""
insert into user_dim (user_id,first_name,last_name,gender,level) 
select l0.userid,l0.firstname,l0.lastname,l0.gender,l0.level
from log_data_staging l0
join(
select max(ts) as ts,userid from log_data_staging
where page='NextSong'
group by userid) l1 on l0.userid=l1.userid and l0.ts=l1.ts
order by userid;
""")

song_table_insert = ("""
insert into songs_dim (song_id,title,artist_id,year,duration)
select distinct song_data_staging.song_id,
song_data_staging.title, song_data_staging.artist_id,
song_data_staging.year, song_data_staging.duration from song_data_staging;
""")

artist_table_insert = ("""
insert into artist_dim (artist_id,name,location,latitude,longitude)
select song_data_staging.artist_id,
song_data_staging.artist_name, 
song_data_staging.artist_location,
song_data_staging.artist_latitude,
song_data_staging.artist_longitude
from song_data_staging 
group by song_data_staging.artist_id,
song_data_staging.artist_name,
song_data_staging.artist_location,
song_data_staging.artist_latitude,
song_data_staging.artist_longitude;
""")

time_table_insert = ("""
insert into time_dim(start_time,hour,day,week,month,year,weekday)
select t0.start_time,
extract(hour from t0.start_time) as hour,
extract(day from t0.start_time) as day,
extract(week from t0.start_time) as week,
extract(month from t0.start_time) as month,
extract(year from t0.start_time) as year,
extract(dow from t0.start_time) as weekday
from (select distinct timestamp 'epoch'+ts/1000*interval '1 second' as start_time from log_data_staging
where page='NextSong') t0;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop,songplay_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
