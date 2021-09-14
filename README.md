# Data Lake Project

## Overview

*A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.*

In this project we will build an ETL pipeline that extracts their data from the data lake hosted on S3, processes them using Spark which will be deployed on an EMR cluster using AWS, and load the data back into S3 as a set of dimensional tables in parquet format. 

From this tables we will be able to find insights in what songs their users are listening to.

## How to run

*To run this project in local mode*, create a file `dl.cfg` in the root of this project with the following data:

```
KEY=YOUR_AWS_ACCESS_KEY
SECRET=YOUR_AWS_SECRET_KEY
```

Create an S3 Bucket named `sparkify-output` where output results will be stored.

Finally, run the following command:

`python etl.py`

*To run on an Jupyter Notebook powered by an EMR cluster*, import the notebook found in this project.

## Project structure

The files found at this project are the following:

- dl.cfg: File with AWS credentials.
- etl.py: Program that extracts songs and log data from S3, transforms it using Spark, and loads the dimensional tables created in parquet format back to S3.

## ETL pipeline

1. Load credentials
2. Read data from S3
    - Song data: `s3://udacity-dend/song_data`
    - Log data: `s3://udacity-dend/log_data`

    The script reads song_data and load_data from S3.

3. Process data using spark

    Transforms them to create five different tables listed under `Dimension Tables and Fact Table`.
    Each table includes the right columns and data types. Duplicates are addressed where appropriate.

4. Load it back to S3

    Writes them to partitioned parquet files in table directories on S3.

    Each of the five tables are written to parquet files in a separate analytics directory on S3. Each table has its own folder within the directory. Songs table files are partitioned by year and then artist. Time table files are partitioned by year and month. Songplays table files are partitioned by year and month.

### Source Data
- **Song datasets**: all json files are nested in subdirectories under *s3a://udacity-dend/song_data*. A sample of this files is:

```
{
  "song_id": "SOBLFFE12AF72AA5BA",
  "num_songs": 1,
  "title": "Scream",
  "artist_name": "Adelitas Way",
  "artist_latitude": null,
  "year": 2009,
  "duration": 213.9424,
  "artist_id": "ARJNIUY12298900C91",
  "artist_longitude": null,
  "artist_location": ""
}
```

- **Log datasets**: all json files are nested in subdirectories under *s3a://udacity-dend/log_data*. A sample of a single row of each files is:

```
{
  "artist": "Blue October / Imogen Heap",
  "auth": "Logged In",
  "firstName": "Kaylee",
  "gender": "F",
  "itemInSession": 7,
  "lastName": "Summers",
  "length": 241.3971,
  "level": "free",
  "location": "Phoenix-Mesa-Scottsdale, AZ",
  "method": "PUT",
  "page": "NextSong",
  "registration": 1540344794796,
  "sessionId": 139,
  "song": "Congratulations",
  "status": 200,
  "ts": 1541107493796,
  "userAgent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36",
  "userId": 8
}
```

### Dimension Tables and Fact Table

**songplays** - Fact table - records in log data associated with song plays 

**users** - users in the app

**songs** - songs in music database

**artists** - artists in music database

**time** - timestamps of records in songplays broken down into specific units
