## Data Modeling with Postgres
### Schema for Song Play Analysis
Using the song and log datasets to create a star schema optimized for queries on song play analysis. 

#### Step 1: Creating `Fact` and `Dimension` Tables
The SQL quries are written in *sql_queries.ipynb* file that helps to create `Fact` table `Dimension` tables. Next, the database and tables are created using the *create_tables.py* file which inherit from the *sql_queries.ipynb* file

|Fact table|Dimension tables| 
|:-:       |:-:             |
|songplays |   users        |
|          |   songs        |
|          |   artists      |
|          |   time         |

Using the *test.ipynb* file to confirm whether the tables(empty) created are in the database successfully.

#### Step 2: Loading data into individual tables
Using *etl.ipynb* file to read and load the song_data and log_data into individual tables in the database
```
|root directory |
|__data
     |
     |__song_data
             |....
             |....
     |__log_data
             |....
             |....
```
Using again the *test.ipynb* file to confirm whether the tables are created with defined samples in the database successfully.

#### Step 3: ETL pipeline
Finally, after finishing the first two steps, the *etl.py* file is used to transfers data from files in two local directories into each tables in Postgres using Python and SQL.

The similar procedure from step 2 has been followed in *etl.py* file. But instead of working on each table individually, the functions **process_song_file** and **process_log_file** in the *etl.py* has beed modified to extract, transform and load the entire dataset in the tables.