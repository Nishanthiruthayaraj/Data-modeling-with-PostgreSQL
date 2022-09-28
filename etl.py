import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Read all the song files and insert the song_data and artist_data into the tables song_table_insert and artist_table_insert respectively
    Parameters:
                cur (psycopg2.connect().cursor): cursor to sparkifydb database
                filepath (str): Filepath of data to to be read
    
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df.loc[0, ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Read all the log files and insert the data into time_table, user_table and songplay_table
    
    Parameters:
                cur (psycopg2.connect().cursor): cursor to sparkifydb database
                filepath (str): Filepath of data to to be read
    
    """
    # open log file
    df = pd.read_json(filepath, lines=True)
    
    # filter by NextSong action
    df = df[df['page'] == 'NextSong'].copy()
    
    # convert timestamp column to datetime
    df["ts"] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (df["ts"], df["ts"].dt.hour, df["ts"].dt.day, df["ts"].dt.isocalendar().week, df["ts"].dt.month, df["ts"].dt.year, df["ts"].dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week of year', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict({column_labels[i]: time_data[i] for i in range(len(time_data))})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender","level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, song_id, artist_id, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        

def process_data(cur, conn, filepath, func):
    """
    Function to read and load data from song_data and log_data into the database  
    Parameters:
                cur (psycopg2.connect().cursor): cursor to sparkifydb database
                conn (psycopg2.connect): connect to sparkifydb database
                filepath (str): path to root directory
                func(name of function): what function to call
    
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=7535")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()