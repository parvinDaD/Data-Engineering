import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This funnction reads informations stored in song_data json files into song and artist tables
    """
    # open song file
    df = pd.read_json(filepath, typ='series', orient='columns').to_frame()

    # insert song record
    song_data = [val[0] for val in df.loc[['song_id', 'title', 'artist_id', 'year', 'duration']].values]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [val[0] for val in df.loc[['artist_id', 'artist_name', 'artist_location','artist_latitude', 'artist_longitude']].values]

    cur.execute(artist_table_insert, artist_data)

def _read_json_files(file):
    """ helper function to digest json records from log files into a dictionary"""
    import json
    output = {}
    c= 0
    with open(file) as f:
        for l in f:
            output.update({c:list(json.loads(l).values())})
            c+=1
    return output

def process_log_file(cur, filepath):
    
    """
    This funnction reads informations stored in log_data json files into time, user and songplay tables
    """
    
    columns=['artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', 'level',  'location', 
         'method', 'page', 'registration', 'sessionId', 'song', 'status', 'ts', 'userAgent', 'userId']
    # open log file
    df = pd.DataFrame.from_dict(_read_json_files(filepath),orient='index', columns=columns)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'])
    
    # insert time data records
    time_data = (df['ts'], t.dt.hour, t.dt.day, t.dt.week, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('timestamp', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels,time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        print(results)
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()