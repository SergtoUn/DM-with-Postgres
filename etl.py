import os
import datetime;
import glob
import psycopg2
import pandas as pd
from sql_queries import *

 
    
def process_song_file(cur, filepath):
    """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    # open song file
    try:
        df = pd.read_json(filepath, lines=True)

    # Catching Exceptions
        
    except FileNotFoundError:
        print('Song file not found:' + " " + filepath)
        
    except ParserError:
        print('Parser Error!' + ' ' + filepath)
        
    except Exception as e:
        print('Unknown exception' + ' ' + e)
        
    except BaseException as e:
        print("BaseException:" + " " + e)
    
    except:
        print('Unknown exception found while reading the metadata file {}!'.format(filepath))

    # insert song record
    try:
        song_data = (df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist())[0]
        
    except Exception as e:
        print('Exception while processing song_data found: ' + e)
        
    try:
        cur.execute(song_table_insert, song_data)
        
    except psycopg2.Error as e: 
        print('Exception while inserting the song_data with song_table_insert')
        print (e)
    
    cur.execute("SELECT COUNT (*) FROM songs")
    
    # insert artist record
    try:    
        artist_data = (df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist())[0]
    except Exception as e:
        print('Exception while processing artist_data found: ' + e)
        
    try:
        cur.execute(artist_table_insert, artist_data)
        
    except psycopg2.Error as e:
        print('Exception while inserting the artist_data with artist_table_insert')
        print (e)

    cur.execute("SELECT COUNT(*) FROM artists")

     
    
def process_log_file(cur, filepath):
    """
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts the log information, filters the records by NextSong action, converts the time data 
    in order to store it into the time table. 
    After it the data for the users table is extracted and inserted.
    As a final step the records to the songlays table are inserted accordingly.
    File 'warnings.txt' is created as a log file for the case when no corresponding songs and artists are found in the database and the metadata files.
    
    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the log file
    """
    # open log file
    try:
        df = pd.read_json(filepath, lines=True)
        
    except FileNotFoundError:
        print('Log file not found:' + " " + filepath)
        
    except ParserError:
        print('Parser Error!' + ' ' + filepath)
        
    except Exception as e:
        print('Unknown exception' + ' ' + e)
        
    except BaseException as e:
        print("BaseException:" + " " + e)
    
    except:
        print('Unknown exception found while opening the log file {}!'.format(filepath))

    # filter by NextSong action
    df = df[df.page=='NextSong']

    try:
    # convert timestamp column to datetime
        t = df['ts'] = pd.to_datetime(df['ts'], unit = 'ms')
    
    # insert time data records
        time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.dayofweek]
        column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'DoW']
        time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))
    
    except BaseException as e:
        print("BaseException:" + " " + e)
    
    except:
        print("Unknown exception!")
        
    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
            #conn.commit()
        
        except psycopg2.Error as e:
            print('Exception while inserting the time data with time_table_insert'  + ' ' + e)
            
        except:
            print('Unknown exception while inserting the time data with time_table_insert!')
            

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        #conn.commit()

    # insert songplay records
    
    f = open("warnings.txt", "a")
    
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None
        except psycopg2.Error as e:
            print(e)
            print(row.song + " " + row.artist + " " + str(row.length))
        
        try:
            songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
            cur.execute(songplay_table_insert, songplay_data)
            
            
        except psycopg2.Error as e:
            print('Exception while inserting the songplay records with songplay_table_insert')
            print (e)
            
        except:
            print('Unknown exception found while inserting the songplay records with songplay_table_insert!')    
            
            
        # insert songplay record
                
        if songid is None or artistid is None:
            f.write(str(datetime.datetime.now()) + ' ' + 'Warning: the data is found in logs, but is not available in the metadata files: song "{}", artist "{}"\n'.format(row.song, row.artist))
            
            
        else:
            continue
            
    if os.stat('warnings.txt').st_size != 0: print('Please check warnings.txt file for the warnings!') 
        


def process_data(cur, conn, filepath, func):
    '''
    This function has two parts:
    1. It processes the files according to the mask;
    2. It delivers the files into the functions as per the input variables.

     INPUTS: 
    * cur the cursor variable
    * conn the connection variable to the database
    * filepath the path to the files to be processed
    * func the function applied to the data
'''
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
    
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)
    
    try:
        cur = conn.cursor()
        
    except psycopg2.Error as e:
        print("Error: Could not make cursor")
        print(e)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()