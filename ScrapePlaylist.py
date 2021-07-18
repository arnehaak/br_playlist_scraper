#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import datetime
import sqlite3

import bs4
import requests

import dbutil


def write_text_file(filename, content):
  with open(filename, "wb") as f:
    f.write(content.encode("utf-8"))
    
    
def write_binary_file(filename, content):
  with open(filename, "wb") as f:
    f.write(content)
    
    
def get_script_dir():
  return os.path.dirname(os.path.realpath(__file__))


def process_response(db_cursor, session_stats, response):
  # Parse the website which contains the playlist.
  # Return value: Success of the parsing.

  if response.status_code != 200:
    return False

  soup = bs4.BeautifulSoup(response.content, 'html.parser')

  # print(soup.prettify())
  
  the_day = soup.find(name = "li", class_="playlist_navi_head")
  
  # If the page doesn't contain the day, it's most likely because there's no
  # data (too old)
  if the_day is None:
    return False
   
  the_day = the_day.contents[0].strip()
  
  music_info = soup.find(name = "dl", class_="music_research")
  
  # There might be occasions where there is no playlist data for a certain
  # hour (e.g., because no music was played during that hour).
  # This can happen in between, it doesn't mean we already reached the
  # end of the valid data range, so return True here.
  if music_info is None:
    return True
  
  songs = zip(
    music_info.find_all(name = "dt", class_="time"),
    music_info.find_all(name = "dd", class_="audio"))

  for song in songs:
  
    transmission_datetime = the_day + " " + song[0].contents[0].strip()
    
    song_info = song[1].find(name = "li", class_="title").find_all(name="span")
    
    song_artist = song_info[0]
    try:
      song_artist = song_artist.contents[0].strip()
    except IndexError:
      song_artist = "UNKNOWN"
     
    song_title = song_info[1]
    try:
      song_title = song_title.contents[0].strip()
    except IndexError:
      song_title = "UNKNOWN"
    
    
    # TODO: Parse from response
    station = "Bayern 1"
    
    
    # Artist    
    data = db_cursor.execute(
      "SELECT artist_id FROM artists WHERE artist_name = (?)",
      (song_artist,)).fetchall()
    
    if len(data) > 1:
      raise RuntimeError(f'Multiple matches for artist "{song_artist}"!')
    elif len(data) == 1:
      artist_id = data[0][0]
    else:
      artist_id = db_cursor.execute(
        "INSERT INTO artists (artist_name) VALUES (?) RETURNING artist_id;",
        (song_artist, )).fetchone()[0]
      session_stats.num_artists_added += 1
          
    
    # Song
    data = db_cursor.execute(
      "SELECT song_id FROM songs WHERE artist_id = (?) AND song_title = (?)",
      (artist_id, song_title)).fetchall()
    
    if len(data) > 1:
      raise RuntimeError(f'Multiple matches for song "{song_title}" (artist: "{song_artist}")!')
    elif len(data) == 1:
      song_id = data[0][0]
    else:
      song_id = db_cursor.execute(
        "INSERT INTO songs (artist_id, song_title) VALUES (?, ?) RETURNING song_id;",
        (artist_id, song_title)).fetchone()[0]
      session_stats.num_songs_added += 1
      
      
    # Station    
    data = db_cursor.execute(
      "SELECT station_id FROM stations WHERE station_name = (?)",
      (station,)).fetchall()
    
    if len(data) > 1:
      raise RuntimeError(f'Multiple matches for station "{station}"!')
    elif len(data) == 1:
      station_id = data[0][0]
    else:
      station_id = db_cursor.execute(
        "INSERT INTO stations (station_name) VALUES (?) RETURNING station_id;",
        (station, )).fetchone()[0]
      session_stats.num_stations_added += 1
          
      
    # Transmission
    data = db_cursor.execute(
      "SELECT transmission_id FROM transmissions WHERE song_id = (?) AND station_id = (?) AND transmission_datetime = (?)",
      (song_id, station_id, transmission_datetime)).fetchall()
    
    if len(data) > 1:
      raise RuntimeError(f'Multiple identical transmissions found!')
    elif len(data) == 1:
      transmission_id = data[0][0]
    else:
      transmission_id = db_cursor.execute(
        "INSERT INTO transmissions (song_id, station_id, transmission_datetime) VALUES (?, ?, ?) RETURNING transmission_id;",
        (song_id, station_id, transmission_datetime)).fetchone()[0]
      session_stats.num_transmissions_added += 1
    
    
    print(f"{transmission_datetime}: {song_artist} - {song_title}")    
    
  return True
 
  
  
def initialize_db(db_file):

  if os.path.exists(db_file):
    raise RuntimeError("Database already exists, will not attempt to modify it!")
  
  conn = sqlite3.connect(db_file)
  c = conn.cursor()
  
  c.executescript("""

    CREATE TABLE artists
      (
        artist_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        artist_name TEXT,
        UNIQUE (artist_name) ON CONFLICT ABORT
      );

    CREATE TABLE songs
      (
        song_id    INTEGER PRIMARY KEY AUTOINCREMENT,
        artist_id  INTEGER,
        song_title TEXT,
        UNIQUE (artist_id, song_title) ON CONFLICT ABORT,
        FOREIGN KEY (artist_id) REFERENCES artists (artist_id) ON UPDATE RESTRICT ON DELETE RESTRICT 
      );
       
    CREATE TABLE stations
      (
        station_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        station_name TEXT,
        UNIQUE (station_name) ON CONFLICT ABORT 
      );

    CREATE TABLE transmissions
      (
        transmission_id       INTEGER PRIMARY KEY AUTOINCREMENT,
        song_id               INTEGER,
        station_id            INTEGER,
        transmission_datetime TEXT,
        UNIQUE (song_id, station_id, transmission_datetime) ON CONFLICT ABORT,
        FOREIGN KEY (song_id) REFERENCES songs (song_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (station_id) REFERENCES stations (station_id) ON UPDATE RESTRICT ON DELETE RESTRICT
      );
    """)

  conn.commit()
  c.close()
 

  
class SessionStats:

  def __init__(self):
    self.num_artists_added       = 0
    self.num_songs_added         = 0
    self.num_stations_added      = 0
    self.num_transmissions_added = 0
    
  
def main():

  post_url  = 'https://www.br.de/radio/bayern-1/welle110~playlist.html'
  delay_secs = 0

  db_file = os.path.join(get_script_dir(), "song_history.db")
  
  # We require at least SQLite 3.35.0, because we make use of "RETURNING"
  min_version = (3,35,0)
  
  
  with dbutil.SqliteDbCursor(db_file, min_version = min_version, initializer = initialize_db) as db_cursor:
    
    # Start from yesterday (today's data is still incomplete)
    date_curr     = datetime.date.today()  - datetime.timedelta(days = 1)
    date_min      = None
    date_max      = None
    keep_running  = True
    session_stats = SessionStats()
    
    while keep_running:
      for hour in range(23, -1, -1):
        post_data = {"date": date_curr.strftime("%d.%m.%Y"), "hour": f"{hour:d}"}

        response = requests.post(post_url, data = post_data)
                
        try:
          if not process_response(db_cursor, session_stats, response):
            keep_running = False
            break;
            
          if date_max is None:
            date_max = date_curr
          date_min = date_curr
        except:
          # The current content caused issues - dump it to file and abort
          print("Error processing content! Dumping content to file.")
          write_binary_file(os.path.join(get_script_dir(), "troublesome_content.txt"), response.content)
          raise

        
        # Delay the next request, to avoid getting blocked by the server
        time.sleep(delay_secs)
      
      date_curr -= datetime.timedelta(days = 1)
        
    print(f"\nFinished! Processed dates in range {date_min}...{date_max}")
    print(f"  # artists added:       {session_stats.num_artists_added}")
    print(f"  # songs added:         {session_stats.num_songs_added}")
    print(f"  # stations added:      {session_stats.num_stations_added}")
    print(f"  # transmissions added: {session_stats.num_transmissions_added}")
  
    
if __name__ == "__main__":
  main()
