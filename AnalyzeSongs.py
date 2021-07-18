#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import datetime

# import pandas as pd

import dbutil



def write_text_file(filename, content):
  with open(filename, "wb") as f:
    f.write(content.encode("utf-8"))
    
    
def get_script_dir():
  return os.path.dirname(os.path.realpath(__file__))



def main():

  post_url  = 'https://www.br.de/radio/bayern-1/welle110~playlist.html'
  delay_secs = 0

  db_file = os.path.join(get_script_dir(), "song_history.db")
  
  with dbutil.SqliteDbCursor(db_file, read_only = True) as db_cursor:
    
    
    #  query = """
    #    SELECT 
    #      artists.artist_name,
    #      songs.song_title
    #    FROM 
    #      songs
    #    INNER JOIN artists 
    #      ON artists.artist_id = songs.artist_id
    #    ORDER BY artists.artist_name;
    #    """
    #  
    #  query = """
    #    SELECT 
    #      transmissions.station_id,
    #      transmissions.transmission_datetime,
    #      songs.song_title
    #    FROM 
    #      transmissions
    #    INNER JOIN songs 
    #      ON transmissions.song_id = songs.song_id;
    #    """
      
    # Transmissions per song
    query = """
      SELECT
        artists.artist_name,
        s.song_title,
        t.cnt AS transmission_count
      FROM
          songs s
          LEFT JOIN
              (SELECT song_id, COUNT(*) AS cnt
              FROM transmissions
              GROUP BY song_id
              ) AS t
              ON s.song_id = t.song_id
          LEFT JOIN artists 
            ON s.artist_id = artists.artist_id
      WHERE t.cnt > 0
      ORDER BY t.cnt;
    """
        
    #  # Songs per artist
    #  query = """
    #    SELECT
    #      a.artist_name,
    #      s.cnt AS song_count
    #    FROM
    #        artists a
    #        LEFT JOIN
    #            (SELECT artist_id, COUNT(*) AS cnt
    #            FROM songs
    #            GROUP BY artist_id
    #            ) AS s
    #            ON a.artist_id = s.artist_id
    #    WHERE s.cnt > 0
    #    ORDER BY s.cnt;
    #  """
    
    songs = []
    for row in db_cursor.execute(query):
      print(row)
      
    print(songs)
    
if __name__ == "__main__":
  main()
