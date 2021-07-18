

import os
import sqlite3

from urllib.parse import urljoin
from urllib.request import pathname2url



def path_to_url(path):
    return urljoin('file:', pathname2url(path))
          


class SqliteDbCursor:

  def __init__(self, db_file, read_only = False, initializer = None, min_version = None):
    self.db_file     = db_file
    self.db_conn     = None
    self.db_cursor   = None
    self.min_version = min_version  # Minimum version of SQLite that is required
    self.read_only   = read_only
    self.initializer = initializer
    
    
  def __enter__(self):

    if self.min_version is not None:
      sqlite_ver_actual = sqlite3.sqlite_version_info
      
      if len(sqlite_ver_actual) != len(self.min_version):
        raise RuntimeError("Mismatch of version specifier lengths!")
        
      if sqlite_ver_actual < self.min_version:
        raise RuntimeError("Minimum SQLite version requirement not fulfilled!")  
    
    if not os.path.exists(self.db_file):
      if (self.initializer is not None) and (not self.read_only):
        self.initializer(self.db_file)
      
    if not os.path.exists(self.db_file):
      raise RuntimeError("Database file does not exist!")
      
    if self.read_only:
      self.db_conn = sqlite3.connect(path_to_url(self.db_file) + '?mode=ro', uri=True)
    else:
      self.db_conn = sqlite3.connect(self.db_file)
    
    self.db_cursor = self.db_conn.cursor()

    return self.db_cursor
    
    
  def __exit__(self, exc_type, exc_value, tb):
    print("Closing database connection...")
    
    if self.db_conn is not None:
      self.db_conn.commit()
      
    if self.db_cursor is not None:
      self.db_cursor.close()

    print("Done closing database connection!")
  
  
  