import datetime, sqlite3
import logging

class Item:
    
    def __init__(self, name, asin, listing_url):
        self.name = name
        self.asin = asin
        self.listing_url = listing_url
        
    def insert_into_db(self, dataDirectory):
        try:
            database_connection = sqlite3.connect(dataDirectory)
            cursor = database_connection.cursor()
            insert_statement = "INSERT OR REPLACE INTO item (asin, name, listing_url, last_found_date) VALUES (?, ?, ?, ?)"
            # Insert into table
            cursor.execute(insert_statement, (self.asin, self.name, self.listing_url, datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")))
            database_connection.commit()
        except:
            logging.debug(self)
            logging.fatal(f'Failed to insert or replace Marketplace record!')