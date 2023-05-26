import sqlite3

class Item:
    
    def __init__(self, name, asin, listing_url):
        self.name = name
        self.asin = asin
        self.listing_url = listing_url
        
    def insert_into_db(self, dataDirectory):
        database_connection = sqlite3.connect(dataDirectory)
        
        cursor = database_connection.cursor()
        
        insert_statement = "INSERT OR REPLACE INTO item (asin, name, listing_url) VALUES (?, ?, ?)"
        
        # Insert into table
        cursor.execute(insert_statement, (self.asin, self.name, self.listing_url))
        
        database_connection.commit()