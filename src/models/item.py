import sqlite3
import logging
from datetime import datetime, timedelta

class ItemDAO:
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = sqlite3.connect(db_file)
        self.cursor = self.conn.cursor()
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS item (
                asin TEXT,
                first_seen_date DATE,
                last_seen_date DATE,
                listing_url TEXT,
                name TEXT,
                PRIMARY KEY (asin)
            )
        '''
        self.cursor.execute(create_table_query)
        self.conn.commit()

    def create(self, items):
        insert_query = '''
            INSERT INTO item (asin, first_seen_date, last_seen_date, listing_url, name)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(asin) DO UPDATE SET
                last_seen_date = excluded.last_seen_date
        '''
        
        current_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        for item in items:
            self.cursor.execute(insert_query, (item.asin, current_date, current_date, item.listing_url, item.name))
        self.conn.commit()

    def read(self, item_asin):
        select_query = '''
            SELECT * FROM item WHERE asin = ?
        '''
        self.cursor.execute(select_query, (item_asin,))
        row = self.cursor.fetchone()
        if row:
            asin, first_seen_date, last_seen_date, listing_url, name = row
            return Item(asin, last_seen_date, listing_url, name)
        else:
            return None

    def update(self, item):
        update_query = '''
            UPDATE item SET last_seen_date = ?, listing_url = ?, name = ?, WHERE asin = ?
        '''
        self.cursor.execute(update_query, (item.last_seen_date, item.listing_url, item.name, item.asin))
        self.conn.commit()

    def delete(self, item_asin):
        delete_query = '''
            DELETE FROM item WHERE asin = ?
        '''
        self.cursor.execute(delete_query, (item_asin,))
        self.conn.commit()

    def close(self):
        self.conn.close()

class Item:
    def __init__(self, asin, last_seen_date, listing_url, name):
        self.asin = asin
        self.last_seen_date = last_seen_date
        self.listing_url = listing_url
        self.name = name