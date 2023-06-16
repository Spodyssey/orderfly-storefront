import logging, sqlite3
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
                name TEXT,
                listing_url TEXT,
                first_seen DATE,
                last_seen DATE,
                PRIMARY KEY (asin)
            )
        '''
        self.cursor.execute(create_table_query)
        self.conn.commit()

    def read(self, item):
        logging.info(f'Searching for an ITEM record for ASIN: { item.asin }!')
        try:
            self.cursor.execute("SELECT * FROM item WHERE asin = ?", (item.asin,))
        except Exception as exception:
            logging.warning(f'Failed to find ITEM record for ASIN: { item.asin }!')
            logging.exception(exception)
        
        row = self.cursor.fetchone()
        if row:
            asin, name, listing_url, first_seen, last_seen = row
            return Item(asin, name, listing_url, first_seen, last_seen)
        else:
            return None

    def insert(self, item):
        logging.info(f'Inserting an ITEM record for ASIN: { item.asin }!')
        try:
            self.cursor.execute("INSERT INTO item VALUES (?, ?, ?, ?, ?)",
                (item.asin, item.name, item.listing_url, item.first_seen, item.last_seen))
        except Exception as exception:
            logging.warning(f'Failed to insert an ITEM record for ASIN: { item.asin }!')
            logging.exception(exception)
            
        self.conn.commit()

    def update(self, item):
        logging.info(f'Updating an ITEM record for ASIN: { item.asin }!')
        try:
            self.cursor.execute("UPDATE item SET name = ?, listing_url = ?, last_seen = ? WHERE asin = ?",
                (item.name, item.listing_url, item.last_seen, item.asin))
        except Exception as exception:
            logging.warning(f'Failed to update ITEM record for ASIN: { item.asin }!')
            logging.exception(exception)
            
        self.conn.commit()

    def read_and_update_or_insert(self, item):
        existing_record = self.read(item)
        if existing_record:
            self.update(item)
            return self.read(item)
        else:
            self.insert(item)
            return self.read(item)

    def close(self):
        self.conn.close()

class Item:
    def __init__(self, asin, name, listing_url, first_seen, last_seen):
        self.asin = asin
        self.name = name
        self.listing_url = listing_url
        self.first_seen = first_seen
        self.last_seen = last_seen