import datetime, sqlite3
import logging

class Inventory:

    def __init__(self, id, marketplace_id, items):
        self.id = id
        self.marketplace_id = marketplace_id
        self.items = items

    def insert_into_db(self, dataDirectory):
        try:
            database_connection = sqlite3.connect(dataDirectory)
            cursor = database_connection.cursor()
            insert_statement = "INSERT OR REPLACE INTO inventory (id, marketplace_id, item_asin) VALUES (?, ?, ?)"

            # Insert each item asin for the inventory into the table
            for item in self.items:
                cursor.execute(insert_statement, (self.id, self.marketplace_id, item.asin))

            database_connection.commit()
        except:
            logging.debug(self)
            logging.fatal(f'Failed to insert or replace Inventory record!')