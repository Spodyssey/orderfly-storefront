import sqlite3

class InventoryDAO:
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = sqlite3.connect(db_file)
        self.cursor = self.conn.cursor()
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS inventory (
                id TEXT,
                marketplace_id TEXT,
                item_asin TEXT,
                PRIMARY KEY (id, marketplace_id, item_asin),
                FOREIGN KEY (marketplace_id) REFERENCES marketplace(id),
                FOREIGN KEY (item_asin) REFERENCES item(asin)
            )
        '''
        self.cursor.execute(create_table_query)
        self.conn.commit()
        
    def close(self):
        self.conn.close()

class Inventory:
    def __init__(self, id, items, marketplace_id):
        self.id = id
        self.items = items
        self.marketplace_id = marketplace_id