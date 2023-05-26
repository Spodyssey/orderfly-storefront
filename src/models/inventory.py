import datetime
import sqlite3, uuid

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
                last_updated_date DATE,
                marketplace_id TEXT,
                uuid TEXT,
                PRIMARY KEY (id, marketplace_id)
            )
        '''
        self.cursor.execute(create_table_query)
        self.conn.commit()

    ## TODO - HANDLE CONFLICTS
    def create(self, inventory):
        insert_query = '''
            INSERT INTO inventory (id, last_updated_date, marketplace_id, uuid)
            VALUES (?, ?, ?, ?)
        '''        
        self.cursor.execute(insert_query, (inventory.id, inventory.last_updated_date, inventory.marketplace_id, inventory.uuid))
        self.conn.commit()

    def read(self, item_asin):
        select_query = '''
            SELECT * FROM inventory WHERE id = ? AND marketplace_id = ?
        '''
        self.cursor.execute(select_query, (item_asin,))
        row = self.cursor.fetchone()
        if row:
            id, last_updated_date, marketplace_id, uuid = row
            return Inventory(id, last_updated_date, marketplace_id, uuid)
        else:
            return None

    def update(self, inventory):
        update_query = '''
            UPDATE inventory SET last_updated_date = ? WHERE id = ? AND marketplace_id = ?
        '''
        self.cursor.execute(update_query, (inventory.last_updated_date, inventory.id, inventory.marketplace_id))
        self.conn.commit()

    def delete(self, inventory):
        delete_query = '''
            DELETE FROM inventory WHERE id = ? AND marketplace_id = ?
        '''
        self.cursor.execute(delete_query, (inventory.id, inventory.marketplace_id))
        self.conn.commit()
    
    def close(self):
        self.conn.close()

class Inventory:
    def __init__(self, id, items, last_updated_date, marketplace_id):
        self.id = id
        self.items = items
        self.last_updated_date = last_updated_date
        self.marketplace_id = marketplace_id
        self.uuid = str(uuid.uuid4())