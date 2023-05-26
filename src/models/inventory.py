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
                last_updated_date DATE,
                marketplace_id TEXT,
                PRIMARY KEY (id, marketplace_id)
            )
        '''
        self.cursor.execute(create_table_query)
        self.conn.commit()

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
    def __init__(self, id, items, marketplace_id):
        self.id = id
        self.items = items
        self.marketplace_id = marketplace_id