import json, sqlite3

class InventoryItemsDAO:
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = sqlite3.connect(db_file)
        self.cursor = self.conn.cursor()
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS inventory_items (
                inventory_uuid TEXT,
                item_asin TEXT,
                FOREIGN KEY (inventory_uuid) REFERENCES inventory (uuid)
                FOREIGN KEY (item_asin) REFERENCES item (asin)
            )
        '''
        self.cursor.execute(create_table_query)
        self.conn.commit()

    # TODO - HANDLE CONFLICTS
    def create(self, inventory):
        insert_query = '''
            INSERT OR REPLACE INTO inventory_items (inventory_uuid, item_asin)
            VALUES (?, ?)
        '''
        for item in inventory.items:
            self.cursor.execute(insert_query, (inventory.uuid, item.asin))
        self.conn.commit()

    def read(self, inventory_uuid):
        select_query = '''
            SELECT * FROM inventory_items WHERE inventory_uuid = ?
        '''
        self.cursor.execute(select_query, (inventory_uuid,))
        rows = self.cursor.fetchall()
        
        inventory_items = []
        # TODO
        if rows:
            for row in rows:
                inventory_uuid, item_asin = row
                inventory_items.append(InventoryItem(inventory_uuid, item_asin))    
            return inventory_items
        else:
            return None
            
    # TODO? - I don't think this is needed
    # def update(self, inventory):
    #     update_query = '''
    #         UPDATE inventory_items SET last_updated_date = ? WHERE id = ? AND marketplace_id = ?
    #     '''
    #     self.cursor.execute(update_query, (inventory.last_updated_date, inventory.id, inventory.marketplace_id))
    #     self.conn.commit()

    def delete(self, inventory):
        delete_query = '''
            DELETE FROM inventory_items WHERE inventory_uuid = ?
        '''
        self.cursor.execute(delete_query, (inventory.uuid,))
        self.conn.commit()
    
    def close(self):
        self.conn.close()
        
        
class InventoryItem:
    def __init__(self, inventory_uuid, item_asin):
        self.inventory_uuid = inventory_uuid
        self.item_asin = item_asin
        
    def __str__(self):
        return json.dumps(self.__dict__)