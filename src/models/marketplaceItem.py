import json, sqlite3

class MarketplaceItemDAO:
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = sqlite3.connect(db_file)
        self.cursor = self.conn.cursor()
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS marketplace_item (
                marketplace_uuid TEXT,
                item_asin TEXT,
                FOREIGN KEY (marketplace_uuid) REFERENCES marketplace (uuid)
                FOREIGN KEY (item_asin) REFERENCES item (asin)
                PRIMARY KEY (marketplace_uuid,)
            )
        '''
        self.cursor.execute(create_table_query)
        self.conn.commit()

    # TODO - HANDLE CONFLICTS
    def create(self, inventory):
        insert_query = '''
            INSERT OR REPLACE INTO marketplace_item (marketplace_uuid, item_asin)
            VALUES (?, ?)
        '''
        for item in inventory.items:
            self.cursor.execute(insert_query, (inventory.uuid, item.asin))
        self.conn.commit()

    def read(self, marketplace_uuid):
        select_query = '''
            SELECT * FROM marketplace_item WHERE marketplace_uuid = ?
        '''
        self.cursor.execute(select_query, (marketplace_uuid,))
        rows = self.cursor.fetchall()
        
        marketplace_item = []
        # TODO
        if rows:
            for row in rows:
                marketplace_uuid, item_asin = row
                marketplace_item.append(MarketplaceItem(marketplace_uuid, item_asin))    
            return marketplace_item
        else:
            return None
            
    # TODO? - I don't think this is needed
    # def update(self, inventory):
    #     update_query = '''
    #         UPDATE marketplace_item SET last_updated_date = ? WHERE id = ? AND marketplace_id = ?
    #     '''
    #     self.cursor.execute(update_query, (inventory.last_updated_date, inventory.id, inventory.marketplace_id))
    #     self.conn.commit()

    def delete(self, inventory):
        delete_query = '''
            DELETE FROM marketplace_item WHERE marketplace_uuid = ?
        '''
        self.cursor.execute(delete_query, (inventory.uuid,))
        self.conn.commit()
    
    def close(self):
        self.conn.close()

class MarketplaceItem:
    def __init__(self, marketplace_uuid, item_asin):
        self.marketplace_uuid = marketplace_uuid
        self.item_asin = item_asin
        
    def __str__(self):
        return json.dumps(self.__dict__)