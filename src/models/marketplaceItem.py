import json, logging, sqlite3
from datetime import datetime, timedelta

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
                first_seen DATE,
                last_seen DATE,
                FOREIGN KEY (marketplace_uuid) REFERENCES marketplace (uuid)
                FOREIGN KEY (item_asin) REFERENCES item (asin)
                PRIMARY KEY (marketplace_uuid, item_asin)
            )
        '''
        self.cursor.execute(create_table_query)
        self.conn.commit()

    def read(self, marketplace, item):
        logging.info(f'Searching for a MARKETPLACE_ITEM record for marketplace ID: { marketplace.id } | ASIN: { item.asin }!!')
        try:
            self.cursor.execute("SELECT * FROM marketplace_item WHERE item_asin = ?", (item.asin,))
        except Exception as exception:
            logging.warning(f'Failed to find MARKETPLACE_ITEM record for marketplace ID: { marketplace.id } | ASIN: { item.asin }!!')
            logging.exception(exception)
        
        row = self.cursor.fetchone()
        if row:
            marketplace_uuid, item_asin, first_seen, last_seen = row
            return MarketplaceItem(marketplace_uuid, item_asin, first_seen, last_seen)
        else:
            return None

    def insert(self, marketplace, item):
        logging.info(f'Inserting a MARKETPLACE_ITEM record for marketplace ID: { marketplace.id } | ASIN: { item.asin }!')
        try:
            self.cursor.execute("INSERT INTO marketplace_item (marketplace_uuid, item_asin, first_seen, last_seen) VALUES (?, ?, ?, ?)",
                (marketplace.uuid, item.asin, item.first_seen, item.last_seen))
        except Exception as exception:
            logging.warning(f'Failed to insert an MARKETPLACE_ITEM record for marketplace ID: { marketplace.id } | ASIN: { item.asin }!')
            logging.exception(exception)
            
        self.conn.commit()

    def update(self, marketplace, item):
        logging.info(f'Updating a MARKETPLACE_ITEM record for marketplace ID: { marketplace.id } | ASIN: { item.asin }!')
        try:
            self.cursor.execute("UPDATE marketplace_item SET last_seen_date = ? WHERE marketplace_uuid = ? AND asin = ?",
                (marketplace.uuid, item.asin))
        except Exception as exception:
            logging.warning(f'Failed to update MARKETPLACE_ITEM record for marketplace ID: { marketplace.id } | ASIN: { item.asin }!')
            logging.exception(exception)
            
        self.conn.commit()

    def read_and_update_or_insert(self, marketplace, item):
        existing_record = self.read(marketplace, item)
        if existing_record:
            self.update(marketplace, item)
            return self.read(marketplace, item)
        else:
            self.insert(marketplace, item)
            return self.read(marketplace, item)

    # # TODO - HANDLE CONFLICTS
    # def create(self, marketplace, items):
    #     insert_query = '''
    #         INSERT INTO marketplace_item (marketplace_uuid, item_asin, first_seen, last_seen)
    #         VALUES (?, ?, ?, ?)
    #         ON CONFLICT(marketplace_uuid, item_asin) DO UPDATE SET
    #             last_seen = excluded.last_seen;
    #     '''
    #     current_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    #     for item in items:
    #         self.cursor.execute(insert_query, (marketplace.uuid, item.asin, current_date, current_date))
    #     self.conn.commit()

    # def read(self, marketplace_uuid):
    #     select_query = '''
    #         SELECT * FROM marketplace_item WHERE marketplace_uuid = ?
    #     '''
    #     self.cursor.execute(select_query, (marketplace_uuid,))
    #     rows = self.cursor.fetchall()
        
    #     marketplace_items = []
    #     # TODO
    #     if rows:
    #         for row in rows:
    #             marketplace_uuid, item_asin, first_seen, last_seen = row
    #             marketplace_items.append(MarketplaceItem(marketplace_uuid, item_asin, first_seen, last_seen))    
    #         return marketplace_items
    #     else:
    #         return None
    
    # def read_with_item_info(self, marketplace_uuid):
    #     select_query = '''            
    #         SELECT
    #             item.name,
    #             item.asin,
    #             item.listing_url,
    #             marketplace_item.first_seen,
    #             marketplace_item.last_seen
    #         FROM marketplace_item
    #         JOIN item ON marketplace_item.item_asin = item.asin
    #         WHERE marketplace_item.marketplace_uuid = ?;
    #     '''
    #     self.cursor.execute(select_query, (marketplace_uuid,))
    #     rows = self.cursor.fetchall()
        
    #     marketplace_items = []
    #     # TODO
    #     if rows:
    #         for row in rows:
    #             item_name, item_asin, listing_url, first_seen, last_seen = row
    #             marketplace_items.append(MarketplaceItem(item_name, item_asin, listing_url, first_seen, last_seen))    
    #         return marketplace_items
    #     else:
    #         return None
    
    # # TODO? - I don't think this is needed
    # # def update(self, inventory):
    # #     update_query = '''
    # #         UPDATE marketplace_item SET last_updated_date = ? WHERE id = ? AND marketplace_id = ?
    # #     '''
    # #     self.cursor.execute(update_query, (inventory.last_updated_date, inventory.id, inventory.marketplace_id))
    # #     self.conn.commit()

    # def delete(self, inventory):
    #     delete_query = '''
    #         DELETE FROM marketplace_item WHERE marketplace_uuid = ?
    #     '''
    #     self.cursor.execute(delete_query, (inventory.uuid,))
    #     self.conn.commit()
    
    def clean_week_old(self, marketplace):
        one_week_ago = datetime.now() - timedelta(weeks=1)
        clean_query = '''
            DELETE FROM marketplace_item
            WHERE marketplace_uuid = ? AND last_seen < ?        
        '''        
        self.cursor.execute(clean_query, (marketplace.uuid, one_week_ago))
        self.conn.commit()
    
    def close(self):
        self.conn.close()

class MarketplaceItem:
    def __init__(self, item_name, item_asin, first_seen, last_seen):
        self.item_name = item_name
        self.item_asin = item_asin
        self.first_seen = first_seen
        self.last_seen = last_seen
        
    def __str__(self):
        return json.dumps(self.__dict__)