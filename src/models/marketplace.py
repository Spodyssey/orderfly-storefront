import logging, requests, sqlite3
from bs4 import BeautifulSoup

from models.marketplaceItem import MarketplaceItem

class MarketplaceDAO:
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = sqlite3.connect(db_file)
        self.cursor = self.conn.cursor()
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS marketplace (
                id TEXT,
                uuid TEXT,
                last_updated_date DATE,
                PRIMARY KEY (id)
            )
        '''
        self.cursor.execute(create_table_query)
        self.conn.commit()

    def read(self, marketplace):
        logging.info(f'Searching for a MARKETPLACE record for ID: { marketplace.id }!')
        try:
            self.cursor.execute("SELECT * FROM marketplace WHERE id = ?", (marketplace.id,))
        except Exception as exception:
            logging.warning(f'Failed to find MARKETPLACE record for ID: { marketplace.id }!')
            logging.exception(exception)
        
        row = self.cursor.fetchone()
        if row:
            id, uuid, last_updated_date = row
            return Marketplace(id, uuid, last_updated_date)
        else:
            return None

    def insert(self, marketplace):
        logging.info(f'Inserting a MARKETPLACE record for ID: { marketplace.id }!')
        try:
            self.cursor.execute("INSERT INTO marketplace VALUES (?, ?, ?)",
                            (marketplace.id, marketplace.uuid, marketplace.last_updated_date))
        except Exception as exception:
            logging.warning(f'Failed to insert MARKETPLACE record for ID: { marketplace.id }!')
            logging.exception(exception)
            
        self.conn.commit()

    def update(self, marketplace):
        logging.info(f'Updating a marketplace record for ID: { marketplace.id }!')
        try:
            self.cursor.execute("UPDATE marketplace SET last_updated_date = ? WHERE id = ?",
                            (marketplace.last_updated_date, marketplace.id))
        except Exception as exception:
            logging.warning(f'Failed to update MARKETPLACE record for ID: { marketplace.id }!')
            logging.exception(exception)
            
        self.conn.commit()

    def read_and_update_or_insert(self, marketplace):
        existing_record = self.read(marketplace)
        if existing_record:
            self.update(marketplace)
            return self.read(marketplace)
        else:
            self.insert(marketplace)
            return self.read(marketplace)

    def read_active_items(self, marketplace):
        logging.info(f'Searching for active item records for marketplace ID: { marketplace.id }!')
        try:            
            self.cursor.execute('''SELECT
                    asin,
                    name,
                    listing_url,
                    marketplace_item.first_seen,
                    marketplace_item.last_seen
                FROM marketplace
                JOIN marketplace_item ON marketplace.uuid = marketplace_item.marketplace_uuid
                JOIN item ON marketplace_item.item_asin = item.asin
                WHERE marketplace.id = ?''', (marketplace.id,))
        except Exception as exception:
            logging.warning(f'Failed to fetch active item records for marketplace ID: { marketplace.id }!')
            logging.exception(exception)
            
        rows = self.cursor.fetchall()
        marketplace_items = []
        if rows:
            for row in rows:
                item_asin, item_name, listing_url, first_seen, last_seen = row
                marketplace_items.append({
                    "item_name": item_name, 
                    "item_asin": item_asin, 
                    "listing_url": listing_url, 
                    "first_seen": first_seen, 
                    "last_seen": last_seen
                })    
            return marketplace_items
        else:
            return None

    def close(self):
        self.conn.close()

class Marketplace:
    def __init__(self, id, uuid, last_updated_date):
        self.id = id
        self.uuid = uuid
        self.last_updated_date = last_updated_date
        self.number_of_pages = 0
    
    ###################################################################################################
    # 
    # Method that finds the number of pages a marketplace has.
    # 
    # If the paginator element containing the pages for a marketplace does not exist, then it is assumed
    # that there is only a single page. If that paginator element does exist, then determine how many pages
    # a marketplace has based off of how many elements exist within the paginator.
    #
    # If the paginator has a total of 4 child span elements, then the total number of pages is in the last
    # index. In the example below, the following elements are span elements ( | being the separator of the elements):
    # - < Previous
    # - 1
    # - ...
    # - 11
    #
    # EXAMPLE
    # < Previous | 1 | 2 | 3 | ... | 11 | Next >
    #
    ###################################################################################################
    def find_number_of_pages(self, url, request_headers):
        html = requests.get(url, headers=request_headers)
        soup = BeautifulSoup(html.text, features="html.parser")
        
        # Try to find the pagination element that contains the number of pages for a marketplace
        paginationElement = ''
        try:
            paginationElement = soup.find('span', class_='s-pagination-strip')
        except:
            logging.warning(f'Failed to find pagination element!')
            logging.debug(f'Marketplace probably only has one page of items...')

        # Find the span elements within the paginator
        spanElements = []
        try:
            spanElements = paginationElement.find_all('span')
        except:
            logging.warning(f'Failed to retrieve number of pages!')
            logging.debug(f'Marketplace only has one page of items!')

        # Find the number of pages a marketplace has
        totalPages = 0
        if (len(spanElements) == 0): # If 0 span elements are found, only one page exists
            totalPages = 1
        if (len(spanElements) == 4): # If 4 span elements are found, the largest page number is in the last span element
            totalPages = int(spanElements[len(spanElements) - 1].text.strip())
        else: # If any other number of span elements are found, the largest page number is in the second to last span element
            if paginationElement:
                pageLinks = paginationElement.find_all('a')
                totalPages = int(pageLinks[len(pageLinks) - 2].text.strip())

        self.number_of_pages = totalPages