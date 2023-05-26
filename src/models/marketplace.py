import logging, requests, sqlite3
from bs4 import BeautifulSoup

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
                last_updated_date DATE,
                PRIMARY KEY (id)
            )
        '''
        self.cursor.execute(create_table_query)
        self.conn.commit()

    def create(self, marketplace):
        insert_query = '''
            INSERT INTO marketplace (id, last_updated_date)
            VALUES (?, ?)
        '''
        self.cursor.execute(insert_query, (marketplace.id, marketplace.last_updated_date))
        self.conn.commit()

    def read(self, marketplace_id):
        select_query = '''
            SELECT * FROM marketplace WHERE id = ?
        '''
        self.cursor.execute(select_query, (marketplace_id,))
        row = self.cursor.fetchone()
        if row:
            marketplace_id, last_updated_date = row
            return Marketplace(marketplace_id, last_updated_date)
        else:
            return None

    def update(self, marketplace):
        update_query = '''
            UPDATE marketplace SET last_updated_date = ? WHERE id = ?
        '''
        self.cursor.execute(update_query, (marketplace.last_updated_date, marketplace.id))
        self.conn.commit()

    def delete(self, marketplace_id):
        delete_query = '''
            DELETE FROM marketplace WHERE id = ?
        '''
        self.cursor.execute(delete_query, (marketplace_id,))
        self.conn.commit()

    def close(self):
        self.conn.close()

class Marketplace:
    def __init__(self, id, last_updated_date):
        self.id = id
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