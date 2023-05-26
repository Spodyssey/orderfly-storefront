import logging
import sqlite3
from bs4 import BeautifulSoup

import requests


class Marketplace:
    
    def __init__(self, id):
        self.id = id
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
        
    def insert_into_db(self, dataDirectory):
        database_connection = sqlite3.connect(dataDirectory)
        
        cursor = database_connection.cursor()
        
        insert_statement = "INSERT OR REPLACE INTO marketplace (id) VALUES (?)"
        
        # Insert into table
        cursor.execute(insert_statement, (self.id,))
        
        database_connection.commit()