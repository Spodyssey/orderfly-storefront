import csv, datetime, getopt, logging, os, random, requests, sys, sqlite3
from bs4 import BeautifulSoup
from models.item import Item

from models.marketplace import Marketplace

logDirectory = 'resources\\app\\logs\\'
dataDirectory = 'resources\\app\\data\\'
marketplaceDBDirectory = 'resources\\app\\data\\marketplace.db'

# If the logs directory does not exist, create it
if not os.path.exists(logDirectory):
   os.makedirs(logDirectory)

if not os.path.exists(dataDirectory):
   os.makedirs(dataDirectory)

###
#
###
def main(argv):

    database_connection = sqlite3.connect(marketplaceDBDirectory)
    
    # Create database
    createDatabase(database_connection)    
    
    # Set initial values
    logging_level = logging.INFO
    marketplaces = []
    pages = ''
    items = list[Item]

    opts, args = getopt.getopt(argv,"hdm:p:",["debug=","marketplaceIDs=","pages="])

    for opt, arg in opts:
        if opt == '-h':
            print ('main.py --debug -m <marketplaceID>, -p <pages>')
            sys.exit()
        elif opt in ("-d", "--debug"): # Set debug mode for logging
            logging_level = logging.DEBUG
        elif opt in ("-m", "--marketplaceIDs"):
            # Read each marketplace ID value (split by ,) from the command line
            requestedMarketplaceIDs = arg.split(',')
            for marketplaceID in requestedMarketplaceIDs:
                marketplaces.append(Marketplace(marketplaceID))
        elif opt in ("-p", "--pages"):
            # TODO 
            pages = arg

    logging.basicConfig(filename=logDirectory + 'orderfly-storefront.log', encoding='utf-8', level=logging_level, format='[%(levelname)s] %(asctime)s: %(message)s')

    for marketplace in marketplaces:
        print(marketplace.id)
        logging.info(f'Requested marketplace ID: { marketplace.id }')
        marketplace.insert_into_db(marketplaceDBDirectory)
        
        # logging.info(f'Requested page(s): { pages }')
        
        
        # For each item a marketplace has, add it to the corresponding tables
        items = scrapeMarketplaceItems(marketplace, pages)
        for item in items:
            item.insert_into_db(marketplaceDBDirectory)
        
        # # Export results to CSV file
        # # For some reason adding a \\ between storefrontsdirectory and marketplaceID adds an extra \\ to the string...
        # storefrontPath = dataDirectory + marketplace.id + '\\'
        # if not os.path.exists(storefrontPath):
        #     os.makedirs(storefrontPath)

        # csvFileName = datetime.datetime.now().strftime('%Y-%d-%m-%H%M%S') + '.csv'
        # csvFilePath = storefrontPath + csvFileName

        # try:
        #     with open(csvFilePath, 'w', newline='') as csvFile:
        #         fieldnames = [ 'item_name', 'asin', 'listing_url' ]
        #         writer = csv.DictWriter(csvFile, fieldnames=fieldnames)
        #         writer.writeheader()

        #         for item in items:
        #             writer.writerow({
        #                 'item_name': item.name, 
        #                 'asin': item.asin, 
        #                 'listing_url': item.listing_url
        #             })
                    
        # except:
        #     logging.fatal(f'Failed to write items!')

###
#
###
def scrapeMarketplaceItems(marketplace, pages):
    
    # Only first 10 of each is tested (Chrome ^52.0.2762.73 | FireFox ^72.0)
    USER_AGENTS = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.79 Safari/537.36",    # Chrome 104.0.5112.79
        "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",                          # Chrome 104.0.0.0
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",        # Chrome 104.0.0.0
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.53 Safari/537.36",                    # Chrome 103.0.5060.53
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36",     # Chrome 99.0.4844.84
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",           # Chrome 70.0.3538.77
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2919.83 Safari/537.36",             # Chrome 55.0.2919.83
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2866.71 Safari/537.36",      # Chrome 54.0.2866.71
        "Mozilla/5.0 (X11; Ubuntu; Linux i686 on x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2820.59 Safari/537.36",     # Chrome 53.0.2820.59
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2762.73 Safari/537.36",      # Chrome 52.0.2762.73
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2656.18 Safari/537.36",      # Chrome 49.0.2656.18
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/44.0.2403.155 Safari/537.36",                 # Chrome 44.0.2403.155
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",                         # Chrome 41.0.2228.0
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36",      # Chrome 41.0.2227.1
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36",                      # Chrome 41.0.2227.0
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36",                  # Chrome 41.0.2227.0
        "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2226.0 Safari/537.36",                  # Chrome 41.0.2226.0
        "Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36",                  # Chrome 41.0.2225.0
        "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36",                  # Chrome 41.0.2225.0
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2224.3 Safari/537.36",                         # Chrome 41.0.2224.3
        "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.93 Safari/537.36",                       # Chrome 40.0.2214.93
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36",    # Chrome 37.0.2062.124
        "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36",             # Chrome 37.0.2049.0
        "Mozilla/5.0 (Windows NT 4.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36",                  # Chrome 37.0.2049.0
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.67 Safari/537.36",                 # Chrome 36.0.1985.67
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:101.0) Gecko/20100101 Firefox/101.0",                                         # Firefox 101.0
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:99.0) Gecko/20100101 Firefox/99.0",                                               # Firefox 99.0
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:77.0) Gecko/20190101 Firefox/77.0",                                                     # Firefox 77.0
        "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:77.0) Gecko/20100101 Firefox/77.0",                                                    # Firefox 77.0
        "Mozilla/5.0 (X11; Linux ppc64le; rv:75.0) Gecko/20100101 Firefox/75.0",                                                        # Firefox 75.0
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:39.0) Gecko/20100101 Firefox/75.0",                                                     # Firefox 75.0
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.10; rv:75.0) Gecko/20100101 Firefox/75.0",                                        # Firefox 75.0
        "Mozilla/5.0 (X11; Linux; rv:74.0) Gecko/20100101 Firefox/74.0",                                                                # Firefox 74.0
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:61.0) Gecko/20100101 Firefox/73.0",                                           # Firefox 73.0
        "Mozilla/5.0 (X11; OpenBSD i386; rv:72.0) Gecko/20100101 Firefox/72.0"                                                          # Firefox 72.0   
        "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:71.0) Gecko/20100101 Firefox/71.0",                                                     # Firefox 71.0
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:70.0) Gecko/20191022 Firefox/70.0",                                                     # Firefox 70.0
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:70.0) Gecko/20190101 Firefox/70.0",                                                     # Firefox 70.0
        "Mozilla/5.0 (Windows; U; Windows NT 9.1; en-US; rv:12.9.1.11) Gecko/20100821 Firefox/70",                                      # Firefox 70.0
        "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:69.2.1) Gecko/20100101 Firefox/69.2",                                                  # Firefox 69.2
        "Mozilla/5.0 (Windows NT 6.1; rv:68.7) Gecko/20100101 Firefox/68.7",                                                            # Firefox 68.7
        "Mozilla/5.0 (X11; Linux x86_64; rv:54.0) Gecko/20100101 Firefox/64.0",                                                         # Firefox 64.0
        "Mozilla/5.0 (X11; Linux i686; rv:64.0) Gecko/20100101 Firefox/64.0",                                                           # Firefox 64.0
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:64.0) Gecko/20100101 Firefox/64.0",                                                     # Firefox 64.0
        "Mozilla/5.0 (X11; Linux i586; rv:63.0) Gecko/20100101 Firefox/63.0",                                                           # Firefox 63.0
        "Mozilla/5.0 (Windows NT 6.2; WOW64; rv:63.0) Gecko/20100101 Firefox/63.0",                                                     # Firefox 63.0
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.10; rv:62.0) Gecko/20100101 Firefox/62.0",                                        # Firefox 62.0
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:10.0) Gecko/20100101 Firefox/62.0",                                           # Firefox 62.0
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.13; ko; rv:1.9.1b2) Gecko/20081201 Firefox/60.0",                                 # Firefox 60.0
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/58.0.1",                              # Firefox 58.0.1
    ]

    HEADERS = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US, en;q=0.5'
    }

    baseURL = f"https://www.amazon.com/s?&me={ marketplace.id }"

    # Find the number of pages for a marketplace    
    marketplace.find_number_of_pages(baseURL, HEADERS)
    logging.info(f'Found { str(marketplace.number_of_pages) } pages!')

    # For each page
    currentPage = 1
    items: list[Item] = []

    while currentPage < marketplace.number_of_pages + 1:
        HEADERS = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept-Language': 'en-US, en;q=0.5'
        }

        url = baseURL + "&page=" + str(currentPage)
        html = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(html.text, features="html.parser")

        item_elements = soup.find_all('a', {'class': 'a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal'})
        logging.info(f'Found { str(len(item_elements)) } items on page { str(currentPage) }')

        for item_element in item_elements:
            href = item_element.get('href')
            href = href.split('/ref')[0]
            listing_url = 'https://amazon.com' + href.split('`/')[0].split('">')[0]
            item_name = href.split('`/')[0].replace('/', '').split('dp')[0].replace('-', ' ')
            asin = href.split('/dp/')[1].split('/')[0]
            
            items.append(Item(item_name, asin, listing_url))
        
        currentPage += 1
        
    return items

def createDatabase(database_connection):
    
    # Create a cursor object
    cursor = database_connection.cursor()

    # Create MARKETPLACE table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS marketplace (
            id TEXT,
            PRIMARY KEY (id)
        )
    ''')
    
    # Create ITEM table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS item (
            asin TEXT NOT NULL,
            name TEXT, 
            listing_url TEXT,
            PRIMARY KEY (asin)
        )
    ''')

    # Create STORE table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS inventory (
            marketplace_id TEXT,
            item_asin TEXT,
            FOREIGN KEY (marketplace_id) REFERENCES marketplace(id),
            FOREIGN KEY (item_asin) REFERENCES item(asin)
        )
    ''')
    
    # Commit the changes
    database_connection.commit()

    # Close the cursor and the connection
    cursor.close()
    database_connection.close()

if __name__ == "__main__":
   main(sys.argv[1:])