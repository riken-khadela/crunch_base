# Import necessary modules
import new_scrapper.settings as cf
import time, os
import requests
from bs4 import BeautifulSoup
import datetime
from datetime import date, timedelta
import random, json, urllib
 
CONFIG_FILE = "config.json"

with open(CONFIG_FILE, "r") as f: config = json.load(f)
    
TOKEN = config["token_1"]
COOKIE_FILE = "session_data.json"
OUTPUT_DIR = "results"

# Define a function to get the html content of search results from Google 
# for a particular search query and page number
def get_request(search, page):
    """
    This function sends a request to Google with a given search query and page number
    and returns the response object if successful.

    Args:
    - search (str): the search query to send to Google
    - page (int): the page number of the search results to request

    Returns:
    - tuple: a tuple containing a boolean value indicating whether the request was successful
    and the response object if successful, otherwise False
    """
    print('searching for : '+search+' and page :' +str(page))
    proxies=cf.proxies()
    isData=True
    while isData:
        try:
            getHeaders = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
            }
            
            #Google request to get html body of the search
            queryParameters = {"q": str(search), "start": page}
            
            url = f"https://www.google.com/search?q={urllib.parse.quote_plus(search)}"
            url = f"http://api.scrape.do/?token={TOKEN}&url={url}&render=true"
            res = requests.get(url)
            # https://www.google.com/search?q=loreal+site%3Awww.crunchbase.com%2Forganization%2F&oq=lo&gs_lcrp=EgZjaHJvbWUqCAgAEEUYJxg7MggIABBFGCcYOzIMCAEQABhDGIAEGIoFMgYIAhBFGDkyBggDEEUYPDIGCAQQRRg8MgYIBRBFGDwyBggGEEUYPDIGCAcQRRg80gEIMTAzOWowajeoAgCwAgA&sourceid=chrome&ie=UTF-8
            
            
            # res = requests.get("https://www.google.com/search", params=queryParameters, headers=getHeaders, timeout=20,proxies=proxies)
            # res = requests.get("https://www.google.com/search", params=queryParameters, headers=getHeaders, timeout=20)
            print(res.status_code)
            
            if res.status_code== 200:
                #Return Response with status True
                return True, res
            
            print(res.status_code)
            
        except Exception as e:
            print(e)
        time.sleep(0.1)
        print("Retrying again for:" + str(search))
        
    return False, False

def collect_links_and_store(res, key,search,page):
    """
    This function collects links from the given search results and stores them in the database.

    Args:
    - res (requests.models.Response): the response object containing the search results
    - key (dict): a dictionary containing information about the search keywords
    - search (str): the search query used to obtain the search results
    - operator (str): the search operator used to obtain the search results
    - page (int): the page number of the search results

    Returns:
    - list: a list of dictionaries containing the links to be stored in the database
    """
    insert_links = []
    current_page = (page / 10) + 1
    try:
        
        soup = BeautifulSoup(res.text, 'lxml')
        links = soup.find_all('div', {'class':'yuRUbf'})
        index_url = 0
        for link in links:
            index_url += 1
            href = link.find('a').get('href')
            if 'https://www.crunchbase.com/organization/' in href:
                test_href=href.split('https://www.crunchbase.com/organization/')[1]
                if '/' not in test_href:
                    obj={}
                    obj['sector'] = ''
                    obj['orgkey'] = ''
                    obj['tag'] = ''
                    if current_page == 1 and index_url == 1:
                        obj['sector'] = key['sector']
                        obj['orgkey'] = key['orgkey']
                        obj['tag'] = key['tag']
                    obj['search_string'] = search
                    obj['url'] = href
                    obj['created_at'] = datetime.datetime.now()
                    obj['is_read'] = 0
                    obj['status'] = 'pending'
                    obj['index'] = index_url
                    obj['google_page'] = current_page
                    obj['count'] = 1
                    obj['update_first'] = 1
                    insert_links.append(obj)
                    print(href)
    except Exception as e:
        print(e)
        pass
    
    if len(insert_links) > 0:
        cf.insert_multiple_urls_from_google(insert_links)
        # print("isnert links " +str(len(insert_links)))
    return insert_links

def collect_page_details():
    """
    This function collects search results from Google for a list of keywords and stores the relevant links in a database.
    """
    # Get the list of keywords to search for from the settings module
    m = random.randrange(2,10)
    time.sleep(m)
    keywords = cf.read_crunch_keywords()
    # For each keyword, search for links across multiple pages of Google search results
    for key in keywords:
        page = 0
        isfound=False
        while True:
            # Construct the search query for this keyword and operator
            search = str(key['orgkey']+ ' site:www.crunchbase.com/organization/')

            # Request the HTML content of the search results from Google
            isdone, res = get_request(search, page)
            if isdone:
                # Collect the links from this page of search results and store them in the database
                insert_links = collect_links_and_store(res, key,search,page)
                if isfound ==False and len(insert_links)==0:
                    obj={
                        'orgkeyword':key['orgkey'],
                        'search_string':search,
                        'created_at': datetime.datetime.now(),
                        
                    }
                    cf.insert_blacklist_keywords(obj)
                    break
                # If there are more pages of search results to process and links were found on this page, move to the next page
                elif len(insert_links) > 0 and page < 20:
                    isfound = True
                    page = page + 10
                else:
                    # If there was an error requesting the search results from Google, move on to the next keyword
                    break
            else:
                break


# if __name__ == "__main__":
#     try:
#         file_path = os.path.dirname(os.path.realpath(__file__))
#         filename = os.path.join(file_path, "crunchlink_status1.txt")
#         f = open(filename, 'r')
#         txt = f.read()
#         f.close()
#         if str(txt) == "0":
#             f = open(filename, 'w')
#             f.write("1")
#             f.close()
#             try:
#                 separator_line =  '-' * 40
#                 print(separator_line)
#                 print("            Starting Scraper1           ")
#                 print(separator_line)
#                 collect_page_details()
#                 print(separator_line)
#                 print("            Stopping Scraper1           ")
#                 print(separator_line)
#             except Exception as e:
#                 print(e)
#                 pass
#             f = open(filename, 'w')
#             f.write("0")
#             f.close()
#         else:
#             print("***** Another process is already running *****")
#     except Exception as e:
#         print(f"Error before running the script: {e}")
