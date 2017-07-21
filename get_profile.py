# -*- coding: utf-8 -*-
import json
import datetime
import csv
import time
import mysql.connector
from mysql.connector import errorcode
from config import config, access_token

arquivoExemplo = open('listaDeIds.csv')
leitorArquivo = csv.reader(arquivoExemplo)
codINEP = []
link = []
pageId = []

app_id = "226997921088693"
app_secret = "24a5f7ac0b1620b0b59145fba97d5125"
access_token = app_id + "|" + app_secret

for row in leitorArquivo:
    codINEP.append(row[0])
    link.append(row[1])
    pageId.append(row[2])

try:
    from urllib.request import urlopen, Request
except ImportError:
    from urllib2 import urlopen, Request

def request_until_succeed(url):
    req = Request(url)
    success = False
    while success is False:
        try:
            response = urlopen(req)
            if response.getcode() == 200:
                success = True
        except Exception as e:
            print(e)
            time.sleep(5)

            print("Error for URL {}: {}".format(url, datetime.datetime.now()))
            print("Retrying.")

    return response.read().decode('utf-8') #Usei o .decode('utf-8') porque estava dando erro: "TypeError: the JSON object must be str, not 'bytes'"

cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()

#cursor.execute ("DROP TABLE IF EXISTS profile")
cursor.execute('SET NAMES utf8mb4')
cursor.execute("SET CHARACTER SET utf8mb4")
cursor.execute("SET character_set_connection=utf8mb4")
cnx.commit()

# O "CHARACTER SET utf8mb4" serve para eviar o erro de encode, repassando a configuração de UTF-8 ao MySQL.
table_name = 'profile'
table = ("CREATE TABLE `profile` ("
         "  `codINEP` int(11) NOT NULL,"
         "  `pageId` TEXT CHARACTER SET utf8mb4,"
         "  `nomePagina` TEXT CHARACTER SET utf8mb4,"
         "  `sobre` TEXT CHARACTER SET utf8mb4,"
         "  `website` TEXT CHARACTER SET utf8mb4,"
         "  `email` TEXT CHARACTER SET utf8mb4,"
         "  `link` TEXT CHARACTER SET utf8mb4,"
         "  `fan_count` TEXT CHARACTER SET utf8mb4,"
         "  PRIMARY KEY (`codINEP`)"
         ") ENGINE=InnoDB")

def create_db_table():
    cursor.execute('SHOW DATABASES;')
    all_dbs = cursor.fetchall()
    if all_dbs.count((config['database'],)) == 0:
        print("Creating db")
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(config['database']))

    cursor.execute('USE %s;' % config['database'])
    cursor.execute("SHOW TABLES;")
    all_tables = cursor.fetchall()
    if all_tables.count(('pages',)) == 0:
        print("creating table")
        cursor.execute(table)

add_message = ("INSERT INTO profile "
               "(codINEP, pageId, nomePagina, sobre, website, email, link, fan_count)"
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")

def insert_post(codINEP, pageId, page_name, page_about, page_website, page_email, page_link, fan_count):
    
    cursor.execute(add_message,(codINEP, pageId, page_name, page_about, page_website, page_email, page_link, fan_count))
    cnx.commit()

def getFacebookPageFeedUrl(base_url):

    # Construct the URL string; see http://stackoverflow.com/a/37239851 for
    # Reactions parameters
    fields = "?fields=name,about,website,emails,link,fan_count"
    parameters = "&access_token={}".format(access_token)

    return base_url + fields + parameters

def processFacebookPageFeedStatus(statuses):

    # The status is now a Python dictionary, so for top-level items,
    # we can simply call the key.

    # Additionally, some items may not always exist,
    # so must check for existence first
    page_name = 'None' if 'name' not in statuses.keys() else statuses['name']
    page_about = 'None' if 'about' not in statuses.keys() else statuses['about']
    page_website = 'None' if 'website' not in statuses.keys() else statuses['website']
    page_email = 'None' if 'emails' not in statuses.keys() else statuses['emails']
    page_link = 'None' if 'link' not in statuses.keys() else statuses['link']
    fan_count = 0 if 'fan_count' not in statuses.keys() else statuses['fan_count']
    
    return (page_name,page_about,page_website,page_email, page_link, fan_count)

def scrapeFacebookPageFeedStatus(page_id, access_token):
    has_next_page = True
    scrape_starttime = datetime.datetime.now()
    after = ''
    base = "https://graph.facebook.com/v2.9"
    node = "/{}".format(page_id)
    
    print("Scraping {} Facebook Page: {}\n".format(page_id, scrape_starttime))
    base_url = base + node + after

    url = getFacebookPageFeedUrl(base_url)
    statuses = json.loads(request_until_succeed(url))
        
    status_data = processFacebookPageFeedStatus(statuses)
    insert_post(codINEP = int(codINEP[aux]), pageId = str(pageId[aux]), page_name = str(status_data[0]), page_about = str(status_data[1]),
                page_website = str(status_data[2]), page_email = str(status_data[3]), page_link = str(status_data[4]), fan_count = int(status_data[5]))
    
#create_db_table()

def identificar(page_id, access_token):
    cont = 0
    after = ''
    base = "https://graph.facebook.com/v2.9"
    node = "/{}".format(page_id)
    base_url = base + node + after

    url = getFacebookPageFeedUrl(base_url)
    
    req = Request(url)

    success = False
    while success is False:
        try:
            resposta = urlopen(req)
            if resposta.getcode() == 200:
                success = True
        except Exception as e:
            print(e)
            time.sleep(5)
            cont+= 1
            print("Error for URL {}: {}".format(url, datetime.datetime.now()))
            print("Retrying.")
            if cont > 2:
                return 'Null'
            


aux = 0

for i in range(len(pageId)):
    if identificar(pageId[i], access_token) == 'Null':   #Pular as páginas offline
        i +=1
        time.sleep(2)
        insert_post(codINEP = int(codINEP[aux]), pageId = str(pageId[aux]), page_name = 'Fail', page_about = 'Fail',
                page_website = 'Fail', page_email = 'Fail', page_link = 'Fail', fan_count = 0)
        
    elif __name__ == '__main__':
        scrapeFacebookPageFeedStatus(pageId[i], access_token)
        print("\nDone!\n{} Profiless Processed in {}".format(aux, datetime.datetime.now()))
        
    aux += 1
