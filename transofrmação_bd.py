import csv
import mysql.connector
from mysql.connector import errorcode
from config import config, access_token
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()

cursor.execute('SET NAMES utf8mb4')
cursor.execute("SET CHARACTER SET utf8mb4")
cursor.execute("SET character_set_connection=utf8mb4")
cnx.commit()

cursor.execute ("SELECT codINEP, pageId, nomePagina, sobre, website, email, link, fan_count  FROM profile")
all_profiles = cursor.fetchall()
cnx.commit()

with open('profiles_info.csv', 'w', newline = '', encoding = 'utf-8') as file:
    w = csv.writer(file)
    for (codINEP, pageId, nomePagina, sobre, website, email, link, fan_count) in all_profiles:
        w.writerow([codINEP, pageId, nomePagina, sobre, website, email, link, fan_count])
        
    

