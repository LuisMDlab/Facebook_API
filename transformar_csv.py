# -*- coding: utf-8 -*-
import csv
import mysql.connector
from mysql.connector import errorcode
from config import config, access_token

cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()

cursor.execute ("DROP TABLE IF EXISTS escolas")
cursor.execute('SET NAMES utf8mb4')
cursor.execute("SET CHARACTER SET utf8mb4")
cursor.execute("SET character_set_connection=utf8mb4")
cnx.commit()


table_name = 'escolas'

table = ("CREATE TABLE `escolas` ("
         "  `codINEP` int (20) NOT NULL,"
         "  `nome_escola` TEXT CHARACTER SET utf8mb4 NOT NULL,"
         "  `fb_link` TEXT CHARACTER SET utf8mb4 NOT NULL,"
         "  `uf` TEXT CHARACTER SET utf8mb4 NOT NULL," # O "CHARACTER SET utf8mb4" serve para eviar o erro de encode, repassando a configuração de UTF-8 ao MySQL.
         "  `municipio` TEXT CHARACTER SET utf8mb4 NOT NULL,"
         "  `endereco` TEXT CHARACTER SET utf8mb4 NOT NULL,"
         "  `fb_id` TEXT CHARACTER SET utf8mb4 NOT NULL,"
         "  PRIMARY KEY (`codINEP`)"
         ") ENGINE=InnoDB")

cursor.execute(table)
cnx.commit()

add_message = ("INSERT INTO escolas "
               "(codINEP, nome_escola, fb_link, uf, municipio, endereco, fb_id)"
               "VALUES (%s, %s, %s, %s, %s, %s, %s)")

arquivoExemplo = open('ListadeEscolas.csv', encoding = 'utf-8')
leitorArquivo = csv.reader(arquivoExemplo)
codINEP = []

nome_escola = []
fb_link = []
uf = []
municipio = []
endereco = []
fb_id = []

for row in leitorArquivo:
    codINEP.append(row[0])
    nome_escola.append(row[1])
    fb_link.append(row[2])
    uf.append(row[3])
    municipio.append(row[4])
    endereco.append(row[5])
    fb_id.append(row[6])

i = 0    
for r in codINEP:
    cursor.execute(add_message, (codINEP[i] ,nome_escola[i],fb_link[i],uf[i],
                   municipio[i], endereco[i], fb_id[i]))
    i+= 1
    print(i)
    cnx.commit()
