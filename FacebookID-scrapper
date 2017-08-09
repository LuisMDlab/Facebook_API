# -*- coding: utf-8 -*-
"""
Created on Wed Aug  9 08:59:15 2017
@author: LuisMDlab
"""
import time
import datetime
import csv
from selenium import webdriver
import re

#Dicionário de armazenamento das IDs
ident = {'Link':'Facebook ID'}

#Lista auxiliar com links do CSV.
link = []

#Tirar Links do CSV
with open('links.csv', encoding = "utf-8") as csvfile:
    leitorArquivo = csv.reader(csvfile)
    for row in leitorArquivo:
        link.append(row[0])
csvfile.close()

#Inicia o Borwser (Firefox)
#Obs. Necessáiro Instalar o geckodriver: https://github.com/mozilla/geckodriver/releases
browser = webdriver.Firefox()
#<class 'selenium.webdriver.firefox.webdriver.WebDriver'>
cont = 0
print('Iniciando em: ' + str(datetime.datetime.now()))

#Scrape dos IDs a aparitr da URL resultando no site "https://findmyfbid.com/"
try:
    for i in link:
        browser.set_page_load_timeout(30)
        browser.get('https://findmyfbid.com/')
        inserirElem = browser.find_element_by_name('url')
        inserirElem.send_keys(i)
        inserirElem.submit()
        while browser.current_url == 'https://findmyfbid.com/':
            print('.', end = '.')
        resultado = ''.join(re.findall('[0-9]', browser.current_url))
        print(resultado)
        if resultado != '':
            ident[i] = resultado
        else:
            ident[i] = 'Fail'
        time.sleep(5)
        if resultado != '':
            cont +=1
        if cont != 0:
            if cont%10==0:
                print(str(cont) + ' links processados em ' + str(datetime.datetime.now()))
    #Insere Link e ID no CSV chamado "SaveID.csv"
    with open('saveID.csv', 'w', newline= '') as outputFile:
        outputWriter = csv.writer(outputFile)
        for key, value in ident.items():
            outputWriter.writerow([key, value])
    print(str(cont) + ' IDs recuperados em ' + str(datetime.datetime.now()))

#Se der algum erro de conexão salva os IDs já minerados.
except Exception as e:
    print(e)
    with open('saveID.csv', 'w', newline= '') as outputFile:
        outputWriter = csv.writer(outputFile)
        for key, value in ident.items():
            outputWriter.writerow([key, value])
    print('Deu erro mas: '+ str(cont) + ' IDs recuperados em ' + str(datetime.datetime.now()))

#Fecha o Browser
browser.close()
