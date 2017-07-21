# -*- coding: utf-8 -*-
"""
Created on Fri Jul 14 12:30:28 2017
@author: LuisMDlab
"""
#Scrape facebook IDs from page links on csv.#
import time
import datetime
import csv
from selenium import webdriver

# Énecessario importar o modulo selenium e adicionar o geckodriver ao PATH.
# Geckodriver do Firefox - https://github.com/mozilla/geckodriver/releases
#Documentação selenium - http://selenium-python.readthedocs.io/

identify = []
link = []
cont = 0

arquivoExemplo = open('escolasID.csv')
leitorArquivo = csv.reader(arquivoExemplo)

#Pecorre o csv e adiciona os links das páginas em uma lista.
for row in leitorArquivo:
    link.append(row[0])
# Conecta ao webdriver    
browser = webdriver.Firefox()
print('Iniciando em: ' + str(datetime.datetime.now()))

#Inicia a busca pelas IDs a partir do site: findmyfbid.com
try:
    #Percorre cada ID na lista.
    for i in link:
        browser.set_page_load_timeout(30)
        browser.get('https://findmyfbid.com/')
        
        #Encontra o fomulário para inserção de ID.
        inserirElem = browser.find_element_by_name('url')
        
        #Envia a ID e clica em submit.
        inserirElem.send_keys(i)
        inserirElem.submit()
        
        while browser.current_url == 'https://findmyfbid.com/':
            print('.', end = '.')
            
        identify.append(browser.current_url)
        time.sleep(5)
        cont +=1
        if cont%50==0:
            print(str(cont) + ' IDs recuperados em ' + str(datetime.datetime.now()))
    browser.quit()

    outputFile = open('saveID', 'w', newline='')
    outputWriter = csv.writer(outputFile)
    for row in identify:
        outputWriter.writerow(row)
    outputFile.close()
    
    print(str(cont) + ' IDs recuperados em ' + str(datetime.datetime.now()))

except Exception as e:
    print(e)
    outputFile = open('saveID', 'w', newline='')
    outputWriter = csv.writer(outputFile)
    for row in identify:
        outputWriter.writerow(row)
    outputFile.close() 
    print('Deu erro mas: '+ str(cont) + ' IDs recuperados em ' + str(datetime.datetime.now()))
