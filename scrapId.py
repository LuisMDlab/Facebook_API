import time
import datetime
import csv
from selenium import webdriver

identify = []
arquivoExemplo = open('escolasID.csv')
leitorArquivo = csv.reader(arquivoExemplo)
link = []
for row in leitorArquivo:
    link.append(row[0])
    
browser = webdriver.Firefox()
#<class 'selenium.webdriver.firefox.webdriver.WebDriver'>
cont = 0
print('Iniciando em: ' + str(datetime.datetime.now()))
try:
    for i in link:
        browser.set_page_load_timeout(30)
        browser.get('https://findmyfbid.com/')
        inserirElem = browser.find_element_by_name('url')
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
