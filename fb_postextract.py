# -*- coding: utf-8 -*-
#Python 3.6.1
#Graph API v.2.9
"""
Created on Fri Jul 14 12:30:28 2017
@author:LuisMDlab
"""
# Extract Facebook Posts Metadata Python Script.#
import json
import datetime
import time
import csv
import requests
import pymysql #Conexão com o MySQL a partir do pymysql, para o Python 3.6.1
from config import config, access_token #Acess Token da GraphAPI está armazenada em outro arquivo, seu formato é: "appid|appsecret".
try:
    from urllib.request import urlopen, Request
except ImportError:
    from urllib2 import urlopen, Request
    
# Importa uma lista de ids presentes em um arquivo csv. (Ver scrapeID para extrair multiplas IDs de links de facebook.
arquivoExemplo = open('listaDeIds.csv')
leitorArquivo = csv.reader(arquivoExemplo)
codINEP = []
link = []
pageId = []

# Armazena as informações da planilha em listas.
for row in leitorArquivo:
    codINEP.append(row[0])
    link.append(row[1])
    pageId.append(row[2])
    
###                                     ###         INÍCIO DE DEFINIÇÕES DO BD              ###                                     ###                                                      
    
# Conexão com o Banco de Dados MySQL, arquivo config contém as informações de conexão.
cnx = pymysql.connect(**config)
cursor = cnx.cursor()

# DROPA a tabela posts caso ela exista, por padrão em comentário.
#cursor.execute ("DROP TABLE IF EXISTS posts")

# O "CHARACTER SET utf8mb4" serve para eviar o erro de encode, repassando a configuração de UTF-8 ao MySQL.
cursor.execute('SET NAMES utf8mb4')
cursor.execute("SET CHARACTER SET utf8mb4")
cursor.execute("SET character_set_connection=utf8mb4")
cnx.commit()

# Define a tabela postas e suas propriedades.
table_name = 'posts'
table = ("CREATE TABLE `posts` ("
         "  `cod` int(11) NOT NULL AUTO_INCREMENT,"
         "  `nomePagina` TEXT CHARACTER SET utf8mb4,"
         "  `codINEP` int (11),"
         "  `pageId` TEXT CHARACTER SET utf8mb4,"
         "  `status_id` TEXT CHARACTER SET utf8mb4," 
         "  `status_message` TEXT CHARACTER SET utf8mb4,"
         "  `link_name` TEXT CHARACTER SET utf8mb4,"
         "  `status_type` TEXT CHARACTER SET utf8mb4,"
         "  `status_link` TEXT CHARACTER SET utf8mb4,"
         "  `status_published` TEXT CHARACTER SET utf8mb4,"
         "  `num_reactions` int(11),"
         "  `num_comments` int(11),"
         "  `num_shares` int(11),"
         "  `num_likes` int(11),"
         "  `num_loves` int(11),"
         "  `num_wows` int(11),"
         "  `num_hahas` int(11),"
         "  `num_sads` int(11),"
         "  `num_angrys` int(11),"
         "  PRIMARY KEY (`cod`)"
         ") ENGINE=InnoDB")

# Função para criar a tabela "posts" no banco de dados e seta os caracteres em "utf-8".
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
# Chama a função de criação da tabela, por padrão sem comentário.        
create_db_table()

#Define a inserção de dados no banco de dados.
add_message = ("INSERT INTO posts "
               "(nomePagina, codINEP, pageId, status_id, status_message, link_name, \
               status_type, status_link, status_published, \
               num_reactions, num_comments, num_shares, num_likes, num_loves, num_wows, num_hahas, num_sads, num_angrys)"
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

# Função para inserção dos dados.
def insert_post(nomePagina, codINEP, pageId, status_id, status_message, link_name, status_type, status_link,
                status_published, num_reactions, num_comments, num_shares, num_likes,
                num_loves, num_wows, num_hahas, num_sads, num_angrys):
      
    cursor.execute(add_message, (nomePagina, codINEP, pageId, status_id, status_message, link_name, status_type, status_link,
                    status_published, num_reactions, num_comments, num_shares, num_likes, num_loves,
                    num_wows, num_hahas, num_sads, num_angrys))
    cnx.commit()
###                                     ###            FIM DE DEFINIÇÕES DO BD               ###                                     ###                                                      

###                                     ###         INÍCIO DAS FUNÇÕES DE EXTRAÇÃO           ###                                     ###                                                      

# Função para resquest da cURL, onde os dados dos posts estarão em json.
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
                      
    return response.read().decode('utf-8') #Usei o .decode('utf-8') porque estava dando erro: "TypeError: the JSON object must be str,
    #not 'bytes'"

# Função para organizar a codificação dos dados em "UTF-8"
def unicode_decode(text):
    try:
        return text.encode('utf-8').decode()
    except UnicodeDecodeError:
        return text.encode('utf-8')

#Função para pegar o feed de posts de uma página, de ons os posts_ids serão recuperados.
def getFacebookPageFeedUrl(base_url):

    # Parte da construção da cURL da GraphAPI, para mais informações ver documentação da API.
    # Campos das reactions
    fields = "&fields=message,link,created_time,type,name,id," + \
        "comments.limit(0).summary(true),shares,reactions" + \
        ".limit(0).summary(true)"

    return base_url + fields

#Função para recuperar as reactions dos posts.
def getReactionsForStatuses(base_url):
    reaction_types = ['like', 'love', 'wow', 'haha', 'sad', 'angry']
    reactions_dict = {}   # dict of {status_id: tuple<6>}
    #Procurar cada reaction listada, adiciona à cURL, baixa os dados em Json, e adiciona ao dicionário. 
    for reaction_type in reaction_types:
        fields = "&fields=reactions.type({}).limit(0).summary(total_count)".format(
            reaction_type.upper())

        url = base_url + fields
        data = json.loads(request_until_succeed(url))['data']
        data_processed = set()  # set() remove duplicações raras nos posts.
        
        #Adiciona o ID do post e a contagem total da reação ao docionário data_processed.
        for status in data:
            id = status['id']
            count = status['reactions']['summary']['total_count']
            data_processed.add((id, count))
        
        #Conta e armazena o total de reactions no dicionário reactions_dict.
        for id, count in data_processed:
            if id in reactions_dict:
                reactions_dict[id] = reactions_dict[id] + (count,)
            else:
                reactions_dict[id] = (count,)

    return reactions_dict

#Função de processamento das informações dos posts.
def processFacebookPageFeedStatus(status):

    #A partir do dicionário "status" são chamadas as chaves.
    #Como algumas informaçõe spodem nãi existir, são checadas pelas condicionais abaixo.
    status_id = status['id']
    status_type = status['type']
    status_message = '' if 'message' not in status else \
        unicode_decode(status['message'])
    link_name = '' if 'name' not in status else \
        unicode_decode(status['name'])
    status_link = '' if 'link' not in status else \
        unicode_decode(status['link'])

    # Cuidados especiais com a formatação da informação de data.
    status_published = datetime.datetime.strptime(
        status['created_time'], '%Y-%m-%dT%H:%M:%S+0000')
    status_published = status_published + \
        datetime.timedelta(hours=-5)  # EST
    status_published = status_published.strftime(
        '%Y-%m-%d %H:%M:%S')
    
    # Como os itens abaixo estão aninhados em json, foram utilizados dicionários em cadeia.
    num_reactions = 0 if 'reactions' not in status else \
        status['reactions']['summary']['total_count']
    num_comments = 0 if 'comments' not in status else \
        status['comments']['summary']['total_count']
    num_shares = 0 if 'shares' not in status else status['shares']['count']

    return (status_id, status_message.encode("utf-8"), link_name.encode("utf-8"), status_type.encode("utf-8"), 
            status_link.encode("utf-8"), status_published, num_reactions, num_comments, num_shares)

# Função para extração das informações das postagens das páginas de Facebook.
def scrapeFacebookPageFeedStatus(page_id, access_token):
    #Como cada vez que uma cURL é chamada, se houver muitas informaçoes elas vem separadas por página, então esse fato,
    #tem de ser verificado, para que todas as informações sejam armazenadas.
    
    has_next_page = True
    num_processed = 0
    scrape_starttime = datetime.datetime.now()
    after = ''
    base = "https://graph.facebook.com/v2.9"
    node = "/{}/posts".format(page_id)
    parameters = "/?limit={}&access_token={}".format(100, access_token)

    print("Scraping {} Facebook Page: {}\n".format(page_id, scrape_starttime))

    while has_next_page:
        after = '' if after is '' else "&after={}".format(after)
        base_url = base + node + parameters + after
        #Chama as funções de conexão com a cURL e os dados em Json
        url = getFacebookPageFeedUrl(base_url)
        statuses = json.loads(request_until_succeed(url))
        reactions = getReactionsForStatuses(base_url)
        
        #Percorre todos os posts de uma FanPage
        for status in statuses['data']:
            #Busca as informações de reações de um Post.
            if 'id' in status:
                    status_data = processFacebookPageFeedStatus(status)
                    reactions_data = reactions[status_data[0]]
                    
                    #Insere as informações recuperadas no banco de dados.
                    insert_post(nomePagina = link[aux], codINEP = codINEP[aux], pageId = pageId[aux], status_id = status_data[0],
                                status_message = status_data[1], link_name = status_data[2], status_type = status_data[3],
                                status_link = status_data[4], status_published = status_data[5],num_reactions = status_data[6],
                                num_comments = status_data[7], num_shares = status_data[8], num_likes = reactions_data[0],
                                num_loves = reactions_data[1], num_wows = reactions_data[2],num_hahas = reactions_data[3],
                                num_sads = reactions_data[4], num_angrys = reactions_data[5])
            time.sleep(0.5)
            num_processed += 1
            print(status_data[0])
            if num_processed % 100 == 0:
                print("{} Statuses Processed: {}".format(num_processed, datetime.datetime.now()))
                time.sleep(1.5)
                                
        # Confere se existe próxima página.
        if 'paging' in statuses:
            after = statuses['paging']['cursors']['after']
        else:
            has_next_page = False
    #Print para acompanhamento das postagens resuperadas.
    print("\nDone!\n{} Statuses Processed in {}".format(num_processed, datetime.datetime.now() - scrape_starttime))
    
###                                     ###         FIM DAS FUNÇÕES DE EXTRAÇÃO           ###                                      ###                                                      

###                                     ###         INÍCIO DA CHAMADA À EXTRAÇÃO           ###                                     ###                                                      

#Identifica se a ID da fanpage é válida e qual o erro se ele existir.
def peneira(page_id, access_token):
    after = ''
    base = "https://graph.facebook.com/v2.10"
    node = "/{}/posts".format(page_id)
    parameters = "/?limit={}&access_token={}".format(100, access_token)
    base_url = base + node + parameters + after
    url = getFacebookPageFeedUrl(base_url)
    r = requests.get(url)
    estado = r.json()
    if "error" in estado:
        resultado = estado["error"]["message"]
    elif estado["data"] == []:
        resultado = "Sem Dados"
    else:
        resultado = True
    return resultado
i = 0
aux = 0

#Percorre as IDs armazenadas nas listas criadas do csv, e aciona as funções de extração para cada uma.
for i in range(len(pageId)):
    print('Iniciando o Scrape do: {}'.format(pageId[i]))
    #Pular as páginas invalidas.
    if peneira(pageId[i], access_token) != True:
        #Insere informações nulas no banco de dados.
        insert_post(linkPagina = link[aux], codINEP = codINEP[aux], pageId = pageId[aux], status_id = 'Nulo',
                    status_message = peneira(pageId[i], access_token),link_name = 'Nulo', status_type = 'Nulo',
                    status_link = 'Nulo', status_published = 'Nulo',
                    num_reactions = 0, num_comments = 0, num_shares = 0, num_likes = 0, num_loves = 0,
                    num_wows = 0, num_hahas = 0, num_sads = 0, num_angrys = 0)
        i +=1
    elif __name__ == '__main__':
        #Se o ID for válido, extrair informações dos posts dessas páginas.
        scrapeFacebookPageFeedStatus(pageId[i], access_token)
    aux += 1
###                                     ###                 FIM DO SCRIPT                   ###                                     ###                                                          
'''Este script foi desenvolvido com base em outros dois scripts e adaptado às necessidades do meu mestrado. Os dois 
scripts-base podem ser acompanhados abaixo:

Script Principal - https://github.com/minimaxir/facebook-page-post-scraper
Script Secundário - https://github.com/jdzorz/facebook-graph-save-mysql/blob/master/main.py#L49 '''
