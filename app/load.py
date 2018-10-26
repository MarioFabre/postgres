from datetime import datetime
import csv
import fnmatch
import json
import locale
import os
import pandas as pd
import numpy as np
import numpy
import sys
import fileinput
import psycopg2

def watchDog(sMsg):
    print(sMsg)
    return

def get_date(s_date):
    date_patterns = datetime.strptime(s_date, '%Y-%m-%d')
    return datetime.strftime(date_patterns, '%d/%m/%Y')

# CHANGE INCORRECT OR NULL GUID TO ZERO FORMAT
def config_id(guid):
    return '00000000-0000-0000-0000-000000000000' if (guid == None or len(guid) < 36) else guid.strip()

# CHANGE CODE TO STRING IN POSTGRES TABLE
def contractType(uf):
    switcher = {
        1: "PF",
        2: "PJ"
    }
    return switcher.get(uf, "XX")

# CHANGE CODE TO STRING IN POSTGRES TABLE
def productId(uf):
    switcher = {
        1: "CGI",
        2: "FI"
    }
    return switcher.get(uf, "XX")
    
appConfigFilePath = os.getcwd()+'/app.json'
# Load config files
watchDog('Open JSON Config File: '+appConfigFilePath)
with open(appConfigFilePath) as config_json_file:
    appConfig = json.load(config_json_file)

# set parameters
#filename = sys.argv[1]
inboundFilesPath = appConfig['appFolders']['inputDataFolder']+'/'
outputFilesPath = appConfig['appFolders']['outputDataFolder']+'/'
LogFilesPath = appConfig['appFolders']['logDataFolder']+'/'
errorFilesPath = appConfig['appFolders']['errorDataFolder']+'/'
appLocale = appConfig['locale']

# Set Locale
locale.setlocale(locale.LC_ALL, appLocale)

# read files in input folder
watchDog('Reading Inbound Folder List: '+inboundFilesPath)
processFiles = os.listdir(inboundFilesPath)
processFiles.sort()

conn = psycopg2.connect(
    "host=localhost dbname=postgres user=postgres password=postgres")

curDev = conn.cursor()
curDev.execute('SELECT * FROM "cross_ga";')
gaDescomplicaSC = curDev.fetchall()

curDev = conn.cursor()
curDev.execute('SELECT * FROM "cross_simulator";')
gaDescomplicaSC = curDev.fetchall()

# # CRIAR O CRUZAMENTO DO SIMULADOR SITE/FRANQUIAS COM GA VER SIMULAÇÃO
# curDev = conn.cursor()
# curDev.execute("""  SELECT distinct(O.guid), g.date, g.source_medium, g.channel_group, g.campaign, g.event_type, g.system, 
#                         c.name, c.email, c.age, 
#                         o.realty_value, o.loan_value, o.amortization, o.realty_locale, 'BCREDI/FRANQUIAS', 
#                         o.realty_type, o.contract_type_id, o.inserted_at
#                     FROM opportunities O, contacts C, ga_site_ver_simulacao G 
#                     WHERE length(guid)=36 
#                     AND o.guid = CAST(g.event_id AS VARCHAR) 
#                     AND o.contact_id = c.id 
#                     AND o.guid in
#                         (SELECT CAST(event_id AS VARCHAR)
#                         FROM ga_site_ver_simulacao 
#                         WHERE length(CAST(event_id AS VARCHAR))=36);
#                 ;""")
# cross_ga_site_vs = curDev.fetchall()

# # INSERIR O CRUZAMENTO DO SIMULADOR SITE/FRANQUIAS COM GA VER SIMULAÇÃO
# for row_simulacao in cross_ga_site_vs:
#     cur = conn.cursor()
#     insert = """INSERT INTO SIMULATOR_ANALYTICS 
#                 (EVENT_ID, GA_DATE_SESSION, SOURCE_MEDIUM,
#                 CHANNEL_GROUP, CAMPAIGN, EVENT_TYPE, SYSTEM_GA,
#                 CLIENT_NAME, CLIENT_EMAIL, CLIENT_AGE, REALTY_VALUE,
#                 LOAN_VALUE, AMORTIZATION, REALTY_LOCALE, 
#                 SYSTEM_SIMULATOR, REALTY_TYPE, CONTRACTTYPE, SIMULATOR_DATE)
#             VALUES ('%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
#         row_simulacao[0],
#         row_simulacao[1],
#         row_simulacao[2],
#         row_simulacao[3].lower(),
#         row_simulacao[4],
#         row_simulacao[5],
#         row_simulacao[6],
#         row_simulacao[7].replace("'", "''").title(),
#         row_simulacao[8],
#         row_simulacao[9],
#         row_simulacao[10],
#         row_simulacao[11],
#         row_simulacao[12],
#         row_simulacao[13],
#         row_simulacao[14],
#         row_simulacao[15],
#         row_simulacao[16],
#         row_simulacao[17])
#     cur.execute(insert)

# conn.commit()