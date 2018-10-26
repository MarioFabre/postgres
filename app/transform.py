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

# INSERIR O CRUZAMENTO DO SIMULADOR DESCOMPLICA COM GA_DESCOPLICA_SOLIC_CREDITO
for processFile in processFiles:
    if fnmatch.fnmatch(processFile, 'relatorio_personalizado.xls'):
        processFile4 = pd.read_excel(
            inboundFilesPath+processFile, 'Analítico', sep=',', header=1, encoding='latin-1')

# INSERT DATA FROM CRM
for index, row in processFile4.iterrows():
    cur = conn.cursor()
    insert = """INSERT INTO EXPORT_CRM 
                (GUID, EVENTO, STATUS_EVENTO, TIPO_EVENTO, CLIENTE,
                CPF_CNPJ, MOTIVO_RESULTADO, DATA_CONCLUSAO_EVENTO,
                VALOR_CREDITO, VALOR_IMOVEL, EMAIL)
            VALUES ('%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
        config_id(processFile4['GUID'][0]),
        processFile4['EVENTO'][0],
        processFile4['STATUS EVENTO'][0],
        processFile4['TIPO EVENTO'][0],
        processFile4['CLIENTE'][0],
        processFile4['CPF/CNPJ'][0],
        processFile4['MOTIVO RESULTADO'][0],
        processFile4['DATA CONCLUSÃO EVENTO'][0],
        processFile4['VALOR DO CRÉDITO:'][0],
        processFile4['VALOR DO IMÓVEL:'][0],
        processFile4['EMAIL'][0])
    cur.execute(insert)
conn.commit()


cur = conn.cursor()
cur.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')

curDev = conn.cursor()
curDev.execute('SELECT * FROM SIMULACAO;')
simulacaoDesc = curDev.fetchall()

curDev = conn.cursor()
curDev.execute('''  SELECT  o.id, o.guid, o.loan_value, o.realty_value, o.realty_type, o.realty_locale, 
                        o.payment_term, o.amortization, c.name, c.email, c.phone, c.age, o.inserted_at, 
                        o.contract_type_id, product_id, app_id
                    FROM opportunities o, contacts c
                    WHERE o.contact_id = c.id;''')
simulacaoSite = curDev.fetchall()

# INSERT CROSS_SIMULATOR FROM SITE SIMULATOR
for row_simulacao in simulacaoSite:
    cur = conn.cursor()
    insert = """INSERT INTO CROSS_SIMULATOR 
                (SIMULATOR_ID, EVENT_ID, LOAN_VALUE, REALTY_VALUE, REALTY_TYPE,
                REALTY_LOCALE, PAYMENT_TERM, AMORTIZATION, NAME, EMAIL, PHONE,
                AGE, INSERTED_AT, CONTRACTTYPE, PRODUCT_ID, APP_ID)
            VALUES ('%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
            row_simulacao[0],
            config_id(row_simulacao[1]),
            row_simulacao[2],
            row_simulacao[3],
            row_simulacao[4],
            row_simulacao[5],
            row_simulacao[6],
            row_simulacao[7],
            row_simulacao[8].replace("'", "''").title(),
            row_simulacao[9],
            row_simulacao[10],
            row_simulacao[11],
            row_simulacao[12],
            contractType(row_simulacao[13]),
            productId(row_simulacao[14]),
            row_simulacao[15])
    cur.execute(insert)

conn.commit()

# INSERT CROSS_SIMULATOR FROM DESCOMPLICA SIMULATOR
for row_simulacao in simulacaoDesc:
    cur = conn.cursor()
    insert = """INSERT INTO CROSS_SIMULATOR 
                (SIMULATOR_ID, EVENT_ID, LOAN_VALUE, REALTY_VALUE, REALTY_TYPE,
                REALTY_LOCALE, PAYMENT_TERM, AMORTIZATION, NAME, EMAIL, PHONE,
                AGE, INSERTED_AT, CONTRACTTYPE, PRODUCT_ID, APP_ID)
            VALUES ('%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
            row_simulacao[0],
            config_id(row_simulacao[20]),
            row_simulacao[8],
            row_simulacao[10],
            row_simulacao[7],
            row_simulacao[19],
            row_simulacao[12],
            row_simulacao[18],
            row_simulacao[1].replace("'", "''").title(),
            row_simulacao[5],
            row_simulacao[2],
            row_simulacao[3],
            row_simulacao[22],
            contractType(row_simulacao[4]),
            productId(row_simulacao[6]),
            'DESCOMPLICA')
    cur.execute(insert)

conn.commit()
conn.close()