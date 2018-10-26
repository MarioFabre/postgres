from datetime import datetime
import csv
import fnmatch
import json
import locale
import os
import pandas as pd
import numpy
import sys
import fileinput
import psycopg2

def watchDog(sMsg):
    print(sMsg)
    return

def get_date(s_date, default_null_date_value):
    date_patterns = ['%d/%b/%Y', '%d/%m/%Y']

    for pattern in date_patterns:
        try:
            return datetime.strptime(s_date, pattern)
        except:
            pass

    return datetime.strptime(default_null_date_value, '%d/%m/%Y')

# CHANGE INCORRECT OR NULL GUID TO ZERO FORMAT
def config_id(guid):
    return '00000000-0000-0000-0000-000000000000' if (guid == None or len(guid) < 36) else guid.strip()

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

# LOAD Analytics-ExtraçãoSiteBcrediA.csv EXTRACT FROM GA
# SPLIT EVENT_TYPE TO GUID FROM EVENT_LABEL
# CREATE A NEW COLUMN WITH EVENT_TYPE
for processFile in processFiles:
    if fnmatch.fnmatch(processFile, 'Analytics-ExtraçãoSiteBcrediA.csv'):
        processFile1 = pd.read_csv(
            inboundFilesPath+processFile, sep=',', header=14, encoding='latin-1', engine='python')

        # rename columns
        actual_columns_list = ['Date',
                               'Source / Medium',
                               'Event Label',
                               'Default Channel Grouping',
                               'Campaign',
                               'Sessions']

        renamed_columns_list = ['DATE',
                                'SOURCE_MEDIUM',
                                'EVENT_LABEL',
                                'CHANNEL_GROUP',
                                'CAMPAIGN',
                                'EVENT_TYPE']

        processFile1.rename(columns=dict(
            zip(actual_columns_list, renamed_columns_list)), inplace=True)
        # Transformação de Data
        processFile1['EVENT_LABEL'] = processFile1['EVENT_LABEL'].apply(
            lambda x: str(x)[-36:])
        # remove unused columns
        processFile1['EVENT_TYPE'] = processFile1['EVENT_TYPE'].apply(
            lambda x: str('VER-SIMULACAO'))

        add_columns_list = [
            'SYSTEM']
        for add_Column_Name in add_columns_list:
            processFile1 = processFile1.assign(
                **{add_Column_Name: numpy.full(len(processFile1), 'SITE')})

# LOAD Analytics-ExtraçãoSiteBcrediB.csv EXTRACT FROM GA
# SPLIT EVENT_TYPE TO GUID FROM EVENT_LABEL
# CREATE A NEW COLUMN WITH EVENT_TYPE
for processFile in processFiles:
    if fnmatch.fnmatch(processFile, 'Analytics-ExtraçãoSiteBcrediB.csv'):
        processFile2 = pd.read_csv(
            inboundFilesPath+processFile, sep=',', header=14, encoding='latin-1', engine='python')

        # rename columns
        actual_columns_list = ['Date',
                               'Source / Medium',
                               'Event Label',
                               'Default Channel Grouping',
                               'Campaign',
                               'Sessions']

        renamed_columns_list = ['DATE',
                                'SOURCE_MEDIUM',
                                'EVENT_LABEL',
                                'CHANNEL_GROUP',
                                'CAMPAIGN',
                                'EVENT_TYPE']

        processFile2.rename(columns=dict(
            zip(actual_columns_list, renamed_columns_list)), inplace=True)
        # Transformação de Data
        processFile2['EVENT_LABEL'] = processFile2['EVENT_LABEL'].apply(
            lambda x: str(x)[-36:])
        # remove unused columns
        processFile2['EVENT_TYPE'] = processFile2['EVENT_TYPE'].apply(
            lambda x: str('SOLICITAR-CREDITO'))

        add_columns_list = [
            'SYSTEM']
        for add_Column_Name in add_columns_list:
            processFile2 = processFile2.assign(
                **{add_Column_Name: numpy.full(len(processFile2), 'SITE')})

# LOAD Analytics-ExtraçãoDescomplica.csv EXTRACT FROM GA
# SPLIT EVENT_TYPE TO GUID FROM EVENT_LABEL
# CREATE A NEW COLUMN WITH EVENT_TYPE
for processFile in processFiles:
    if fnmatch.fnmatch(processFile, 'Analytics-ExtraçãoDescomplica.csv'):
        processFile3 = pd.read_csv(
            inboundFilesPath+processFile, sep=',', header=14, encoding='latin-1', engine='python')

        # rename columns
        actual_columns_list = ['Date',
                               'Source / Medium',
                               'Event Label',
                               'Default Channel Grouping',
                               'Campaign',
                               'Sessions']

        renamed_columns_list = ['DATE',
                                'SOURCE_MEDIUM',
                                'EVENT_LABEL',
                                'CHANNEL_GROUP',
                                'CAMPAIGN',
                                'EVENT_TYPE']

        processFile3.rename(columns=dict(
            zip(actual_columns_list, renamed_columns_list)), inplace=True)
        # Transformação de Data
        processFile3['EVENT_LABEL'] = processFile3['EVENT_LABEL'].apply(
            lambda x: str(x)[-36:])
        # remove unused columns
        processFile3['EVENT_TYPE'] = processFile3['EVENT_TYPE'].apply(
            lambda x: str('SOLICITAR-CREDITO'))

        add_columns_list = [
            'SYSTEM']
        for add_Column_Name in add_columns_list:
            processFile3 = processFile3.assign(
                **{add_Column_Name: numpy.full(len(processFile3), 'DESCOMPLICA')})


# Save CSV output file
# progNumOutputFile = datetime.now().strftime('%Y%m%d_%H%M%S') + '_A.csv'
# processFile1.to_csv(outputFilesPath+progNumOutputFile, index=False, encoding='utf-8')

# progNumOutputFile = datetime.now().strftime('%Y%m%d_%H%M%S') + '_B.csv'
# processFile2.to_csv(outputFilesPath+progNumOutputFile, index=False, encoding='utf-8')

# progNumOutputFile = datetime.now().strftime('%Y%m%d_%H%M%S') + '_C.csv'
# processFile3.to_csv(outputFilesPath+progNumOutputFile, index=False, encoding='utf-8')

conn = psycopg2.connect(
    "host=localhost dbname=postgres user=postgres password=postgres")

cur = conn.cursor()
cur.execute("""DROP TABLE IF EXISTS ga_descomplica_solic_credito""")

cur = conn.cursor()
cur.execute("""DROP TABLE IF EXISTS ga_site_solic_credito""")

cur = conn.cursor()
cur.execute("""DROP TABLE IF EXISTS ga_site_ver_simulacao""")

cur = conn.cursor()
cur.execute("""DROP TABLE IF EXISTS cross_ga""")

cur = conn.cursor()
cur.execute("""DROP TABLE IF EXISTS export_crm""")

cur = conn.cursor()
cur.execute('DROP TABLE IF EXISTS CROSS_SIMULATOR;')

cur = conn.cursor()
cur.execute("""DROP TABLE IF EXISTS bcredi_analytics""")

# CREATE TABLE TO STORE ALL INFORMATION FROM GA_SITE_VER_SIMULACAO EXPORT
cur = conn.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS GA_SITE_VER_SIMULACAO
            (
                GA_CODE SERIAL PRIMARY KEY,
                EVENT_ID UUID NOT NULL, 
                DATE VARCHAR (30) NOT NULL, 
                SOURCE_MEDIUM VARCHAR (300) NOT NULL, 
                CHANNEL_GROUP VARCHAR (200) NOT NULL, 
                CAMPAIGN VARCHAR (350) NOT NULL, 
                EVENT_TYPE VARCHAR (55) NOT NULL, 
                SYSTEM VARCHAR (55) NOT NULL, 
                INSERT_DATE timestamp default current_timestamp
            );""")
conn.commit()

#CREATE TABLE TO STORE ALL INFORMATION FROM GA_SITE_SOLIC_CREDITO EXPORT
cur = conn.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS GA_SITE_SOLIC_CREDITO
            (
                GA_CODE SERIAL PRIMARY KEY,
                EVENT_ID UUID NOT NULL, 
                DATE VARCHAR (30) NOT NULL, 
                SOURCE_MEDIUM VARCHAR (300) NOT NULL, 
                CHANNEL_GROUP VARCHAR (200) NOT NULL, 
                CAMPAIGN VARCHAR (350) NOT NULL, 
                EVENT_TYPE VARCHAR (55) NOT NULL, 
                SYSTEM VARCHAR (55) NOT NULL, 
                INSERT_DATE timestamp default current_timestamp
            );""")
conn.commit()

#CREATE TABLE TO STORE ALL INFORMATION FROM GA_DESCOMPLICA_SOLIC_CREDITO EXPORT
cur = conn.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS GA_DESCOMPLICA_SOLIC_CREDITO
            (
                GA_CODE SERIAL PRIMARY KEY,
                EVENT_ID UUID NOT NULL, 
                DATE VARCHAR (30) NOT NULL, 
                SOURCE_MEDIUM VARCHAR (300) NOT NULL, 
                CHANNEL_GROUP VARCHAR (200) NOT NULL, 
                CAMPAIGN VARCHAR (350) NOT NULL, 
                EVENT_TYPE VARCHAR (55) NOT NULL, 
                SYSTEM VARCHAR (55) NOT NULL, 
                INSERT_DATE timestamp default current_timestamp
            );""")
conn.commit()

#CREATE TABLE TO STORE ALL INFORMATION FROM CROSS_GA
cur = conn.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS CROSS_GA
            (
                GA_CODE SERIAL PRIMARY KEY,
                EVENT_ID UUID NOT NULL, 
                DATE VARCHAR (30) NOT NULL, 
                SOURCE_MEDIUM VARCHAR (300) NOT NULL, 
                CHANNEL_GROUP VARCHAR (200) NOT NULL, 
                CAMPAIGN VARCHAR (350) NOT NULL, 
                EVENT_TYPE VARCHAR (55) NOT NULL, 
                SYSTEM VARCHAR (55) NOT NULL, 
                INSERT_DATE timestamp default current_timestamp
            );""")
conn.commit()

cur = conn.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS EXPORT_CRM
            (
                CODE SERIAL PRIMARY KEY,
                GUID UUID NOT NULL,
                EVENTO VARCHAR (10) NOT NULL, 
                STATUS_EVENTO VARCHAR (20) NOT NULL, 
                TIPO_EVENTO VARCHAR (30) NOT NULL, 
                CLIENTE VARCHAR (70) NOT NULL, 
                CPF_CNPJ VARCHAR (25) NOT NULL, 
                MOTIVO_RESULTADO VARCHAR (50) NOT NULL, 
                DATA_CONCLUSAO_EVENTO VARCHAR (15) NOT NULL, 
                VALOR_CREDITO VARCHAR (20) NOT NULL, 
                VALOR_IMOVEL VARCHAR (20) NOT NULL, 
                EMAIL VARCHAR (70) NOT NULL
            );""")
conn.commit()

cur = conn.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS CROSS_SIMULATOR
            (
                CODE SERIAL PRIMARY KEY,
                SIMULATOR_ID VARCHAR (36) NOT NULL,
                EVENT_ID VARCHAR (36) NOT NULL,
                LOAN_VALUE VARCHAR (20) NOT NULL, 
                REALTY_VALUE VARCHAR (20) NOT NULL,
                REALTY_TYPE VARCHAR (70) NOT NULL, 
                REALTY_LOCALE VARCHAR (5) NOT NULL, 
                PAYMENT_TERM VARCHAR (10) NOT NULL, 
                AMORTIZATION VARCHAR (5) NOT NULL, 
                NAME VARCHAR (70) NOT NULL, 
                EMAIL VARCHAR (70) NOT NULL,
                PHONE VARCHAR (70) NOT NULL, 
                AGE VARCHAR (5) NOT NULL, 
                INSERTED_AT VARCHAR (30) NOT NULL, 
                CONTRACTTYPE VARCHAR (5) NOT NULL,
                PRODUCT_ID VARCHAR (5) NOT NULL,
                APP_ID VARCHAR (20) NOT NULL
            );""")
conn.commit()

cur = conn.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS BCREDI_ANALYTICS
            (
                CODE SERIAL PRIMARY KEY,
                EVENT_ID UUID NOT NULL,
                GA_DATE_SESSION VARCHAR (20) NOT NULL, 
                SOURCE_MEDIUM VARCHAR (70) NOT NULL,
                CHANNEL_GROUP VARCHAR (70) NOT NULL, 
                CAMPAIGN VARCHAR (80) NOT NULL, 
                EVENT_TYPE VARCHAR (20) NOT NULL, 
                SYSTEM_GA VARCHAR (20) NOT NULL,
                CLIENT_NAME VARCHAR (70) NOT NULL, 
                CLIENT_EMAIL VARCHAR (70) NOT NULL, 
                CLIENT_AGE VARCHAR (5) NOT NULL, 
                REALTY_VALUE VARCHAR (30) NOT NULL,
                LOAN_VALUE VARCHAR (30) NOT NULL, 
                AMORTIZATION VARCHAR (5) NOT NULL, 
                REALTY_LOCALE VARCHAR (5) NOT NULL, 
                SYSTEM_SIMULATOR VARCHAR (20) NOT NULL, 
                REALTY_TYPE VARCHAR (70) NOT NULL, 
                CONTRACTTYPE VARCHAR (5) NOT NULL,
                SIMULATOR_DATE TIMESTAMP NOT NULL
            );""")
conn.commit()

# INSERT DATA FROM GA_SITE_VER_SIMULACAO, TRANSFORMING GUID
for index, row in processFile1.iterrows():
    cur = conn.cursor()
    insert = """INSERT INTO GA_SITE_VER_SIMULACAO 
                        (EVENT_ID, DATE, SOURCE_MEDIUM,
                        CHANNEL_GROUP, CAMPAIGN, EVENT_TYPE, 
                        SYSTEM)
                    VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
        config_id(row['EVENT_LABEL']),
        row['DATE'],
        row['SOURCE_MEDIUM'],
        row['CHANNEL_GROUP'],
        row['CAMPAIGN'],
        row['EVENT_TYPE'],
        row['SYSTEM'])
    cur.execute(insert)

    insert_c = """INSERT INTO CROSS_GA
                        (EVENT_ID, DATE, SOURCE_MEDIUM,
                        CHANNEL_GROUP, CAMPAIGN, EVENT_TYPE, 
                        SYSTEM)
                    VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
        config_id(row['EVENT_LABEL']),
        row['DATE'],
        row['SOURCE_MEDIUM'],
        row['CHANNEL_GROUP'],
        row['CAMPAIGN'],
        row['EVENT_TYPE'],
        row['SYSTEM'])
    cur.execute(insert_c)

conn.commit()

# INSERT DATA FROM GA_SITE_SOLIC_CREDITO, TRANSFORMING GUID
for index, row in processFile2.iterrows():
    cur = conn.cursor()
    insert = """INSERT INTO GA_SITE_SOLIC_CREDITO 
                        (EVENT_ID, DATE, SOURCE_MEDIUM,
                        CHANNEL_GROUP, CAMPAIGN, EVENT_TYPE, 
                        SYSTEM)
                    VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
        config_id(row['EVENT_LABEL']),
        row['DATE'],
        row['SOURCE_MEDIUM'],
        row['CHANNEL_GROUP'],
        row['CAMPAIGN'],
        row['EVENT_TYPE'],
        row['SYSTEM'])
    cur.execute(insert)

    insert_c = """INSERT INTO CROSS_GA 
                        (EVENT_ID, DATE, SOURCE_MEDIUM,
                        CHANNEL_GROUP, CAMPAIGN, EVENT_TYPE, 
                        SYSTEM)
                    VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
        config_id(row['EVENT_LABEL']),
        row['DATE'],
        row['SOURCE_MEDIUM'],
        row['CHANNEL_GROUP'],
        row['CAMPAIGN'],
        row['EVENT_TYPE'],
        row['SYSTEM'])
    cur.execute(insert_c)

conn.commit()

# INSERT DATA FROM GA_DESCOMPLICA_SOLIC_CREDITO, TRANSFORMING GUID
for index, row in processFile3.iterrows():
    cur = conn.cursor()
    insert = """INSERT INTO GA_DESCOMPLICA_SOLIC_CREDITO 
                        (EVENT_ID, DATE, SOURCE_MEDIUM,
                        CHANNEL_GROUP, CAMPAIGN, EVENT_TYPE, 
                        SYSTEM)
                    VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
        config_id(row['EVENT_LABEL']),
        row['DATE'],
        row['SOURCE_MEDIUM'],
        row['CHANNEL_GROUP'],
        row['CAMPAIGN'],
        row['EVENT_TYPE'],
        row['SYSTEM'])
    cur.execute(insert)

    insert_c = """INSERT INTO CROSS_GA 
                        (EVENT_ID, DATE, SOURCE_MEDIUM,
                        CHANNEL_GROUP, CAMPAIGN, EVENT_TYPE, 
                        SYSTEM)
                    VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
        config_id(row['EVENT_LABEL']),
        row['DATE'],
        row['SOURCE_MEDIUM'],
        row['CHANNEL_GROUP'],
        row['CAMPAIGN'],
        row['EVENT_TYPE'],
        row['SYSTEM'])
    cur.execute(insert_c)

conn.commit()

conn.close()

# IMPORT DUMP FILE TO CREATE SIMULATOR_SITE
os.system("psql -h localhost -d postgres -U postgres -p 5432 -a -q -f postgres.pgsql")

# IMPORT DUMP FILE TO CREATE SIMULATOR_DESCOMPLICA
os.system("psql -h localhost -d postgres -U postgres -p 5432 -a -q -f bkp-mysql-descomplicaai.sql")

##################################################################
# for processFile in processFiles:
#     if fnmatch.fnmatch(processFile, 'simulator.json'):
#         processFile4 = pd.read_json(
#             inboundFilesPath+processFile, encoding='latin-1')

# simulator = json.loads(processFile4.to_json())