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

conn = psycopg2.connect(
    "host=localhost dbname=olist user=olist password=postgres")

res = conn.cursor()
res.execute('SELECT count(*) FROM "opportunities";')
result = res.fetchall()

print("\nRESULTADO: ", result[0][0], "\n")
# conn.commit()
conn.close()

# IMPORT DUMP FILE TO CREATE SIMULATOR_SITE
#os.system("psql -h localhost -d postgres -U postgres -p 5432 -a -q -f postgres.pgsql")

# IMPORT DUMP FILE TO CREATE SIMULATOR_DESCOMPLICA
#os.system("psql -h localhost -d postgres -U postgres -p 5432 -a -q -f bkp-mysql-descomplicaai.sql")
