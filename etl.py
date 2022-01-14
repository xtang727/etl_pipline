import sys
import petl
import pymssql
import configparser
import requests
import datetime
import json
import decimal

# Load configurations
config = configparser.ConfigParser()
try:
    config.read('config.ini')
except Exception as e:
    print('Could not read configuration file:' + str(e))
    sys.exit()

# Parse in configurations
startDate = config['CONFIG']['startDate']
url = config['CONFIG']['url']
destServer = config['CONFIG']['server']
destDatabase = config['CONFIG']['database']

# Request data from URL
try:
    BOCResponse = requests.get(url+startDate)
except Exception as e:
    print('Could not make request:' + str(e))
    sys.exit()

BOCDates = []
BOCRates = []
# Check response status and process BOC JSON object
if (BOCResponse.status_code == 200):
    BOCRaw = json.loads(BOCResponse.text)
    # Extract observation data into column arrays
    for row in BOCRaw['observations']:
        BOCDates.append(datetime.datetime.strptime(row['d'],'%Y-%m-%d'))
        BOCRates.append(decimal.Decimal(row['FXUSDCAD']['v']))
    # Create petl table from column arrays and rename the columns
    exchangeRates = petl.fromcolumns([BOCDates,BOCRates],header=['date','rate'])

    # Load expense document
    try:
        expenses = petl.io.xlsx.fromxlsx('Expenses.xlsx',sheet='Github')
    except Exception as e:
        print('Could not open expenses.xlsx:' + str(e))
        sys.exit()

    # Join tables
    expenses = petl.outerjoin(exchangeRates,expenses,key='date')
    # Fill down missing values
    expenses = petl.filldown(expenses,'rate')
    # Remove dates with no expenses
    expenses = petl.select(expenses,lambda rec: rec.USD != None)
    # Add CDN column
    expenses = petl.addfield(expenses,'CAD', lambda rec: decimal.Decimal(rec.USD) * rec.rate)
    
    # Intialize database connection
    try:
        dbConnection = pymssql.connect(server=destServer,database=destDatabase)
    except Exception as e:
        print('Could not connect to database:' + str(e))
        sys.exit()

    # Populate Expenses database table
    try:
        petl.io.todb (expenses,dbConnection,'Expenses')
    except Exception as e:
        print('Could not write to database:' + str(e))

    print (expenses)