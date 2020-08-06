from requests_html import HTMLSession
from bs4 import BeautifulSoup
from datetime import datetime
import time
import pprint
import getopt
import sys
import re
import string
from gspreaddb import GspreadDB

Url = "http://www.bursamarketplace.com/mkt/themarket/etf"
Options = {}
GetAllStocks = False
OutputFile = ""
InputFile = ""
SaveToDBase = False
DailyDbName = 'iopvdb-daily'
StockDbName = 'iopvdb2'
JsonFile = 'iopv.json'

StockList = (
    #"TradePlus Shariah Gold Tracker",
    "TradePlus S&P New China Tracker-MYR",
    #"FTSE Bursa Malaysia KLCI ETF",
    #"ABF Malaysia Bond Index Fund",    # debug use, sheet not exists
)

def getdaily(tgtdate, records):
    dayrecs = []
    tdata = []
    tprices = {}
    for rec in records:
        date, time = rec['TIME'].split(" ")
        if date == tgtdate:
            #print(date, time, rec['IOPV'])
            tprices[time] = rec['IOPV']
            tdata.append(time)

    tdata.sort()
    dopen = tprices[tdata[0]]
    close = tprices[tdata[-1]]

    prices = [*tprices.values()]
    prices.sort()

    high = prices[0]
    low = prices[-1]

    return dopen, high, low, close

def runmain():
    # get date and time
    now = datetime.now()
    nowdate = now.strftime("%d/%m/%Y")
    nowtime = now.strftime("%d/%m/%Y %H:%M:%S")

    # connect to google sheets
    dailydb = GspreadDB(DailyDbName, JsonFile)
    stockdb = GspreadDB(StockDbName, JsonFile)

    # update data to google sheets
    for stock in StockList:
        try:
            tickersheet = stockdb.getstocksheet(stock)
            dailysheet = dailydb.getstocksheet(stock)
        except ValueError as ve:
            dailydb.log(nowtime, ve)
            next
        
        trecs = tickersheet.get_all_records()
        dopen, high, low, close = getdaily(nowdate, trecs)
        print(nowdate, dopen, high, low, close)
        lastdaily = dailysheet.row_values(2)
        lastdate = lastdaily[0]
        cells = [nowdate, dopen, high, low, close]
        if lastdate == nowdate:
            dailysheet.update('A2', [cells], value_input_option='USER_ENTERED')
        else:
            dailysheet.insert_row(cells, 2, value_input_option='USER_ENTERED')

    dailydb.log(nowtime, "stock update completed.")

def showhelp():
    print("Help is on the way...")

if __name__ == "__main__":
    argv = sys.argv[1:]
    try:
        # parse command line options
        opts, args = getopt.getopt(argv, 'ab:ghi:j:l:o:w:')
        Options = dict(opts)
        if '-h' in Options.keys():
            showhelp()
            sys.exit(2)

        if '-a' in Options.keys():
            GetAllStocks = True

        if '-o' in Options.keys():
            OutputFile = Options['-o']

        if '-i' in Options.keys():
            InputFile = Options['-i']

        if '-g' in Options.keys():
            SaveToDBase = True

        if '-b' in Options.keys():
            WorkbookName = Options['-b']

        if '-j' in Options.keys():
            JsonFile = Options['-j']

        # get IOPV info
        runmain()

    except getopt.GetoptError:
        #Print a message or do something useful
        print('Invalid command line option or arguments')
        sys.exit(2)