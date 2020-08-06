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
Initialize = False
SaveToDBase = False
DailyDbName = 'iopvdb-daily'
SourceDbName = 'iopvdb2'
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

def getdates(records):
    dhash = {}
    dates = []
    for rec in records:
        date, time = rec['TIME'].split(" ")
        dhash[date] = 1

    dates = [*dhash.keys()]
    return dates

def updatedaily():
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
            continue
        
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

        dailydb.log(nowtime, "stock update completed for '%s'." % stock)

def initstock(stock):
    nowtime = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    # connect to google sheets
    dailydb = GspreadDB(DailyDbName, JsonFile)
    stockdb = GspreadDB(StockDbName, JsonFile)

    try:
        tickersheet = stockdb.getstocksheet(stock)
    except ValueError as ve:
        dailydb.log(nowtime, ve)
        sys.exit(1)

    try:
        dailysheet = dailydb.getstocksheet(stock)
    except ValueError as ve:
        pass
    else:
        dailydb.deletesheet(stock)

    dailydb.initsheet(stock, ['DATE','OPEN','HIGH','LOW','CLOSE','REMARK'])
    dailysheet = dailydb.getstocksheet(stock)

    trecs = tickersheet.get_all_records()
    dates = sorted(getdates(trecs),
            key=lambda x: datetime.strptime(x, "%d/%m/%Y").strftime("%Y-%m-%d"),
            reverse=True)
    cells = []
    for nowdate in dates:
        dopen, high, low, close = getdaily(nowdate, trecs)
        print(nowdate, dopen, high, low, close)
        cells.append([nowdate, dopen, high, low, close])

    dailysheet.update('A2' , cells, value_input_option='USER_ENTERED')
    dailydb.log(nowtime, "new stock creation completed for '%s'." % stock)

def runmain(args):
    if args:
        for stock in args:
            initstock(stock)
    else:
        updatedaily()

def showhelp():
    print("Help is on the way...")

if __name__ == "__main__":
    argv = sys.argv[1:]
    try:
        # parse command line options
        opts, args = getopt.getopt(argv, 'ab:ghIj:s:')
        Options = dict(opts)
        if '-h' in Options.keys():
            showhelp()
            sys.exit(2)

        if '-a' in Options.keys():
            GetAllStocks = True

        if '-o' in Options.keys():
            OutputFile = Options['-o']

        if '-I' in Options.keys():
            Initialize = True

        if '-g' in Options.keys():
            SaveToDBase = True

        if '-b' in Options.keys():
            DailyDbName = Options['-b']

        if '-s' in Options.keys():
            SourceDbName = Options['-s']

        if '-j' in Options.keys():
            JsonFile = Options['-j']

        runmain(args)

    except getopt.GetoptError:
        #Print a message or do something useful
        print('Invalid command line option or arguments')
        sys.exit(2)
