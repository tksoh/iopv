from requests_html import HTMLSession
from bs4 import BeautifulSoup
from datetime import datetime
import time
import pprint
import getopt
import sys
import re
import string
import gspreaddb

Url = "http://www.bursamarketplace.com/mkt/themarket/etf"
Options = {}
MaxRetry = 3
RetryInterval = 3       # wait this many seconds before trying to reload webpage
GetAllStocks = False
OutputFile = ""
HtmlFile = ""
SaveToDBase = False
StockList = (
    "TradePlus Shariah Gold Tracker",
    "TradePlus S&P New China Tracker-MYR",
    "FTSE Bursa Malaysia KLCI ETF",
)
    
def getstockdata(html):
    soup = BeautifulSoup(html, "lxml")
    etfs = soup.find_all(class_="tb_row tb_data")
    iopvinfo = []
    for row in etfs:
        tbname = row.find(class_="tb_name")
        tbiopv = row.find(class_="tb_iopv")
        if tbname and tbiopv:
            name = tbname.get_text().split('\xa0')[-1]
            if GetAllStocks or name in StockList:
                info = [name]
                val = tbiopv.get_text()
                iopv = re.sub(f'[^{re.escape(string.printable)}]', '', val)
                info.append(iopv)
                iopvinfo.append(info)
    
    return iopvinfo

def getstocklive():
    # create an HTML Session object
    session = HTMLSession()
     
    # Use the object above to connect to needed webpage
    resp = session.get(Url)
    for i in range(MaxRetry):
        # Run JavaScript code on webpage to load stock data
        resp.html.render()

        # extract stock info
        iopvinfo = getstockdata(resp.html.html)
        if iopvinfo:
            return iopvinfo

        # wait before reloading data
        time.sleep(RetryInterval)

    # loop timeout
    print("WARNING:\tmax retry exceeded!\n")
    raise TimeoutError

def getiopvfromfile(fn):
    f = open(fn, "rb")
    html = f.read()
    iopvinfo = getstockdata(html)
    return iopvinfo
    
def runmain():
    # get time stamp for record
    nowtime = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    if HtmlFile:
        iopvinfo = getiopvfromfile(HtmlFile)
        for stock in iopvinfo:
            print('\t'.join([nowtime] + stock))
    elif SaveToDBase:
        from gspreaddb import GspreadDB
        db = GspreadDB()

        try:
            iopvinfo = getstocklive()
            for stock in iopvinfo:
                name, iopv = stock
                db.add(name, nowtime, iopv)
            db.log(nowtime, "stock updated successfully.")
        except ValueError as ve:
            db.log(nowtime, ve)
        except TimeoutError:
            db.log(nowtime, "Timeout on downloading data")
    else:
        iopvinfo = getstocklive()
        for stock in iopvinfo:
            print('\t'.join([nowtime] + stock))

def showhelp():
    print("Help is on the way...")

if __name__ == "__main__":
    argv = sys.argv[1:]
    try:
        # parse command line options
        opts, args = getopt.getopt(argv, 'aghi:l:o:w:')
        Options = dict(opts)
        if '-h' in Options.keys():
            showhelp()
            sys.exit(2)

        if '-a' in Options.keys():
            GetAllStocks = True

        if '-o' in Options.keys():
            OutputFile = Options['-o']

        if '-l' in Options.keys():
            MaxRetry = Options['-l']

        if '-i' in Options.keys():
            HtmlFile = Options['-i']

        if '-w' in Options.keys():
            RetryInterval = Options['-w']

        if '-g' in Options.keys():
            SaveToDBase = True

        # get IOPV info
        runmain()

    except getopt.GetoptError:
        #Print a message or do something useful
        print('Invalid command line option or arguments')
        sys.exit(2)
