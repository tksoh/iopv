from requests_html import HTMLSession
from bs4 import BeautifulSoup
from datetime import datetime
import time
import pprint
import getopt
import sys
import re
import string

Url = "http://www.bursamarketplace.com/mkt/themarket/etf"
Options = {}
MaxRetry = 3
RetryInterval = 3       # wait this many seconds before trying to reload webpage
GetAllStocks = False
OutputFile = ""
HtmlFile = ""
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
        cell = row.find(class_="tb_name")
        if cell:
            name = cell.get_text().split('\xa0')[-1]
            if GetAllStocks or name in StockList:
                info = [name]
                val = row.find(class_="tb_iopv").get_text()
                iopv = re.sub(f'[^{re.escape(string.printable)}]', '', val)
                info.append(iopv)
                #info.append(row.find(class_="tb_volume").get_text())   // volume info useful?
                iopvinfo.append(info)
    
    return iopvinfo


def getstockupdate(resp):
    outf = sys.stdout
    if OutputFile:
        outf = open(OutputFile, "a")

    i = 1
    while True:
        # show current time
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        
        # Run JavaScript code on webpage to load stock data
        resp.html.render()
         
        # extract stock info
        iopvinfo = getstockdata(resp.html.html)
        
        # check loop condition
        if (iopvinfo):
            #pprint.pprint(iopvinfo)
            for stock in iopvinfo:
                outf.write('\t'.join([dt_string] + stock))
                outf.write("\n")
            break

        if i < MaxRetry:
            i = i + 1
            print("WARNING:\tdata invalid, trying again...\n")
            time.sleep(RetryInterval)
        else:
            print("WARNING:\tmax retry exceeded!\n")
            outf.write('\t'.join([dt_string, "", 'TIMEOUT']))
            outf.write("\n")
            break
    #outf.close()

def runmain():
    # create an HTML Session object
    session = HTMLSession()
     
    # Use the object above to connect to needed webpage
    resp = session.get(Url)
    getstockupdate(resp)
    resp.close()

def showhelp():
    print("Help is on the way...")
    
def parsehtmlfile(fn):
    f = open(fn, "rb")
    html = f.read()
    iopvinfo = getstockdata(html)
    pprint.pprint(iopvinfo)
    
argv = sys.argv[1:]
try:
    # parse command line options
    opts, args = getopt.getopt(argv, 'ahi:l:o:w:')
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
        
    # get IOPV info
    if HtmlFile:
        parsehtmlfile(HtmlFile)
    else:
        runmain()
except getopt.GetoptError:
    #Print a message or do something useful
    print('Something went wrong!')
    sys.exit(2)
