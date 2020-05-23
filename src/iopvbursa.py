# import HTMLSession from requests_html
from requests_html import HTMLSession
from bs4 import BeautifulSoup
from datetime import datetime
import time
import pprint
import getopt
import sys

lookup = ("IOPV", "IOPV Chg")
Options = {}
etf_url = "http://www.bursamarketplace.com/mkt/themarket/etf"
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
            if name in StockList:
                info = [name]
                info.append(row.find(class_="tb_iopv").get_text())
                #info.append(row.find(class_="tb_volume").get_text())   // volume info useful?
                iopvinfo.append(info)
    
    return iopvinfo


def getstockupdate(resp):
    outf = sys.stdout
    if '-o' in Options.keys():
        outf = open(Options['-o'], "a")

    maxloop = 3
    if '-t' in Options.keys():
        maxloop = Options['-t']
    i = 1
    while True:
        # show current time
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        #outf.write("TIME: %s\n" % (dt_string))
        
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

        if i < maxloop:
            i = i + 1
            print("WARNING:\tdata invalid, trying again...\n")
            time.sleep(2)
        else:
            print("WARNING:\tmax loop exceeded!\n")
            outf.write('\t'.join([dt_string, "", 'TIMEOUT']))
            outf.write("\n")
            break
    #outf.close()

def runmain():
    # create an HTML Session object
    session = HTMLSession()
     
    # Use the object above to connect to needed webpage
    resp = session.get(etf_url)
    getstockupdate(resp)
    resp.close()

def parsehtmlfile(fn):
    f = open(fn, "rb")
    html = f.read()
    iopvinfo = getstockdata(html)
    pprint.pprint(iopvinfo)
    
argv = sys.argv[1:]
try:
    opts, args = getopt.getopt(argv, 'D:ho:t:')
    Options = dict(opts)
    #print(args)
    #print(Options)
    if '-D' in Options.keys():
        parsehtmlfile(Options['-D'])
    else:
        runmain()
    
except getopt.GetoptError:
    #Print a message or do something useful
    print('Something went wrong!')
    sys.exit(2)
