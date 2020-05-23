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

def getstockupdate(resp, stockid):
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
        
        # Run JavaScript code on webpage
        resp.html.render()
         
        # extract stock info
        soup = BeautifulSoup(resp.html.html, "lxml")
        
        toppnl = soup.find(class_="topPnl_name")
        tbl = soup.select("div.dataItem_hld")
        stocktbl = [(elem.get_text(), elem.find(class_="value").get_text()) for elem in tbl]
        #print(stocktbl)
        
        iopvinfo = {}
        iopvinfo['stock name'] = toppnl.get_text()
        iopvinfo['TIME'] = dt_string
        for elem in stocktbl:
            name = elem[0].split("\n")[1]
            if name in lookup:
                iopvinfo[name] = elem[1]
                
        # check loop condition
        if (len(stocktbl[0][1]) > 0):
            #pprint.pprint(iopvinfo)
            outf.write('\t'.join([iopvinfo['TIME'], stockid, iopvinfo['IOPV']]))
            outf.write("\n")
            break

        if i < maxloop:
            i = i + 1
            print("Data invalid. Trying again...\n")
            time.sleep(2)
        else:
            print("max loop exceeded!\n")
            outf.write('\t'.join([iopvinfo['TIME'], stockid, 'TIMEOUT']))
            outf.write("\n")
            break
    outf.close()

def runmain():
    # create an HTML Session object
    session = HTMLSession()
     
    # Use the object above to connect to needed webpage
    resp = session.get("http://www.bursamarketplace.com/mkt/themarket/etf/TRCM")
    getstockupdate(resp, 'TRCM')
    resp.close()

argv = sys.argv[1:]
try:
    opts, args = getopt.getopt(argv, 'ho:t:')
    Options = dict(opts)
    #print(args)
    #print(Options)
    runmain()
except getopt.GetoptError:
    #Print a message or do something useful
    print('Something went wrong!')
    sys.exit(2)
