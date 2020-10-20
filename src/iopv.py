from requests_html import HTMLSession
from bs4 import BeautifulSoup
from datetime import datetime
from pprint import pprint
import time
import getopt
import sys
import re
import string
from urllib.parse import urlparse


Url = "http://www.bursamarketplace.com/mkt/themarket/etf"
Options = {}
MaxRetry = 3
RetryInterval = 3  # wait this many seconds before trying to reload webpage
GetAllStocks = False
OutputFile = ""
HtmlFile = ""
SaveToDBase = False
WorkbookName = ''
JsonFile = 'iopv.json'
StockListFile = ''
StockList = []


def parse_html(html):
    soup = BeautifulSoup(html, "lxml")
    etfs = soup.find_all(class_="tb_row tb_data")
    iopvinfo = {}
    for row in etfs:
        tbname = row.find(class_="tb_name")
        tbiopv = row.find(class_="tb_iopv")
        if tbname and tbiopv:
            name = tbname.get_text().split('\xa0')[-1]
            val = tbiopv.get_text()
            iopv = re.sub(f'[^{re.escape(string.printable)}]', '', val)
            iopvinfo[name] = iopv

    return iopvinfo


class IopvParser:
    def __init__(self, source=Url):
        self.source = source
        self.max_retry = MaxRetry
        self.iopv_data = None
        self.load_iopv()

    def iopv(self, stock, default=None):
        assert self.iopv_data
        return self.iopv_data.get(stock, default)

    def load_iopv(self):
        result = urlparse(self.source)
        is_url = all([result.scheme, result.netloc])
        if is_url:
            self.get_iopv_from_web()
        else:
            self.get_iopv_from_file()

    def get_iopv_from_web(self):
        # create an HTML Session object
        session = HTMLSession()

        # Use the object above to connect to needed webpage
        resp = session.get(self.source)
        for i in range(self.max_retry):
            # Run JavaScript code on webpage to load stock data
            resp.html.render()

            # extract stock info
            iopv_data = parse_html(resp.html.html)
            if iopv_data:
                self.iopv_data = iopv_data
                return

            # wait before reloading data
            time.sleep(RetryInterval)

        raise TimeoutError

    def get_iopv_from_file(self):
        f = open(self.source, "rb")
        html = f.read()
        self.iopv_data = parse_html(html)


if __name__ == "__main__":
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, 'i:')
        Options = dict(opts)
        if '-i' in Options.keys():
            HtmlFile = Options['-i']
    except getopt.GetoptError:
        print('Invalid command line option or arguments')
        sys.exit(2)

    if HtmlFile:
        iopv1 = IopvParser(HtmlFile)
    else:
        iopv1 = IopvParser(Url)

    pprint(iopv1.iopv_data)
