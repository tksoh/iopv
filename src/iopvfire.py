import time
import pprint
import getopt
import sys
from iopv import IopvParser
from fireworks import Firework
from utils import getstocklist
from pprint import pprint
from datetime import datetime


StockList = []
HtmlFile = None


def iopv_update():
    if HtmlFile:
        parser = IopvParser(HtmlFile)
    else:
        parser = IopvParser()

    # pprint(iopv_loader.iopv_data)
    fire = Firework()
    now = datetime.now()
    date = now.date()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    for stock, iopv in parser.iopv_data.items():
        if iopv == '-':
            continue
        if StockList and stock not in StockList:
            continue

        print(f"[{timestamp}] updating '{stock}', {iopv}")
        ticker = fire.get_stock_ticker(stock)
        fire.update_stock_raw(ticker, iopv, timestamp)
        fire.update_stock_daily(ticker, iopv, date)


if __name__ == "__main__":
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, 'i:L:')
        Options = dict(opts)
        if '-i' in Options.keys():
            HtmlFile = Options['-i']
        if '-L' in Options.keys():
            listfile = Options['-L']
            StockList = getstocklist(listfile)
    except getopt.GetoptError:
        print('Invalid command line option or arguments')
        sys.exit(2)

    iopv_update()
