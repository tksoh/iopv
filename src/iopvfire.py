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

    fire = Firework()
    fire.update_iopv_list(parser.iopv_data.items())


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
