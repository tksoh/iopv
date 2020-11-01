import time
import pprint
import getopt
import sys
import os.path
from iopv import IopvParser
from fireworks import Firework
from utils import getstocklist
from pprint import pprint
from datetime import datetime


StockList = []
HtmlFile = None
IopvArchiveFile = None


def save_to_archive(logtime, iopv_list):
    if not IopvArchiveFile:
        return

    timestamp = logtime.strftime("%Y-%m-%d %H:%M:%S")
    is_new = False if os.path.isfile(IopvArchiveFile) else True
    with open(IopvArchiveFile, "a") as f:
        if is_new:
            f.write('DATE\tSTOCK\tIOPV\n')
        for stock, iopv in iopv_list:
            line = f'{timestamp}\t{stock}\t{iopv}\n'
            f.write(line)

def iopv_update():
    now = datetime.now()

    if HtmlFile:
        parser = IopvParser(HtmlFile)
    else:
        parser = IopvParser()

    iopv_list = parser.iopv_data.items()

    # save IOPV data to archive
    save_to_archive(now, iopv_list)

    # update IOPV data to firebase database
    fire = Firework()
    fire.update_iopv_list(iopv_list, logtime=now)


if __name__ == "__main__":
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, 'a:i:L:')
        Options = dict(opts)
        if '-i' in Options.keys():
            HtmlFile = Options['-i']
        if '-L' in Options.keys():
            listfile = Options['-L']
            StockList = getstocklist(listfile)
        if '-a' in Options.keys():
            IopvArchiveFile = Options['-a']
    except getopt.GetoptError:
        print('Invalid command line option or arguments')
        sys.exit(2)

    iopv_update()
