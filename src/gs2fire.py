import sys
import json
import getopt
import datetime
from fireworks import Firework
from gspreaddb import GspreadDB
from utils import getstocklist
from pandas import to_datetime

DailyDbName = 'Copy of iopvdb-daily'
SourceDbName = 'Copy of iopvdb2'
JsonAuthFile = 'iopv.json'
DebugMode = True
update_fire = False
LastDaysRaw = 5

def main(stocks):
    assert stocks

    start_date = datetime.date.today() - datetime.timedelta(LastDaysRaw)

    # connect to google sheets
    dailydb = GspreadDB(DailyDbName, 'DAILY', JsonAuthFile)
    stockdb = GspreadDB(SourceDbName, 'IOPV', JsonAuthFile)

    iopv_by_date = {}
    daily_by_date = {}
    for stock in stocks:
        print("Processing %s" % stock)

        try:
            ticker = stockdb.getstockticker(stock)
            tickersheet = stockdb.getstocksheet(stock)
            rows = tickersheet.get_all_records()
            for row in rows:
                time = to_datetime(row['TIME'], format='%d/%m/%Y %H:%M:%S')
                if time.date() < start_date:
                    continue
                date = str(time)
                if date not in iopv_by_date:
                    iopv_by_date[date] = {'DATE': date, 'IOPV': {}}
                iopv_by_date[date]['IOPV'][ticker] = str(row['IOPV'])

            dailysheet = dailydb.getstocksheet(stock)
            rows = dailysheet.get_all_records()
            for row in rows:
                date = to_datetime(row['DATE'], format='%d/%m/%Y')
                date = str(date.date())
                if date not in daily_by_date:
                    daily_by_date[date] = {'DATE': date, 'IOPV': {}}
                ohlc = {'OPEN': str(row['OPEN']), 'HIGH': str(row['HIGH']),
                        'LOW': str(row['LOW']), 'CLOSE': str(row['CLOSE'])}
                daily_by_date[date]['IOPV'][ticker] = ohlc
        except ValueError as ve:
            print(f"error getting sheet data for '{stock}': {ve}")
            continue

    print(f'Writing JSON files...')
    with open('iopv-raw.json', 'w') as fp:
        json.dump(iopv_by_date, fp, sort_keys=True, indent=4)
    with open('iopv-daily.json', 'w') as fp:
        json.dump(daily_by_date, fp, sort_keys=True, indent=4)

    if update_fire:
        fire = Firework()

        # update raw database
        print(f'Updating Firebase raw database...')
        for date, rec in iopv_by_date.items():
            fire.update_stock_raw(date, rec)

        # update daily database
        print(f'Updating Firebase daily database...')
        for date, rec in daily_by_date.items():
            fire.update_stock_daily(date, rec)


if __name__ == "__main__":
    argv = sys.argv[1:]
    stocklist = []
    try:
        opts, args = getopt.getopt(argv, 'r:FL:')
        Options = dict(opts)
        if '-L' in Options.keys():
            listfile = Options['-L']
            stocklist = getstocklist(listfile)
        if '-F' in Options.keys():
            update_fire = True
        if '-r' in Options.keys():
            LastDaysRaw = Options['-r']
    except getopt.GetoptError:
        print('Invalid command line option or arguments')
        sys.exit(2)

    main(stocklist)
