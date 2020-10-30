import sys
import getopt
import json
import csv
import pyrebase
from pprint import pprint
from datetime import datetime
from pandas import to_datetime
import pandas as pd


firebase_config_file = 'firebase_config.json'
daily_db = 'iopv-daily'
raw_db = 'iopv-raw'

class Firework:
    def __init__(self, config_file=firebase_config_file):
        self.config_file = config_file
        with open(config_file) as f:
            self.config = json.load(f)

        self.firebase = pyrebase.initialize_app(self.config['firebase_connect'])
        self.load_stock_list()

    def load_stock_list(self):
        self.stock_list = {}
        db = self.firebase.database()
        groups = db.child('stock-list').get()
        for row in groups.each():
            data = row.val()
            self.stock_list[data['STOCK']] = data['TICKER']

    def get_stock_ticker(self, stock):
        if stock not in self.stock_list:
            self.add_stock(stock)
            self.load_stock_list()
        return self.stock_list[stock]

    def add_stock(self, stock):
        # find the next available ETF ticker
        for i in range(1000):
            ticker = 'NEWETF-' + str(i + 1)
            if ticker not in self.stock_list:
                break
        else:
            raise KeyError(f'[ERROR] unable to create a ticker for new ETF: {stock}')

        date = str(datetime.now().date())
        data = {'STOCK': stock, 'TICKER': ticker, 'COMMENT': f'Auto added on {date}'}
        db = self.firebase.database()
        db.child('stock-list').push(data)

    def get_stock_daily(self, tickers, last=100):
        db = self.firebase.database()
        if last < 0:
            data = db.child(daily_db).get()
        else:
            data = db.child(daily_db).order_by_child('DATE').limit_to_last(last).get()

        stock_data = {}
        rows = data.val()
        for row in rows.values():
            date = row['DATE']
            iset = row['IOPV']
            for ticker, iopv in iset.items():
                if tickers and ticker not in tickers:
                    continue
                if ticker not in stock_data:
                    stock_data[ticker] = []
                stock_data[ticker].append({'DATE': date, 'IOPV': iopv})

        return stock_data

    def get_stock_raw(self, tickers, last=100):
        db = self.firebase.database()
        if last < 0:
            data = db.child(raw_db).get()
        else:
            data = db.child(raw_db).order_by_child('DATE').limit_to_last(last).get()

        stock_data = {}
        rows = data.val()
        for row in rows.values():
            date = row['DATE']
            iset = row['IOPV']
            for ticker, iopv in iset.items():
                if tickers and ticker not in tickers:
                    continue
                if ticker not in stock_data:
                    stock_data[ticker] = []
                stock_data[ticker].append({'DATE': date, 'IOPV': iopv})

        return stock_data

    def update_stock_raw(self, date, data):
        db = self.firebase.database()
        db.child(raw_db).child(date).update(data)

    def update_stock_daily(self, date, data):
        db = self.firebase.database()
        db.child(daily_db).child(date).update(data)

    def update_iopv_list(self, iopv_list):
        # download all data
        db = self.firebase.database()
        raw = db.child(raw_db).get()
        raw_data = raw.val()
        if not raw_data:
            raw_data = {}
        daily = db.child(daily_db).get()
        daily_data = daily.val()
        if not daily_data:
            daily_data = {}

        # build dates
        now = datetime.now()
        date = str(now.date())
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

        # update IOPV raw database
        iopv_dict = {}
        for stock, iopv in iopv_list:
            ticker = self.get_stock_ticker(stock)
            iopv_dict[ticker] = iopv
        fire_data = {"DATE": timestamp, "IOPV": iopv_dict}
        db.child(raw_db).child(timestamp).update(fire_data)

        # update IOPV daily database
        stock_iopv = {}
        for dt, iset in raw_data.items():
            if date not in dt:
                continue
            for stk, iopv in iset['IOPV'].items():
                if stk not in stock_iopv:
                    stock_iopv[stk] = []
                stock_iopv[stk].append({'DATE': dt, 'IOPV': iopv})

        def get_ohlc(ilist):
            df = pd.DataFrame(ilist)
            dfs = df.sort_values(by='DATE', ascending=True)
            op = dfs.iloc[1]['IOPV'] if len(dfs) > 1 else dfs.iloc[0]['IOPV']
            cl = dfs.iloc[-1]['IOPV']
            hi = dfs['IOPV'].max()
            lo = dfs['IOPV'].min()
            return op, hi, lo, cl

        daily_dict = {}
        for stk, ilist in stock_iopv.items():
            ilist.append({'DATE': timestamp, 'IOPV': iopv_dict[stk]})
            op, hi, lo, cl = get_ohlc(ilist)
            ohlc = {'OPEN': op, 'HIGH': hi, 'LOW':lo, 'CLOSE':cl}
            daily_dict[stk] = ohlc

        fire_data = {"DATE": date, "IOPV": daily_dict}
        db.child(daily_db).child(date).update(fire_data)

    def import_json(self, db_path, filename):
        with open(filename) as f:
            data = json.load(f)
            db.child(db_path).set(data)

    def import_csv(self, db_path, filename, delim='\t'):
        with open(filename) as f:
            reader = csv.DictReader(f, delimiter=delim)
            data = []
            for row in reader:
                try:
                    date = to_datetime(row['DATE'], format='%d/%m/%Y')
                    row['DATE'] = str(date.date())
                except KeyError:
                    pass
                try:
                    time = to_datetime(row['TIME'], format='%d/%m/%Y %H:%M:%S')
                    row['DATE'] = str(time)
                    del row['TIME']
                except KeyError:
                    pass
                data.append(dict(row))
            db = self.firebase.database()
            db.child(db_path).set(data)

    def purge_daily(self, stock, rows):
        db = self.firebase.database()
        data = db.child(daily_db).child(stock).order_by_child('DATE').limit_to_first(rows).get()
        for rec in data.each():
            key = rec.key()
            db.child(raw_db).child(stock).child(key).remove()

    def purge_raw(self, stock, rows):
        db = self.firebase.database()
        data = db.child(raw_db).child(stock).order_by_child('DATE').limit_to_first(rows).get()
        for rec in data.each():
            key = rec.key()
            db.child(raw_db).child(stock).child(key).remove()

    def upload_file(self, path, remote=None):
        if not remote:
            remote_path = path
        else:
            remote_path = remote
        storage = self.firebase.storage()
        storage.child(remote_path).put(path)

    def download_file(self, path, local=None):
        if not local:
            local_path = path
        else:
            local_path = local
        storage = self.firebase.storage()
        storage.child(path).download(local_path)


if __name__ == "__main__":
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, 'i:')
        Options = dict(opts)
        if '-i' in Options.keys():
            a = Options['-i']
    except getopt.GetoptError:
        print('Invalid command line option or arguments')
        sys.exit(2)

    fire = Firework()
    price = fire.get_stock_daily(['0829EA'], last=3)
    pprint(price)
    price = fire.get_stock_raw(['0829EA'], last=3)
    pprint(price)
