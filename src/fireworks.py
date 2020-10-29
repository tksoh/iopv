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
        try:
            return self.stock_list[stock]
        except KeyError:
            return None

    def get_stock_daily(self, stock, last=100):
        db = self.firebase.database()
        if last<0:
            data = db.child(daily_db).child(stock).get()
        else:
            data = db.child(daily_db).child(stock).order_by_child('DATE').limit_to_last(last).get()
        return data

    def get_stock_raw(self, stock, last=100):
        db = self.firebase.database()
        if last<0:
            data = db.child(raw_db).child(stock).get()
        else:
            data = db.child(raw_db).child(stock).order_by_child('DATE').limit_to_last(last).get()
        return data

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
    price = fire.get_stock_daily('0829EA', last=1)
    pprint(price.val())
    price = fire.get_stock_raw('0829EA', last=2)
    pprint(price.val())
    #fire.purge_daily('0829EA', 1)
    price = fire.get_stock_raw('0829EA', last=1)
    newdata = price.each()[0].val()
    pprint(newdata)
