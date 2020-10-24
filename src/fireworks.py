import sys
import getopt
import json
import csv
import pyrebase
from pprint import pprint
from datetime import datetime
from pandas import to_datetime

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

    def update_stock_daily(self, stock, iopv, iopv_date, create=True):
        rec = self.get_stock_daily(stock, 1)
        date = str(to_datetime(iopv_date, format='%Y-%m-%d %H:%M:%S').date())
        db = self.firebase.database()
        if not rec.each() and create:
            data = {'DATE': date,
                    'OPEN': iopv,
                    'HIGH': iopv,
                    'LOW': iopv,
                    'CLOSE': iopv}
            db.child(daily_db).child(stock).push(data)
            return

        last = rec.each()[0]
        last_data = last.val()
        last_key = last.key()
        db = self.firebase.database()
        if last_data['DATE'] == date:
            data = last_data.copy()
            data['HIGH'] = iopv if iopv > data['HIGH'] else data['HIGH']
            data['LOW'] = iopv if iopv < data['LOW'] else data['LOW']
            data['CLOSE'] = iopv
            db.child(daily_db).child(stock).child(last_key).update(data)
        elif last_data['CLOSE'] != iopv:
            # only create data for new days if iopv changes since previous close
            data = {'DATE': date,
                    'OPEN': iopv,
                    'HIGH': iopv,
                    'LOW': iopv,
                    'CLOSE': iopv}
            db.child(daily_db).child(stock).push(data)

    def update_stock_raw(self, stock, data, create=True):
        latest = self.get_stock_raw(stock, last=2)

        db = self.firebase.database()
        if not latest.each() and create:
            # add new row
            db.child(raw_db).child(stock).push(data)
            return

        rows = latest.each()
        last_n1 = rows.pop()
        iopv_n1 = last_n1.val()['IOPV']
        try:
            last_n2 = rows.pop()
            iopv_n2 = last_n2.val()['IOPV']
        except IndexError:
            iopv_n2 = ''

        if data['IOPV'] != iopv_n1:
            # add new row
            db.child(raw_db).child(stock).push(data)
        elif iopv_n1 != iopv_n2:
            # add new row
            db.child(raw_db).child(stock).push(data)
        else:
            # update latest row in dbase
            db.child(raw_db).child(stock).child(last_n1.key()).set(data)

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
