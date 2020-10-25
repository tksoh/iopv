import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import WorksheetNotFound


class GspreadDB:
    def __init__(self, wbname, dbtype, json='iopv.json'):
        assert dbtype in ['DAILY', 'IOPV']

        # connect to google spreadsheet
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(json, scope)
        client = gspread.authorize(creds)
        self.dbtype = dbtype
        self.workbook = client.open(wbname)
        self.logsheet = self.workbook.worksheet('LOG')
        self.stocklist = self.workbook.worksheet('Stock List').get_all_records()
        self.stocksheet = {}

    def getstockticker(self, stockname):
        # find and return existing stock
        for row in self.stocklist:
            if row['STOCK'] == stockname:
                return row['TICKER']
                
        # stock not exist
        return None
        
    def getstocksheet(self, stockname):
        if stockname in self.stocksheet:
            return self.stocksheet[stockname]

        ticker = self.getstockticker(stockname)
        try:
            sh = self.workbook.worksheet(ticker)
            self.stocksheet[stockname] = sh
            return sh
        except WorksheetNotFound:
            raise ValueError("Undefined stock '%s'" % stockname)

    def append(self, stockname, time, iopv):
        sheet = self.getstocksheet(stockname)
        sheet.append_row([time, iopv], value_input_option='USER_ENTERED')

    def add(self, stockname, time, iopv):
        sheet = self.getstocksheet(stockname)
        sheet.insert_row([time, iopv], 2, value_input_option='USER_ENTERED')

    def addchange(self, stockname, time, iopv):
        sheet = self.getstocksheet(stockname)
        iopv0 = float(iopv)

        try:
            iopv1 = float(sheet.get('B2', value_render_option='UNFORMATTED_VALUE')[0][0])
        except KeyError:
            iopv1 = -1
        try:
            iopv2 = float(sheet.get('B3', value_render_option='UNFORMATTED_VALUE')[0][0])
        except KeyError:
            iopv2 = -1

        if iopv0 != iopv1:
            sheet.insert_row([time, iopv], 2, value_input_option='USER_ENTERED')
        elif iopv1 != iopv2:
            sheet.insert_row([time, iopv], 2, value_input_option='USER_ENTERED')
        else:
            sheet.update('A2', time, value_input_option='USER_ENTERED')

    def log(self, time, msg):
        self.logsheet.insert_row([time, str(msg)], 2, value_input_option='USER_ENTERED')

    def getdatarow(self, stockname, row):
        sheet = self.getstocksheet(stockname)
        return sheet.row_values(row+1)

    def getheaders(self, stockname):
        sheet = self.getstocksheet(stockname)
        return sheet.row_values(1)

    def initsheet(self, stockname):
        if self.dbtype == 'DAILY':
            headers = ['DATE','OPEN','HIGH','LOW','CLOSE','REMARK']
        else:
            headers = ['TIME', 'IOPV']

        try:
            sheet = self.getstocksheet(stockname)
            sheet.clear()
        except ValueError as ve:
            shname = self.getstockticker(stockname)
            sheet = self.workbook.add_worksheet(title=shname, rows="100", cols="20")

        sheet.update('A1', [headers], value_input_option='USER_ENTERED')
        return sheet

    def deletesheet(self, stockname):
        sheet = self.getstocksheet(stockname)
        self.workbook.del_worksheet(sheet)

if __name__ == "__main__":
    from datetime import datetime
    import getopt
    import sys
    import pprint

    argv = sys.argv[1:]
    writetodb = False
    jsonfile = 'iopv.json'
    wbname = 'iopvdb'
    try:
        # parse command line options
        opts, args = getopt.getopt(argv, 'b:wj:')
        Options = dict(opts)
        if '-w' in Options.keys():
            writetodb = True
        if '-b' in Options.keys():
            wbname = Options['-b']
        if '-j' in Options.keys():
            jsonfile = Options['-j']
    except getopt.GetoptError:
        #Print a message or do something useful
        print('Invalid command line option or arguments')
        sys.exit(2)

    # access the database
    db = GspreadDB(wbname, jsonfile)

    # make sure we can read the database
    colnames = db.getheaders("TESTING")
    rowdata = db.getdatarow("TESTING", 1)
    pprint.pprint([colnames, rowdata])

    # test write to database
    if writetodb:
        now = datetime.now()
        nowtime = now.strftime("%d/%m/%Y %H:%M:%S")

        # write to a valid sample stock sheet for testing
        db.addchange("TESTING", nowtime, 1.235)

        # write to unlisted stocks
        try:
            db.add("XXX", nowtime, 1.234)
        except ValueError as ve:
            db.log(nowtime, ve)
            print(ve)

        # write to missing stocks sheet
        try:
            db.add("ABF Malaysia Bond Index Fund", nowtime, 1.234)
        except ValueError as ve:
            db.log(nowtime, ve)
            print(ve)
