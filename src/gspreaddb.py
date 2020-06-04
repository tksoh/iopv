import gspread
from oauth2client.service_account import ServiceAccountCredentials

class GspreadDB:    
    def __init__(self, wbname='iopvdb', json='iopv.json'):
        # connect to google spreadsheet
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(json, scope)
        client = gspread.authorize(creds)
        self.workbook = client.open(wbname)
        self.stocklist = self.workbook.worksheet('Stock List')
        self.logsheet = self.workbook.worksheet('LOG')
    
    def getstockticker(self, stockname):
        # find and return existing stock
        slist = self.stocklist.get_all_records()
        for row in slist:
            if row['STOCK'] == stockname:
                return row['TICKER']
                
        # stock not exist
        return None
        
    def getstocksheet(self, stockname):
        ticker = self.getstockticker(stockname)
        try:
            return self.workbook.worksheet(ticker)
        except:
            raise ValueError("Undefined stock '%s'" % stockname)    


    def add(self, stockname, time, iopv):
        sheet = self.getstocksheet(stockname)
        sheet.append_row([time, iopv])
        
    def log(self, time, msg):
        self.logsheet.append_row([time, str(msg)])

    def getdatarow(self, stockname, row):
        sheet = self.getstocksheet(stockname)
        return sheet.row_values(row+1)

    def getcolnames(self, stockname):
        sheet = self.getstocksheet(stockname)
        return sheet.row_values(1)

if __name__ == "__main__":
    from datetime import datetime
    import getopt
    import sys
    import pprint

    argv = sys.argv[1:]
    writetodb = False
    jsonfile = 'iopv.json'
    try:
        # parse command line options
        opts, args = getopt.getopt(argv, 'wj:')
        Options = dict(opts)
        if '-w' in Options.keys():
            writetodb = True
        if '-j' in Options.keys():
            jsonfile = Options['-j']
    except getopt.GetoptError:
        #Print a message or do something useful
        print('Invalid command line option or arguments')
        sys.exit(2)

    # access the database
    db = GspreadDB(json=jsonfile)

    # make sure we can read the database
    colnames = db.getcolnames("TESTING")
    rowdata = db.getdatarow("TESTING", 1)
    pprint.pprint([colnames, rowdata])

    # test write to database
    if writetodb:
        now = datetime.now()
        nowtime = now.strftime("%d/%m/%Y %H:%M:%S")
        
        # write to a valid sample stock sheet for testing
        db.add("TESTING", nowtime, 1.234)

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
