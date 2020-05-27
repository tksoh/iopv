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

if __name__ == "__main__":
    from datetime import datetime
    now = datetime.now()
    nowtime = now.strftime("%d/%m/%Y %H:%M:%S")
        
    db = GspreadDB()
    
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
