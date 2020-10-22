import os
import sys
import getopt
import pyrebase
import json
import plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from gspreaddb import GspreadDB
from utils import getstocklist
from datetime import datetime


DailyDbName = 'iopvdb-daily'
JsonFile = 'iopv.json'
OutputFile = 'etf_charts.html'
firebase_config_file = 'firebase_config.json'


def load_firebase_stock(db, dbase_id, stock):
    stocklist = db.child(dbase_id).child('Stock List').get()
    for st in stocklist.each():
        val = st.val()
        try:
            if val['STOCK'] == stock:
                ticker = val['TICKER']
                stock_data = db.child(dbase_id).child(ticker).get()
                records = []
                for rec in stock_data.each():
                    data = rec.val()
                    records.append(data)
                df = pd.DataFrame(records)
                return df
        except TypeError:
            pass
    return None

def load_gspread_stock(dailydb, stock):
    dailysheet = dailydb.getstocksheet(stock)
    df = pd.DataFrame(dailysheet.get_all_records())
    df['DATE'] = pd.to_datetime(df.DATE, format='%d/%m/%Y')
    return df


def load_stock_data(stock):
    df = pd.read_csv(stock)
    df['DATE'] = pd.to_datetime(df.DATE, format='%d/%m/%Y')
    return df


def make_candle(df, name='candle'):
    trace = {
        'x': df.DATE,
        'open': df.OPEN,
        'close': df.CLOSE,
        'high': df.HIGH,
        'low': df.LOW,
        'type': 'candlestick',
        'name': name,
        'showlegend': True
    }

    return trace


def make_moving_average(df, window=20, name='MA'):
    ma = df.CLOSE.rolling(window=window).mean()

    trace = {
        'x': df.DATE,
        'y': ma,
        'type': 'scatter',
        'mode': 'lines',
        'line': {
            'width': 1,
            'color': 'blue'
        },
        'name': f'{name}-{window}'
    }

    return ma


def generate_kdj(df, window=9):
    kv = []
    dv = []
    df_asc = df.sort_values(by='DATE', ascending=True)
    dates = list(df_asc.DATE)
    highs = list(df_asc.HIGH)
    lows = list(df_asc.LOW)
    closes = list(df_asc.CLOSE)

    for i, cn in enumerate(closes):
        if i+1 < window:
            kv.append('')
            dv.append('')
            continue

        start = i - window + 1
        end = i + 1
        ll = lows[start : end]
        hl = highs[start : end]
        ln = min(ll)
        hn = max(hl)
        rsv = (cn - ln) / (hn - ln) * 100
        kvn1 = kv[i-1] if kv[i-1] else 50
        dvn1 = dv[i-1] if dv[i-1] else 50
        kvn = kvn1*2/3 + rsv/3
        dvn = dvn1*2/3 + kvn/3
        kv.append(kvn)
        dv.append(dvn)

    ktrace = {
        'x': dates,
        'y': kv,
        'type': 'scatter',
        'mode': 'lines',
        'line': {
            'width': 1,
            'color': 'blue'
        },
        'name': f'K{window}'
    }

    dtrace = {
        'x': dates,
        'y': dv,
        'type': 'scatter',
        'mode': 'lines',
        'line': {
            'width': 1,
            'color': 'orange'
        },
        'name': f'D{window}'
    }

    return kv, dv


def get_change(df):
    close1 = df.CLOSE.iloc[-1]
    close2 = df.CLOSE.iloc[-2]
    change = close1 - close2
    pct = (close1/close2 - 1) * 100
    return close1, change, pct


def get_missing_dates(df):
    # build complete timepline from start date to end date
    start_date, end_date = sorted([df['DATE'].iloc[0], df['DATE'].iloc[-1]])
    all_dates = pd.date_range(start=start_date, end=end_date)

    # retrieve the dates that ARE in the original dataset
    in_dates = [d.strftime("%Y-%m-%d") for d in pd.to_datetime(df['DATE'])]

    # define dates with missing values
    missing = [d for d in all_dates.strftime("%Y-%m-%d").tolist() if d not in in_dates]
    return missing


def make_stock_charts(stocklist):
    assert slist

    dailydb = GspreadDB(DailyDbName, JsonFile)
    figs = []
    data_list = []
    for stock in stocklist:
        df = load_gspread_stock(dailydb, stock).sort_values('DATE')
        fig, data = make_chart(df, stock)
        figs.append(fig)
        data_list.append(data)

    backup = f'{OutputFile}.org'
    try:
        os.remove(backup)
    except FileNotFoundError:
        pass
    try:
        os.rename(OutputFile, backup)
    except FileNotFoundError:
        pass

    with open(OutputFile, 'a') as f:
        table = plot_table(data_list)
        f.write(table)
        for fig in figs:
            f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))



def make_firebase_charts(stocklist):
    assert slist

    with open(firebase_config_file) as f:
        firebase_config = json.load(f)

    firebase_connect = firebase_config['firebase_connect']
    dbase_id = firebase_config['firebase_dbase_id']
    firebase = pyrebase.initialize_app(firebase_connect)
    db = firebase.database()

    figs = []
    data_list = []
    for stock in stocklist:
        df = load_firebase_stock(db, dbase_id, stock).sort_values('DATE')
        fig, data = make_chart(df, stock)
        figs.append(fig)
        data_list.append(data)

    backup = f'{OutputFile}.org'
    try:
        os.remove(backup)
    except FileNotFoundError:
        pass
    try:
        os.rename(OutputFile, backup)
    except FileNotFoundError:
        pass

    with open(OutputFile, 'a') as f:
        table = plot_table(data_list)
        f.write(table)
        for fig in figs:
            f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))


def make_csv_chart(filename):
    df = pd.read_csv(filename)
    df['DATE'] = pd.to_datetime(df.DATE, format='%d/%m/%Y')
    df = df.sort_values('DATE')
    fig, _ = make_chart(df, filename)

    # make backup and write chart to html file
    outfile = 'sample.html'
    backup = f'{outfile}.org'
    try:
        os.remove(backup)
    except FileNotFoundError:
        pass
    try:
        os.rename(outfile, backup)
    except FileNotFoundError:
        pass
    with open(outfile, 'a') as f:
        f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))


def make_chart(df, stock):
    # generate stock indicators
    msize = 60
    mov = make_moving_average(df, window=msize)
    kv, dv = generate_kdj(df)
    cls, chg, chg_pct = get_change(df)

    # plot stock chart with embedded indicators
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Candlestick(x=df['DATE'], open=df['OPEN'], high=df['HIGH'],
                       low=df['LOW'], close=df['CLOSE'], name="Candle")
    )
    fig.add_trace(
        go.Scatter(x=df.DATE, y=mov, mode='lines', name=f"MA{msize}",
                   line={'color': "green"}),
    )
    fig.add_trace(
        go.Scatter(x=df.DATE, y=kv, name="K9",
                   line={'color': "blue"}), secondary_y=True
    )
    fig.add_trace(
        go.Scatter(x=df.DATE, y=dv, name="D9",
                   line={'color': "orange"}), secondary_y=True
    )

    # generate chart html
    # hide dates with no values
    missing_dates = get_missing_dates(df)
    fig.update_xaxes(rangebreaks=[dict(values=missing_dates)])
    dt = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    title = f'{stock}<br>'\
            f'<span style="font-size: 16px;">' \
            f'<b>K9:</b>{kv[-1]:.2f}  <b>D9:</b>{dv[-1]:.2f}  ' \
            f'<b>CLOSE:</b>{cls:.3f} {chg:+.3f} ({chg_pct:+.2f}%)  ' \
            f'<b>Date:</b>{dt}' \
            f'</span>'
    fig.update_layout(title_text=title, title_font_size=30, hovermode='x',
                      xaxis_rangeslider_visible=False)
    chart_data = {
        'CHART': fig,
        'STOCK': stock,
        'K9': kv[-1],
        'D9': dv[-1],
        'CLOSE': cls,
        'CHANGE': chg,
        'CHANGE%': chg_pct,
    }
    return fig, chart_data


def plot_table(data_list):
    stocks = [x['STOCK'] for x in data_list]
    k9 = [f"{x['K9']:.2f}" for x in data_list]
    d9 = [f"{x['D9']:.2f}" for x in data_list]
    closes = [f"{x['CLOSE']:.3f}" for x in data_list]
    change = [f"{x['CHANGE']:+.3f}" for x in data_list]
    change_pct = [f"{x['CHANGE%']:+.2f}" for x in data_list]

    fig = go.Figure(data=[go.Table(
        columnwidth=[300, 80, 80, 80, 80, 80],
        header=dict(values=['STOCK', 'K9', 'D9', 'CLOSE', 'CHANGE', 'CHANGE%'],
                    line_color='darkslategray',
                    fill_color='lightskyblue',
                    align='left'),
        cells=dict(values=[stocks, k9, d9, closes, change, change_pct],
                   line_color='darkslategray',
                   fill_color='white',
                   align='left'))
    ])

    fig.update_layout(height=400)
    return fig.to_html(full_html=False, include_plotlyjs='cdn')


def make_table(data_list):
    from prettytable import PrettyTable
    table = PrettyTable()
    table.format = True
    fields = ['STOCK', 'K9', 'D9', 'CLOSE', 'CHANGE', 'CHANGE%']
    table.field_names = fields
    for x in data_list:
        dlist = [
            x['STOCK'],
            f"{x['K9']:.2f}",
            f"{x['D9']:.2f}",
            f"{x['CLOSE']:.3f}",
            f"{x['CHANGE']:+.3f}",
            f"{x['CHANGE']:+.3f}"
        ]
        table.add_row(dlist)

    return table.get_html_string()


if __name__ == "__main__":
    argv = sys.argv[1:]
    slist = []
    CSVfile = None
    StockListFile = None
    UseFirebase = False
    try:
        # parse command line options
        opts, args = getopt.getopt(argv, 'fL:s:o:')
        Options = dict(opts)

        if '-L' in Options.keys():
            StockListFile = Options['-L']
        if '-s' in Options.keys():
            CSVfile = Options['-s']
        if '-o' in Options.keys():
            OutputFile = Options['-o']
        if '-f' in Options.keys():
            UseFirebase = True
    except getopt.GetoptError:
        print('Invalid command line option or arguments')
        sys.exit(2)

    if CSVfile:
        make_csv_chart(CSVfile)
    elif StockListFile:
        slist = getstocklist(StockListFile)
        if UseFirebase:
            make_firebase_charts(slist)
        else:
            make_stock_charts(slist)
    else:
        print("nothing to do")
