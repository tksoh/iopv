import os
import sys
import getopt
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
    for stock in stocklist:
        df = load_gspread_stock(dailydb, stock).sort_values('DATE')
        figs.append(make_chart(df, stock))

    try:
        os.rename(OutputFile, f'{OutputFile}.org')
    except FileNotFoundError:
        pass

    with open(OutputFile, 'a') as f:
        for fig in figs:
            f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))

def make_csv_chart(filename):
    df = pd.read_csv(filename).sort_values('DATE')
    fig = make_chart(df, filename)
    outfile = 'sample.html'
    try:
        os.rename(outfile, f'{outfile}.org')
    except FileNotFoundError:
        pass
    with open(outfile, 'a') as f:
        f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))


def make_chart(df, stock):
    # generate stock indicators
    msize = 60
    mov = make_moving_average(df, window=msize)
    kv, dv = generate_kdj(df)

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
            f'<b>Date:</b>{dt}' \
            f'</span>'
    fig.update_layout(title_text=title, title_font_size=30, hovermode='x')
    return fig

if __name__ == "__main__":
    argv = sys.argv[1:]
    slist = []
    CSVfile = None
    StockListFile = None
    try:
        # parse command line options
        opts, args = getopt.getopt(argv, 'L:s:o:')
        Options = dict(opts)

        if '-L' in Options.keys():
            StockListFile = Options['-L']
        if '-s' in Options.keys():
            CSVfile = Options['-s']
        if '-o' in Options.keys():
            OutputFile = Options['-o']
    except getopt.GetoptError:
        print('Invalid command line option or arguments')
        sys.exit(2)

    if CSVfile:
        make_csv_chart(CSVfile)
    elif StockListFile:
        slist = getstocklist(StockListFile)
        make_stock_charts(slist)
    else:
        print("nothing to do")
