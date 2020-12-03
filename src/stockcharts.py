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
from fireworks import Firework

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
        ll = lows[start: end]
        hl = highs[start: end]
        ln = min(ll)
        hn = max(hl)
        if hn == ln:
            rsv = 0
        else:
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

    kv = [x if x == "" else round(x, 2) for x in kv]
    dv = [x if x == "" else round(x, 2) for x in dv]
    return kv, dv


def get_change(df):
    close1 = df.CLOSE.iloc[-1]
    close2 = df.CLOSE.iloc[-2]
    change = close1 - close2
    pct = (close1/close2 - 1) * 100
    return close1, change, pct


def make_changes(df):
    df_asc = df.sort_values(by='DATE', ascending=True)
    closes = list(df_asc.CLOSE)

    changes = [0]
    change_pcts = [0]
    for i in range(len(closes)-1):
        close1 = closes[i+1]
        close0 = closes[i]
        change = close1 - close0
        pct = (close1/close0 - 1) * 100
        changes.append(f'{change:+.3f}')
        change_pcts.append(f'{pct:+.2f}')

    return changes, change_pcts


def get_missing_dates(df):
    # build complete timepline from start date to end date
    start_date, end_date = sorted([df['DATE'].iloc[0], df['DATE'].iloc[-1]])
    all_dates = pd.date_range(start=start_date, end=end_date)

    # retrieve the dates that ARE in the original dataset
    in_dates = [d.strftime("%Y-%m-%d") for d in pd.to_datetime(df['DATE'])]

    # define dates with missing values
    missing = [d for d in all_dates.strftime("%Y-%m-%d").tolist() if d not in in_dates]
    return missing


def get_missing_minutes(df):
    # build complete timeline from start date to end date
    all_dates = []
    start_date, end_date = sorted([df['DATE'].iloc[0], df['DATE'].iloc[-1]])
    sd = pd.to_datetime(start_date).strftime("%Y-%m-%d %H:%M")
    ed = pd.to_datetime(start_date).strftime("%Y-%m-%d 17:55")
    dr = pd.date_range(start=sd, end=ed, freq='5T').strftime("%Y-%m-%d %H:%M").tolist()
    all_dates += dr

    sd = pd.to_datetime(end_date).strftime("%Y-%m-%d 09:05")
    ed = pd.to_datetime(end_date).strftime("%Y-%m-%d %H:%M")
    dr = pd.date_range(start=sd, end=ed, freq='5T').strftime("%Y-%m-%d %H:%M").tolist()
    all_dates += dr

    unique_days = sorted(list(set([d.strftime("%Y-%m-%d") for d in pd.to_datetime(df['DATE'])])))
    for d in unique_days[1:-1]:
        sd = pd.to_datetime(d).strftime("%Y-%m-%d 09:05")
        ed = pd.to_datetime(d).strftime("%Y-%m-%d 17:55")
        dr = pd.date_range(start=sd, end=ed, freq='5T').strftime("%Y-%m-%d %H:%M").tolist()
        all_dates += dr

    # retrieve the dates that ARE in the original dataset
    in_dates = [d.strftime("%Y-%m-%d %H:%M") for d in pd.to_datetime(df['DATE'])]

    # define dates with missing values
    missings = [d for d in all_dates if d not in in_dates]
    return missings


def html_title():
    dt = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    title = f'<div style="margin-left: 5em;">' \
            f'<title>Bursa ETF IOPV</title>' \
            f'<h2>Bursa ETF IOPV</h2>' \
            f'<h><i>{dt}</i></h>' \
            f'</div>\n\n'
    return title


def make_stock_charts(stocklist):
    assert slist

    dailydb = GspreadDB(DailyDbName, 'DAILY', JsonFile)
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
        f.write(html_title())
        table = plot_table(data_list)
        f.write(table)
        for fig in figs:
            f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))


def make_ohlc(raw_iopv):
    ohlc_list = []
    last_iopv = None
    for rec in raw_iopv:
        date = rec['DATE']
        iopv = rec['IOPV']
        op = last_iopv if last_iopv else iopv
        cl = iopv
        hi = max(cl, op)
        lo = min(cl, op)
        #ohlc = {'DATE': date, 'OPEN': op, 'HIGH': hi, 'LOW': lo, 'CLOSE': cl}
        ohlc = {'DATE': date, 'OPEN': iopv, 'HIGH': iopv, 'LOW': iopv, 'CLOSE': iopv}
        ohlc_list.append(ohlc)
        last_iopv = iopv
    return ohlc_list


def precond_daily(ohlc_list):
    new_list = []
    for rec in ohlc_list:
        if rec['OPEN'] == rec['HIGH'] == rec['LOW'] == rec['LOW']:
            if new_list and new_list[-1]['CLOSE'] == rec['OPEN']:
                continue
        new_list.append(rec)
    return new_list


def precond_minutes(iopv_list):
    new_list = []
    last_iopv = ''
    for rec in iopv_list:
        date = rec['DATE']
        itime = pd.to_datetime(date).strftime("%H:%M")
        if itime >= '17:00' or itime < '09:00':
            continue
        iopv = rec['IOPV']
        if iopv != last_iopv:
            new_list.append(rec)
        last_iopv = iopv
    return new_list


def make_firebase_charts(stocklist):
    assert slist

    fire = Firework()
    stock_dict = dict([(x, fire.get_stock_ticker(x)) for x in stocklist])
    stock_daily = fire.get_stock_daily(stock_dict.values(), last=200)
    stock_raw = fire.get_stock_raw(stock_dict.values(), last=1200)
    figs = []
    data_list = []
    for stock in stocklist:
        ticker = stock_dict[stock]
        stock_daily_data = precond_daily(stock_daily[ticker])
        df = pd.DataFrame(stock_daily_data).sort_values('DATE')
        fig, data = make_chart(df, f'{stock} [Day]')
        figs.append(fig)
        data_list.append(data)

        stock_raw_data = precond_minutes(stock_raw[ticker])
        df = pd.DataFrame(make_ohlc(stock_raw_data[-120:])).sort_values('DATE')
        fig, data = make_minute_chart(df, f'{stock} [5-min]')
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
        f.write(html_title())
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
        f.write(html_title())
        f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))


def make_chart(df, stock):
    # generate stock indicators
    mov60 = make_moving_average(df, window=60)
    mov20 = make_moving_average(df, window=20)
    kv, dv = generate_kdj(df)
    cls, chg, chg_pct = get_change(df)
    changes, change_pcts = make_changes(df)

    # display daily change info on candlestick's hover
    hovertext = []
    for i in range(len(df.OPEN)):
        hovertext.append(
            # f'Open: {df.OPEN[i]}' +
            # f'<br>High: {df.HIGH[i]}' +
            # f'<br>Low: {df.LOW[i]}' +
            # f'<br>Close: {df.CLOSE[i]}' +
            f'<br>K9: {kv[i]}' +
            f'<br>Chg: {changes[i]} ' +
            f'<br>Chg: {change_pcts[i]}%'
        )

    # plot stock chart with embedded indicators
    fig = make_subplots(rows=2, cols=1,
                        row_heights=[0.70, 0.30],
                        vertical_spacing=0.02,
                        shared_xaxes=True,
                        specs=[[{"secondary_y": True}], [{"secondary_y": True}]])
    fig.add_trace(
        go.Candlestick(x=df['DATE'], open=df['OPEN'], high=df['HIGH'],
                       low=df['LOW'], close=df['CLOSE'], name="Candle",
                       text=hovertext,
                       # hoverinfo='text'
                       ),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=df.DATE, y=mov60, mode='lines', name=f"MA60",
                   line={'color': "green"}),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=df.DATE, y=mov20, mode='lines', name=f"MA20",
                   line={'color': "yellow"}),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=df.DATE, y=kv, name="K9",
                   line={'color': "blue"}), secondary_y=False,
        row=2, col=1
    )
    fig.add_trace(
        go.Scatter(x=df.DATE, y=dv, name="D9",
                   line={'color': "orange"}), secondary_y=False,
        row=2, col=1
    )

    chg_colors = ['#45ad57' if float(x) >= 0 else '#ff9166' for x in changes]
    pct_colors = ['#2ca02c' if float(x) >= 0 else '#ad5a45' for x in change_pcts]
    fig.add_trace(
        go.Bar(x=df.DATE, y=changes, name="CHANGE",
               marker_color=chg_colors), secondary_y=False,
        row=2, col=1
    )
    fig.add_trace(
        go.Bar(x=df.DATE, y=change_pcts, name="CHANGE%",
               marker_color=pct_colors), secondary_y=False,
        row=2, col=1
    )

    # generate chart html
    # hide dates with no values
    missing_dates = get_missing_dates(df)
    fig.update_xaxes(rangebreaks=[dict(values=missing_dates)])
    fig.update_yaxes(fixedrange=True)
    dt = df.DATE.iloc[-1]
    title = f'{stock}<br>'\
            f'<span style="font-size: 16px;">' \
            f'<b>K9:</b>{kv[-1]:.2f}  <b>D9:</b>{dv[-1]:.2f}  ' \
            f'<b>CLOSE:</b>{cls:.3f} {chg:+.3f} ({chg_pct:+.2f}%)  ' \
            f'<b>Date:</b>{dt}' \
            f'</span>'
    fig.update_layout(title_text=title, title_font_size=30, hovermode='x', barmode='stack',
                      xaxis_rangeslider_visible=False, height=650)
    fig.update_yaxes(
        row=2, col=1,
        range=[-10, 100],
        autorange=False,
        dtick=20
    )
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


def make_minute_chart(df, stock):
    # generate stock indicators
    mov5 = make_moving_average(df, window=5)
    mov20 = make_moving_average(df, window=20)
    kv, dv = generate_kdj(df)
    cls, chg, chg_pct = get_change(df)

    # plot stock chart with embedded indicators
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(x=df['DATE'], y=df['CLOSE'],
                   mode='lines+markers', marker_size=5,
                   line={'color': "#2fd479"}, name="IOPV"),
    )
    fig.add_trace(
        go.Scatter(x=df.DATE, y=mov5, mode='lines', name=f"MA5",
                   line={'color': "#349feb"})
    )
    fig.add_trace(
        go.Scatter(x=df.DATE, y=mov20, mode='lines', name=f"MA20",
                   line={'color': "yellow"})
    )
    fig.add_trace(
        go.Scatter(x=df.DATE, y=kv, mode='lines', name="K9",
                   visible='legendonly', line={'color': "blue"}),
        secondary_y=True,
    )
    fig.add_trace(
        go.Scatter(x=df.DATE, y=dv, mode='lines', name="D9",
                   visible='legendonly', line={'color': "orange"}),
        secondary_y=True
    )

    ### generate chart html ###
    # remove the gaps between timestamp
    missings = get_missing_minutes(df)
    fig.update_yaxes(fixedrange=True)
    fig.update_xaxes(rangebreaks=[
            dict(dvalue=5*60*1000, values=missings),
            dict(bounds=["sat", "mon"]),
            dict(bounds=[18, 9], pattern="hour")]
    )

    dt = df.DATE.iloc[-1]
    title = f'{stock}<br>'\
            f'<span style="font-size: 16px;">' \
            f'<b>K9:</b>{kv[-1]:.2f}  <b>D9:</b>{dv[-1]:.2f}  ' \
            f'<b>Date:</b>{dt}' \
            f'</span>'
    fig.update_layout(title_text=title, title_font_size=30, hovermode='x', barmode='stack',
                      xaxis_rangeslider_visible=False, height=300)
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
    from plotly.colors import n_colors
    color_scales = n_colors('rgb(0, 255, 0)', 'rgb(255, 0, 0)', 100, colortype='rgb')

    stocks = [x['STOCK'] for x in data_list]
    k9 = [f"{x['K9']:.2f}" for x in data_list]
    d9 = [f"{x['D9']:.2f}" for x in data_list]
    closes = [f"{x['CLOSE']:.3f}" for x in data_list]
    change = [f"{x['CHANGE']:+.3f}" for x in data_list]
    change_pct = [f"{x['CHANGE%']:+.2f}" for x in data_list]

    # setup cell colors
    headers = ['STOCK', 'K9', 'D9', 'CLOSE', 'CHANGE', 'CHANGE%']
    row_num = len(stocks)
    col_num = len(headers)
    odd_even_colors = ('white', 'beige')
    row_colors = [odd_even_colors[x % 2] for x in range(row_num)]

    blacks = ['black'] * row_num
    k9_colors = [color_scales[int(float(k))] for k in k9]
    chg_colors = [('#db0000', '#02cf4d')[int(x['CHANGE'] >= 0)] for x in data_list]
    cell_colors = [blacks, k9_colors, blacks, blacks, chg_colors, chg_colors]

    fig = go.Figure(data=[go.Table(
        columnwidth=[300, 80, 80, 80, 80, 80],
        header=dict(values=headers,
                    line_color='darkslategray',
                    fill_color='lightskyblue',
                    align='left'),
        cells=dict(values=[stocks, k9, d9, closes, change, change_pct],
                   line_color='darkslategray',
                   fill_color=[row_colors * col_num],
                   font=dict(color=cell_colors),
                   align='left'))
    ])

    fig.update_layout(height=250 + len(data_list)*25)
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
