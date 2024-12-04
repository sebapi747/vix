import requests, re, os
import datetime as dt, time
from lxml import html
import csv, json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import config

csvdirname=config.dirname + '/data'
picsdirname=config.dirname + '/pics'

def get_metadata():
    return {'Creator':os.uname()[1] +":"+__file__+":"+str(dt.datetime.utcnow())}

def sendTelegram(text):
    prefix = os.uname()[1] + __file__ + ":"
    params = {'chat_id': config.telegramchatid, 'text': prefix+text, 'parse_mode': 'markdown'}
    resp = requests.post('https://api.telegram.org/bot{}/sendMessage'.format(config.telegramtoken), params)
    resp.raise_for_status()

def write_url_to_file(url, filename):
    x = requests.get(url)
    print("%s %d" % (url,x.status_code))
    if x.status_code!=200:
        sendTelegram("error %s %d" % (url,x.status_code))
        raise Exception(url)
    f = open(filename, 'w')
    f.write(x.text)
    f.close()

def load_url_to_file(url, filename):
    now = str(dt.datetime.now())[0:10]
    if os.path.isfile(filename) == False:
        print("file %s does not exist" % filename)
        write_url_to_file(url, filename)
    elif time.strftime('%Y-%m-%d', time.localtime(os.path.getmtime(filename)))!=now:
        print("file %s last update %s" % (filename, time.localtime(os.path.getmtime(filename))))
        write_url_to_file(url, filename)  
    else:
        print("skipped file %s already recent" % filename)

def get_webdata():
    baseurl = 'https://www.cboe.com/us/futures/market_statistics/historical_data/'
    x = requests.get(baseurl)
    print("%s %d" % (baseurl,x.status_code))
    if x.status_code!=200:
        sendTelegram("error %s %d" % (baseurl,x.status_code))
        raise Exception(baseurl)
    parsed_body=html.fromstring(x.text)
    scripts = parsed_body.xpath('//script/text()')
    csvstr = scripts[2].split("\n")[2].split("=")[1]
    if csvstr[-1]==";":
        csvstr = csvstr[:-1]
    urls = json.loads(csvstr)
    href = []
    text = []
    for k in urls.keys():
        for u in urls[k]:
            text.append("%s (%s)" % (u['product_display'].replace("/"," "), dt.datetime.strftime(dt.datetime.strptime(u['expire_date'], "%Y-%m-%d"), '%b %Y')))
            href.append("https://cdn.cboe.com/%s" % u['path'])
    #href = parsed_body.xpath('//li[@class="mbn"]/a/@href')
    #text = parsed_body.xpath('//li[@class="mbn"]/a/text()')
    now = dt.datetime.utcnow()
    for i in range(0, len(href)):
        t = text[i]
        h = href[i]
        if re.search("VXT ",t):
            ymdstr = h[77:-4]
            ticker = "VX-Mat-%s" % ymdstr
            filename =  "%s/%s.csv" % (csvdirname,ticker)
            fileexists = os.path.isfile(filename)
            if fileexists==False or pd.to_datetime(ymdstr) > now:
                url = h
                load_url_to_file(url, filename)

## load csv data into dataframe
def readdf(csvdirname):
    print(csvdirname)
    dflist = []
    for f in os.listdir(csvdirname):
        if "VX" not in f:
            continue
        filename = "%s/%s" % (csvdirname, f)
        thisdf = pd.read_csv(filename)
        thisdf['futmat'] = f[7:17]
        dflist.append(thisdf)
    df = pd.concat(dflist)
    df['date'] = pd.to_datetime(df['Trade Date'])
    df['futmat'] = pd.to_datetime(df['futmat'])
    return df

# get series info
def series_info(df):
    tickers = df['Futures'].unique()
    datesinfo = [] 
    # datesinfo.set_index('Futures')
    for f in tickers:
        dlist = df.loc[df['Futures']==f]['date']
        datesinfo.append({'Futures': f, 'min': np.min(dlist), 'max': np.max(dlist)})
    datesinfo = pd.DataFrame(datesinfo).sort_values(['min','max'])
    datesinfo['lasted'] = datesinfo['max']-datesinfo['min']
    datesinfo['index'] = range(0,len(datesinfo))
    # datesinfo.to_csv("datesinfo.csv")
    return datesinfo

# plot all quotes tab
def plot_raw(df, datesinfo, nb=24):
    n = len(datesinfo)
    start = n-nb
    if nb==0:
        start=0
    fig = plt.figure(1)
    for f in datesinfo['Futures'][n-nb:]:
        selector = (df['Futures']==f) & (df['Close']!=0)
        colnames = ["date", "Close"]
        selector = (df['Futures']==f) & (df['Close']!=0)
        ts = df.loc[selector,colnames].sort_values('date')
        plt.plot(ts[colnames[0]], ts[colnames[1]], label=f)
    #plt.legend()
    plt.xticks(rotation='vertical')
    plt.ylabel('vix future quote')
    plt.title("Vix Quotes All Tenors\nlast price: %s" % str(np.max(df['date']))[:10])
    plt.savefig('%s/vix%d.png' % (picsdirname, nb),metadata=get_metadata())
    plt.close(fig)

def cont_futures(df, datesinfo):
    datelist = np.sort(df['date'].unique())
    spread = np.full((len(datelist),13), np.nan)
    datemat = np.full((len(datelist),13), np.nan)
    futreflist = []
    for idate in range(0, len(datelist)):
        d = datelist[idate]
        daydata = df.loc[(df['date']==d) & (df['Close']!=0)]
        if len(daydata)==0:
            futreflist.append("")
            continue
        futureslist = daydata['Futures']
        a = datesinfo['Futures'] == 0
        for f in futureslist:
            a |= datesinfo['Futures'] == f
        futuresinfo = datesinfo.loc[a]
        futref = futuresinfo.iloc[0]['Futures']
        futreflist.append(futref)
        futmatref = daydata[daydata['Futures']==futref]['futmat'].iloc[0]
        spread[idate,1] = (float)(daydata[daydata['Futures']==futref]['Close'].iloc[0])
        datemat[idate,0] = 0
        datemat[idate,1] = (futmatref-d).total_seconds()/3600./24./365.
        if futmatref<d:
            raise Exception("error %s %s" % (str(futmatref),str(d)))
        for i in range(1,len(futuresinfo)):
            fut = futuresinfo.iloc[i]['Futures']
            futmat = daydata[daydata['Futures']==fut]['futmat'].iloc[0]
            futquote = (float)(daydata[daydata['Futures']==fut]['Close'].iloc[0])
            delta = futmat - futmatref
            j = (int)(np.round(delta.total_seconds()/3600/24/30,0))
            spread[idate,j+1] = futquote 
            datemat[idate,j+1] = (futmat-d).total_seconds()/3600./24./365.
        if idate % 100==0:
            print("processed %d/%d" % (idate,len(datelist)))
    spreaddf = pd.DataFrame(spread)
    spreaddf['date'] = datelist
    spreaddf['futref'] = futreflist
    spreaddf.to_csv(csvdirname + "/futbymonth.csv")
    datematdf = pd.DataFrame(datemat)
    datematdf['date'] = datelist
    datematdf['futref'] = futreflist
    datematdf.to_csv(csvdirname + "/datebymonth.csv")
    #spreaddf.describe()
    return spreaddf, datematdf
    

def get_vix_hist():
    url = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
    ticker = "VIX" 
    filename =  "%s/%s.csv" % (csvdirname,ticker)
    load_url_to_file(url, filename)
    vixdf = pd.read_csv(filename)
    vixdf['date'] = pd.to_datetime(vixdf['DATE'])
    return vixdf

def join_df(vixdf, spreaddf):
    joined = spreaddf.set_index('date').join(vixdf.set_index('date'), rsuffix="2", how='left')
    spreaddf[0] = joined['CLOSE'].array
    return spreaddf

def plot_cont(spreaddf):
    fig = plt.figure(1)
    for i in [0,1,2,8]:
        lastspread = spreaddf[i].dropna().iloc[-1]
        q = np.quantile(spreaddf[i].dropna(), [0, 0.25, 0.5,0.75, 1])
        stats= "%d mth: last=%.0f min=%.0f q25=%.0f q50=%.0f q75=%.0f max=%.0f" % (i,lastspread, q[0], q[1],q[2],q[3], q[4])
        plt.plot(spreaddf['date'], spreaddf[i], label=stats)
    plt.legend()
    plt.title("VIX Cont Future\nlast price: %s" % str(np.max(spreaddf['date']))[:10])
    plt.savefig(picsdirname+'/vix_cont.png',metadata=get_metadata())
    plt.close(fig)

def plot_cont_spread(spreaddf):
    i1, i2 = 8,1
    fig = plt.figure(1)
    for i2 in [1,2]:
        spread = spreaddf[i2]-spreaddf[i1]
        lastspread = spread.dropna().iloc[-1]
        q = np.quantile(spread.dropna(), [0, 0.25, 0.5,0.75, 1])
        stats= "%d-%d last=%.2f min=%.2f q25=%.2f q50=%.2f q75=%.2f max=%.2f" % (i1,i2,lastspread, q[0], q[1],q[2],q[3], q[4])
        plt.plot(spreaddf['date'], spreaddf[i2]-spreaddf[i1], label=stats)
    plt.legend()
    plt.title("Vix Future Spread\nlast price: %s" % str(np.max(spreaddf['date']))[:10])
    plt.axhline(y=0, color='black')
    #plt.xlabel(stats)
    plt.savefig(picsdirname+'/vix_spread.png',metadata=get_metadata())
    plt.close(fig)

def graph_vix_curve(datematdf, spreaddf):
    fig = plt.figure(1)
    i = -1
    plt.plot(datematdf.iloc[i][np.arange(0,10)], spreaddf.iloc[i][np.arange(0,10)], label=str(datematdf.iloc[i]['date'])[0:10], marker="o")
    i = -2
    plt.plot(datematdf.iloc[i][np.arange(0,10)], spreaddf.iloc[i][np.arange(0,10)], label=str(datematdf.iloc[i]['date'])[0:10], marker="o", color="gray")
    i = -3
    plt.plot(datematdf.iloc[i][np.arange(0,10)], spreaddf.iloc[i][np.arange(0,10)], label=str(datematdf.iloc[i]['date'])[0:10], marker="o", color="lightgray")
    plt.legend()
    plt.title("Vix Future Curve")
    plt.xlabel("maturity in years")
    plt.ylabel("1month VIX index Quote")
    plt.savefig(picsdirname+'/vix_curve.png',metadata=get_metadata())
    plt.close(fig)

get_webdata()
df = readdf(csvdirname)
datesinfo = series_info(df)
plot_raw(df, datesinfo, 24)
plot_raw(df, datesinfo, 0)
spreaddf, datematdf = cont_futures(df, datesinfo)
vixdf = get_vix_hist()
spreaddf = join_df(vixdf, spreaddf)
plot_cont(spreaddf)
plot_cont_spread(spreaddf)
graph_vix_curve(datematdf, spreaddf)
os.system('rsync -avzhe ssh %s %s' % (picsdirname, config.remotedir))
sendTelegram("INFO:regenerated [vix data](https://www.markowitzoptimizer.pro/blog/32)")
