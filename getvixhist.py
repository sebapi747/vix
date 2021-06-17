import requests, re, os
import datetime as dt
from lxml import html
import csv
csvdirname='/home/pi/work/vix/data'

baseurl = 'https://www.cboe.com/us/futures/market_statistics/historical_data/'
x = requests.get(baseurl)
print("%s %d" % (url,x.status_code))
parsed_body=html.fromstring(x.text)
href = parsed_body.xpath('//li[@class="mbn"]/a/@href')
now = dt.datetime.utcnow()
for h in href:
    ticker = "VX-Mat-%s" % h[16:-1]
    filename =  "%s/%s.csv" % (csvdirname,ticker)
    fileexists = os.path.isfile(filename)
    if fileexists==False or pd.to_datetime(h[16:-1]) > now:
        url = baseurl+h
        x = requests.get(url)
        print("%s %d" % (url,x.status_code))
        f = open(filename, 'w')
        f.write(x.text)
        f.close()
