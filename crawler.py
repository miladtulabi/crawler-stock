import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
import datetime
from dateutil.parser import parse
#from transformers import pipeline

headline_without_ = np.zeros([30,1]).tolist()
headline_time_list = np.zeros([30,1]).tolist()
stock = np.zeros([30,1]).tolist()
goolge_stock = []
goolge_stock_time = []
#sentiment_pipeline = pipeline("sentiment-analysis")

d = 0

for k in range(1,40):
    url = 'https://----/page/{}/'.format(k)
    response = requests.get(url)


    soup = BeautifulSoup(response.text, 'html.parser')
    headline = soup.find_all('h2', attrs={'class':'post-block__title'})
    headline_time = soup.find_all('time', attrs={'class':'river-byline__time'})
    #headline_cont = soup.find_all('div', attrs={'class':'post-block__content'})

    a = parse(re.sub(r'\s+', ' ', headline_time[0].text).strip()).date()
    b = 0
    if k==1:
        headline_time_list[0].append(parse(re.sub(r'\s+', ' ', headline_time[0].text).strip()).date())
    

    for i in range(len(headline)):
        b = parse(re.sub(r'\s+', ' ', headline_time[i].text).strip()).date()
        if b == a:
            headline_without_[d].append(re.sub(r'\s+', ' ', headline[i].text).strip())

            #content.append(re.sub(r'\s+', ' ', headline_cont[i].text).strip())
            a = b
            #sent = sentiment_pipeline(data)
        else:
            d += 1
            a = b
            headline_without_[d].append(re.sub(r'\s+', ' ', headline[i].text).strip())
            headline_time_list[d].append(parse(re.sub(r'\s+', ' ', headline_time[i].text).strip()).date())


url_api = 'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2022-10-27/2022-11-12?adjusted=true&sort=asc&limit=120&apiKey=WpWYDCmtrCIQpFM7V0LA5Q2mLbRhiciF'
response_api = requests.get(url_api)

dict_headline = {
    'Date': headline_time_list,
    'Headline': headline_without_,
    #'Content': content
#    'Google Stock Value': goolge_stock,
    }

for j in range(11):
    # from 10 Nov to 27 oct
    goolge_stock_time.append(datetime.datetime.utcfromtimestamp(response_api.json()['results'][j]['t']/1000).date())
    goolge_stock.append(response_api.json()['results'][j]['c'])

s1=pd.Series(goolge_stock,name='Google Stock')
s2=pd.Series(goolge_stock_time,name='Date')
stock_1 = pd.concat([s1, s2], axis=1)


df = pd.DataFrame(dict_headline)
df_1 = pd.DataFrame(df['Headline'].tolist()).iloc[:,1:]
df_2 = pd.DataFrame(df['Date'].tolist()).iloc[:,1:]
df_2.rename(columns = {1:'Date'}, inplace = True)
df_compare_1 = df_2['Date'].isin(stock_1['Date'])
df_compare_2 = stock_1['Date'].isin(df_2['Date'])
df_compare_1.name = 'COMP'
df_compare_2.name = 'COMP'

df_con_1 = pd.concat([df_compare_1, df_2, df_1], axis=1, join='inner')
df_con_2 = pd.concat([df_compare_2, stock_1], axis=1, join='inner')

df_out_1 = df_con_1.loc[df_con_1.COMP==True]
df_out_2 = df_con_2.loc[df_con_2.COMP==True]
df_out_2.sort_values(by='Date', ascending = False, inplace=True)

df_out_3 = pd.merge(df_out_2, df_out_1, on = 'Date', how='inner')
df_out_3 = df_out_3.drop(['COMP_x', 'COMP_y'], axis=1)

df_out_3.to_csv("./Headlines_Stock.csv", sep=',',index=False)