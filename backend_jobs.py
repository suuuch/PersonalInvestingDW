import datetime
import time
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup
import yfinance as yf

from common.databaseEngine import engine


class IndexComponent(object):
    def get_wiki_component(self, url):
        resp = requests.get(url)
        soup = BeautifulSoup(resp.text, 'lxml')
        content = soup.find('table', attrs={'id': 'constituents'})
        companies = pd.read_html(StringIO(str(content)))[0]
        return companies

    def index_sp_500(self):
        index_name = '^GSPC'
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

        companies = self.get_wiki_component(url)
        companies['IndexName'] = index_name
        return companies

    def index_naq_100(self):
        index_name = '^IXIC_100'
        url = 'https://en.wikipedia.org/wiki/Nasdaq-100'
        companies = self.get_wiki_component(url)
        companies['IndexName'] = index_name
        return companies

    def index_djia(self):
        index_name = '^DJI'
        url = 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average'
        companies = self.get_wiki_component(url)
        companies['IndexName'] = index_name
        return companies

    def run(self):
        renames = {'Security': 'Company', 'Ticker': 'Symbol'}
        sp500 = self.index_sp_500().rename(columns=renames)
        nas_100 = self.index_naq_100().rename(columns=renames)
        dji = self.index_djia().rename(columns=renames)
        df = pd.concat([sp500, nas_100, dji])

        self.save_data_to_database('STK_MKT_WKP_IDX_COMPONENT', df)

    def save_data_to_database(self, table_name, df: pd.DataFrame):
        df['data_in_date'] = datetime.datetime.now()
        df.to_sql(table_name, engine, index=False, if_exists='replace')


class YahooTradeDataAgent(object):
    def get_trade_data(self, symbol, period, interval, **kwargs):
        tick = yf.Ticker(symbol)
        trade_data = tick.history(period=period, interval=interval, **kwargs)
        return trade_data

    def save_data_to_database(self, table_name, df: pd.DataFrame):
        df['data_in_date'] = datetime.datetime.now()
        df.to_sql(table_name, engine, if_exists='append')


class YahooIndexTradeData(YahooTradeDataAgent):
    def run(self):
        index_list = ['^GSPC', '^DJI', '^IXIC']
        rst = list()
        for idx in index_list:
            data = self.get_trade_data(idx, period='6mo', interval='1d')
            data['Symbol'] = idx
            rst.append(data)
            time.sleep(1)
        df = pd.concat(rst)
        self.save_data_to_database('STK_TRD_YHO_DAILY', df)
        print(df)


class YahooStockTradeData(YahooTradeDataAgent):
    def run(self):
        # STK_MKT_WKP_IDX_COMPONENT
        idx_component = pd.read_sql('select "Symbol" from "STK_MKT_WKP_IDX_COMPONENT" ', engine)
        rst = list()
        for i in idx_component['Symbol'].values.tolist():
            df = self.get_trade_data(i, period='6mo', interval='1d')
            df['Symbol'] = i
            rst.append(df)
            time.sleep(1)
        df = pd.concat(rst)
        self.save_data_to_database('STK_TRD_YHO_DAILY', df)
        print(df)


if __name__ == '__main__':
    ic = IndexComponent()
    ic.run()
    ic = YahooIndexTradeData()
    ic.run()
