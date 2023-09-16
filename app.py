import datetime
import pandas as pd
import streamlit as st

from common.databaseEngine import engine, text

st.set_page_config(page_title='Personal Investing Dashboard',
                   layout='wide')

st.title('Personal Investing Dashboard')

hot_index_name = ('^GSPC', '^DJI', '^RUT', '^IXIC')

index_map = {
    '^FTSE': 'FTSE 100',
    '^GSPC': 'S&P 500',
    '^DJI': 'DJIA',
    '^IXIC': 'NASDAQ',
    '^IXIC_100': 'NASDAQ 100',
    '^GDAXI': 'DAX PERFORMANCE-INDEX ',
    '^FCHI': 'CAC 40',
    '^N225': 'Nikkei 225',
    '^HSI': 'HANG SENG INDEX',
    '000001.SS': 'SSE Composite Index',
    '399001.SZ': 'Shenzhen Index ',
    '^AXJO': 'S&P/ASX 200',
    '^GSPTSE': 'S&P/TSX Composite index',
    '^JN0U.JO': 'Top 40 USD Net TRI Index ',
    '^RUT': 'Russ 2000 ',
    '^VIX': 'CBOE Volatility Index ',
    '^STOXX50E': 'ESTX 50 PR.EUR',
    '^N100': 'Euronext 100 Index ',
    '^BFX': 'BEL 20',
    'IMOEX.ME': 'MOEX Russia Index ',
    '^NYA': 'NYSE COMPOSITE (DJ) ',
    '^XAX': 'NYSE AMEX COMPOSITE INDEX ',
    '^STI': 'STI Index ',
    '^AORD': 'ALL ORDINARIES',
    '^BSESN': 'S&P BSE SENSEX',
    '^JKSE': 'IDX COMPOSITE',
    '^KLSE': 'FTSE Bursa Malaysia KLCI',
    '^NZ50': 'S&P/NZX 50 INDEX GROSS',
    '^KS11': 'KOSPI Composite Index',
    '^TWII': 'TSEC weighted index',
    '^BVSP': 'IBOVESPA',
    '^MXX': 'IPC MEXICO',
    '^IPSA': 'S&P/CLX IPSA',
    '^MERV': 'MERVAL',
    '^TA125.TA': 'TA-125',
    '^CASE30': 'EGX 30 Price Return Index',
    '^NSEI': 'NIFTY 50',
}

@st.cache_data
def load_data(ndays: int):
    where_symbol = "','".join(hot_index_name)
    start_dt = datetime.datetime.now() - datetime.timedelta(days=ndays)
    data = pd.read_sql(text(
        f'''select "Date","Close","Symbol" from "STK_TRD_YHO_DAILY" t where "Symbol" 
        in( '{where_symbol}') and "Date" >= '{start_dt}' order by "Date"  '''),
        engine.connect())
    data['first_close'] = data.groupby('Symbol')['Close'].transform('first')
    data['Percent'] = (data['Close'] - data['first_close']) / data['first_close'] * 100
    data['Name'] = data['Symbol'].map(index_map)
    return data

@st.cache_data
def load_ixic_100_data(ndays: int, index_name: str = '^IXIC_100'):
    start_dt = datetime.datetime.now() - datetime.timedelta(days=ndays)
    sql = f'''select t."Symbol",t."Company",t."GICS Sector",t1."Close" ,t1."Date"
    from "STK_MKT_WKP_IDX_COMPONENT" t 
    join "STK_TRD_YHO_DAILY" t1 on t."Symbol" = t1."Symbol" 
    where t."IndexName" = '{index_name}' and t1."Date" >= '{start_dt}' order by "Date"  '''
    data = pd.read_sql(text(sql), engine.connect())

    data['first_close'] = data.groupby('Symbol')['Close'].transform('first')
    data['Percent'] = (data['Close'] - data['first_close']) / data['first_close'] * 100
    return data


def load_ixic_100_top10_bottom10_data(data):
    max_date = data['Date'].max()
    max_data = data[data['Date'] == max_date]
    top10_symbol = max_data.sort_values(by='Percent', ascending=False).head(10)['Symbol'].values.tolist()
    bottom10_symbol = max_data.sort_values(by='Percent', ascending=True).head(10)['Symbol'].values.tolist()

    top10 = data[data['Symbol'].isin(top10_symbol)]
    bottom10 = data[data['Symbol'].isin(bottom10_symbol)]
    return top10, bottom10


def index_ma_count(data):
    data['ma50'] = data.groupby('Symbol')['Close'].transform(lambda x: x.rolling(50).mean())
    data['ma100'] = data.groupby('Symbol')['Close'].transform(lambda x: x.rolling(100).mean())
    data['ma200'] = data.groupby('Symbol')['Close'].transform(lambda x: x.rolling(200).mean())

    # group by Data count how many symbo greate then ma50, ma100, ma200
    ma50_count = data[data['Close'] > data['ma50']].groupby('Date')['Symbol'].count().reset_index()
    ma100_count = data[data['Close'] > data['ma100']].groupby('Date')['Symbol'].count().reset_index()
    ma200_count = data[data['Close'] > data['ma200']].groupby('Date')['Symbol'].count().reset_index()
    ma50_count.rename(columns={'Symbol': 'ma50'}, inplace=True)
    ma100_count.rename(columns={'Symbol': 'ma100'}, inplace=True)
    ma200_count.rename(columns={'Symbol': 'ma200'}, inplace=True)

    ma_count = pd.merge(ma50_count, ma100_count, on='Date')
    ma_count = pd.merge(ma_count, ma200_count, on='Date')
    return ma_count


with st.sidebar:
    option = st.sidebar.selectbox(
        "选择时间周期",
        ("1W", "1M", "3M", "1Y")
    )
    if option == '1Y':
        data = load_data(365)
        naq_100_data = load_ixic_100_data(365)
    elif option == '1W':
        data = load_data(7)
        naq_100_data = load_ixic_100_data(7)
    elif option == '3M':
        data = load_data(90)
        naq_100_data = load_ixic_100_data(90)
    else:
        data = load_data(30)
        naq_100_data = load_ixic_100_data(30)

st.subheader('指数强弱对比')

st.line_chart(
    data,
    x='Date',
    y='Percent',
    color='Name'
)

st.subheader('指数中大于均线的股票数量')
ma_index_name = ('^GSPC', '^DJI', '^IXIC_100')
cols = st.columns(len(ma_index_name))
for i in range(len(ma_index_name)):
    with cols[i]:
        index = ma_index_name[i]
        st.text(f'指数{index_map.get(index)}中大于50,100,200均线的股票数量')
        ma_great_cnt = index_ma_count(load_ixic_100_data(365, index_name=index))
        st.line_chart(
            ma_great_cnt,
            x='Date',
            y=['ma50', 'ma100', 'ma200'],
        )

st.subheader('纳指100成分股强弱对比')

top10, bottom10 = load_ixic_100_top10_bottom10_data(naq_100_data)
col1, col2 = st.columns(2)
with col1:
    st.text('纳指100成分股最强10名对比')
    top10.reset_index(drop=True, inplace=True)
    st.line_chart(
        top10,
        x='Date',
        y='Percent',
        color='Symbol'
    )

with col2:
    st.text('纳指100成分股最弱10名对比')
    bottom10.reset_index(drop=True, inplace=True)

    st.line_chart(
        bottom10,
        x='Date',
        y='Percent',
        color='Symbol'
    )
