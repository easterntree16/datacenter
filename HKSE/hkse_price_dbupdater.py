#================= HKSE_price_dbupdater.py ==================
# Purpose
#----------------------------------------------------------
# read the downloaded price data files
# and update the database timeseries and bar table
# using multithread
#==========================================================

import random
import time
import os
import datetime as dt
from queue import Queue, Empty
from threading import Thread, Lock
from common.constant import Exchange, Period, Interval
from hkse_stocks_updater import hkse_stocks_updater
import yfinance as yf
from sqlalchemy import create_engine
import pandas as pd

THREAD_POOL_SIZE = 4

class hkse_price_dbupdater(object):
    def __init__(self,thread_pool_size = THREAD_POOL_SIZE ):
        self.data_location = './data/daily/'
        self.thread_pool_size = thread_pool_size
        self.exchange = "HKSE"
        self.exchange_postfix = "HK"
        self.data_source = "Yahoo"
        # database user/pwd and DB name
        self.db_usr = 'root'
        self.db_pwd = 'besql3086!'
        self.db_name = 'hkse'
        self.engine = create_engine('mysql+pymysql://{}:{}@localhost/{}'.format(self.db_usr,self.db_pwd,self.db_name))

    # read price csv file and save into database
    def save_prices_DB(self, file_name):
        interval = 'd'
        path = self.data_location + file_name
        symbol = file_name[:-7]
        source_symbol = file_name[:-4]
        col = ['open','high','low','close','adjclose','volume']
        df = pd.read_csv(path, index_col = [0], parse_dates=[0], header = 0,names = col)
        start_date, end_date = df.index[0], df.index[-1]
        count = df.shape[0]
        print(f"{symbol}, {self.exchange},{source_symbol}, {self.data_source},{start_date}, {end_date}, {count}")

        # search timeseries(ts)
        df_ts = self.query_timeseries(symbol, self.exchange, self.data_source, interval)

        # if not exit the ts, inser the ts
        if df_ts.empty :
            # insert timeseries
            ts = self.insert_timeseries(symbol, self.exchange,source_symbol, self.data_source,interval, start_date, end_date, count)
            # insert bars
            timeseries_id = int(ts.loc[0,'id_timeseries'])
            print(f"ts_id:{timeseries_id}")
            self.insert_bars(df, timeseries_id, symbol, self.exchange, interval)

        # if exit the ts, update the ts with end_date, count
        else:
            last_end_date = df_ts['end_date'][0]
            # get the df after ts.end_date
            df_new = self.get_new_df(df, last_end_date)

            #inser new bars
            timeseries_id = df_ts['id_timeseries'][0]
            self.insert_bars(df_new, timeseries_id, symbol, self.exchange, interval)

            # update timeseries
            updated_count = df_ts['count'][0] + len(df_new)
            self.update_timeseries(symbol, self.exchange,self.data_source,interval, end_date, updated_count)


    def insert_timeseries(self,symbol, exchange,vendor_symbol, data_source,interval, start_date, end_date, count):
        # Store the time to later insert into our created_at column
        current_time = dt.datetime.now()
        is_active = True
        is_block = False

        insert_init = """INSERT INTO `timeseries` (\
         `symbol`,`exchange`, `vendor_symbol`, `data_source`, `bar_interval`, `series_begin`, `series_end`, \
         `series_count`,`is_active`, `is_blocked`, \
         `create_date`,`last_update_date`) VALUES """

        # Add values for all days to the insert statement
        vals = ",".join(["""('{}','{}','{}', '{}', '{}', '{}', '{}', {}, {}, {},'{}', '{}')""".format(\
            symbol, exchange, vendor_symbol, data_source, interval, start_date, end_date, count, is_active, is_block,\
            current_time, current_time)])

        # Put the parts together
        query = insert_init + vals
        query = query.replace('nan', 'null').replace('None', 'null').replace('none', 'null')
        # Fire insert statement
        self.engine.execute(query)
        df = self.query_timeseries(symbol, exchange, data_source, interval)
        print(df)
        return df

    def query_timeseries(self, symbol, exchange,data_source, interval):
        # First part of the query statement
        query = f"SELECT `id_timeseries`,`symbol`,`exchange`,`vendor_symbol`, `data_source`, `bar_interval`, `series_begin`, `series_end`, \
         `series_count`,`is_active`, `is_blocked`, `create_date`,`last_update_date` FROM `timeseries` \
         WHERE symbol='{symbol}' AND exchange='{exchange}' AND data_source='{data_source}' AND bar_interval='{interval}'"

        df = pd.read_sql_query(query, self.engine)
        # df.columns = ['id_timeseries','symbol','exchange','vendor_symbol', 'data_source', 'bar_interval', 'series_begin', 'series_end', \
        #  'series_count','is_active', 'is_blocked', 'create_date','last_update_date']

        return df

    def insert_bars(self,df, ts_id, symbol, exchange, interval):
        # Store the time to later insert into our created_at column
        current_time = dt.datetime.now()

        insert_init = """INSERT INTO `bar`(`id_timeseries`,`datetime`,`symbol`,`exchange`,`bar_interval`,\
        `open`,`high`,`low`,`close`,`volume`,`adjclose`,`create_date`,`last_update_date`) VALUES """

        # Add values for all days to the insert statement
        vals = ",".join(["""({},'{}','{}', '{}', '{}', {}, {}, {}, {}, {},{},'{}', '{}')""".format(\
            ts_id, str(index), symbol, exchange, interval,row.open, row.high, row.low, row.close, row.volume,\
            row.adjclose, current_time, current_time) for index, row in df.iterrows()])

        # Put the parts together
        insert_stmt = insert_init + vals
        query = insert_stmt.replace('nan', 'null').replace('None', 'null').replace('none', 'null')
        # Fire insert statement
        self.engine.execute(query)

    def process_updateDB(self):
        files = [s for s in os.listdir(self.data_location)]
        for f in files[:3]:
            self.save_prices_DB(f)

if __name__ == "__main__":
    started =  time.time()
    builder = hkse_price_dbupdater()
    builder.process_updateDB()
    elapsed =  time.time() - started
    print("Process Time Costs:%.6f seconds" % (elapsed))
