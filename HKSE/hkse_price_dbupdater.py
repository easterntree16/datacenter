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
        path = self.data_location + file_name
        symbol = file_name[:-7]
        source_symbol = file_name[:-4]
        df = pd.read_csv(path, index_col = [0], parse_dates=[0], header = 0)
        start_date, end_date = df.index[0], df.index[-1]
        count = df.shape[0]
        print(f"{symbol}, {self.exchange},{source_symbol}, {self.data_source},{start_date}, {end_date}, {count}")
        self.update_timeseries(symbol, self.exchange,source_symbol, self.data_source,start_date, end_date, count)


    def update_timeseries(self,symbol, exchange,vendor_symbol, data_source,start_date, end_date, count):
        # Store the time to later insert into our created_at column
        current_time = dt.datetime.now()
        interveral = 'd'
        is_active = True
        is_block = False
        # search timeseries(ts)
        # if exit the ts, update the ts with end_date, count
        # if not exit the ts, inser the ts
        # return the update ts datafram
        # First part of the instert statement
        insert_init = """INSERT INTO `timeseries` (\
         `symbol`,`exchange`, `vendor_symbol`, `data_source`, `bar_interval`, `series_begin`, `series_end`, \
         `series_count`,`is_active`, `is_blocked`, \
         `create_date`,`last_update_date`) VALUES """

        # Add values for all days to the insert statement
        vals = ",".join(["""('{}','{}','{}', '{}', '{}', '{}', '{}', {}, {}, {},'{}', '{}')""".format(\
            symbol, exchange, vendor_symbol, data_source, interveral, start_date, end_date, count, is_active, is_block,\
            current_time, current_time)])

        # Put the parts together
        query = insert_init + vals
        query = query.replace('nan', 'null').replace('None', 'null').replace('none', 'null')
        # Fire insert statement
        self.engine.execute(query)
        df = self.query_timeseries(symbol, exchange, data_source, interveral)
        print(df)

    def query_timeseries(self, symbol, exchange,data_source, interval):
        # Store the time to later insert into our created_at column
        current_time = dt.datetime.now()

        # First part of the query statement
        query = f"SELECT `symbol`,`exchange`,`vendor_symbol`, `data_source`, `bar_interval`, `series_begin`, `series_end`, \
         `series_count`,`is_active`, `is_blocked`, `create_date`,`last_update_date` FROM `timeseries` \
         WHERE symbol='{symbol}' AND exchange='{exchange}' AND data_source='{data_source}' AND bar_interval='{interval}'"

        df = pd.read_sql_query(query, self.engine)
        return df


    # def update_bar(self,symbol, exchange,vendor_symbol, data_source,start_date, end_date, count):
    #
    #     # Put the parts together
    #     query = insert_init + vals
    #     query = query.replace('nan', 'null').replace('None', 'null').replace('none', 'null')
    #     # Fire insert statement
    #     self.engine.execute(query)

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
