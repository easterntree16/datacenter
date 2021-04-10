#================= HKSE_stocks_updater.py ==================
# Purpose
#----------------------------------------------------------
# To get the Hong Kong Stock exchange listed stock and ETF
# and insert into MySQL database tables
#==========================================================

import os
import datetime as dt
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

class hkse_stocks_updater(object):
    def __init__(self):
        self.data_location = './data/'
        # HKSE secruities file from
        # https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx
        self.equity_list = 'ListOfSecurities'
        # database user/pwd and DB name
        self.db_usr = 'root'
        self.db_pwd = 'besql3086!'
        self.db_name = 'securities_master'
        self.engine = create_engine('mysql+pymysql://{}:{}@localhost/{}'.format(self.db_usr,self.db_pwd,self.db_name))


    # read the securities from the HKSE securities file
    # clean the data to needed format
    def read_stocks_df_from_csv(self):
        path = self.data_location + '{}.csv'.format(self.equity_list)
        if not os.path.exists(path):
            return None
        df = pd.read_csv(path,header=2 ,dtype = str)
        if len(df) == 0 :
            return None

        # only pick the listed stock and ETF
        df = df.loc[df['Category'].isin(["Equity","Exchange Traded Products"])]
		
        # convert to boolean value
        bool_cols = ["Subject to Stamp Duty","Shortsell Eligible","CAS Eligible","VCM Eligible","Admitted to Stock Options","Admitted to Stock Futures", "Admitted to CCASS","POS Eligble"]
        df[bool_cols] = np.where(df[bool_cols] == 'Y',True,False)

        # drop the unnecessary columns
        drop_cols = ['Par Value', 'ISIN', 'Expiry Date',
        'Debt Securities Board Lot (Nominal)', 'Debt Securities Investor Type',
        'Spread Table\n1, 4 = Part A\n3 = Part B\n5 = Part D', 'Unnamed: 20']
        df.drop(drop_cols,axis=1,inplace=True)
        df.reset_index(drop=True,inplace=True)
        # rename columns name
        col_name = { 'Stock Code':'symbol','Name of Securities':'sec_name','Category':'category','Sub-Category':'sub_category',\
                     'Board Lot':'board_lot','Subject to Stamp Duty':'subject_stamp','Shortsell Eligible':'shortsell_eligible',\
                     'CAS Eligible':'cas_eligible','VCM Eligible':'vcm_eligible','Admitted to Stock Options':'admitted_stock_options',\
                     'Admitted to Stock Futures':'admitted_Stock_futures','Admitted to CCASS':'admitted_CCASS','ETF / Fund Manager':'etf_manager',\
                     'POS Eligble':'POS_eligble' }
        df.rename(columns = col_name,inplace=True)
        return df


    """
    Function: import_file
    Purpose: Reads a CSV file and stores the data in a database.
    """
    def import_stocks_intoDB(self,df):
        # Store the time to later insert into our created_at column
        current_time = dt.datetime.now()
        exchange_id = 3
        currency = 'HKD'

        # First part of the instert statement
        insert_init = """insert into `stock_hkse` \
           (`exchange_id`,`symbol`,`name`,`category`,`sub_category`,`currency`,\
            `board_lot`,`subject_stamp`,`shortsell_eligible`,`cas_eligible`,`vcm_eligible`,\
            `admitted_stock_options`,`admitted_Stock_futures`,`admitted_CCASS`,`POS_eligble`,\
            `etf_manager`,`created_date`,`last_updated_date`) values """

        # Add values for all days to the insert statement
        vals = ",".join(["""({},'{}','{}', '{}', '{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, '{}', '{}', '{}')""".format(
            exchange_id, row.symbol, row.sec_name.replace("'","_"), row.category,row.sub_category,currency,\
            int(row.board_lot.replace(',','')), row.subject_stamp, row.shortsell_eligible, row.cas_eligible,\
            row.vcm_eligible, row.admitted_stock_options, row.admitted_Stock_futures, row.admitted_CCASS,\
            row.POS_eligble, row.etf_manager, current_time, current_time
        ) for index, row in df.iterrows()])


        # Put the parts together
        query = insert_init + vals
        query = query.replace('nan', 'null').replace('None', 'null').replace('none', 'null')
        # Fire insert statement
        self.engine.execute(query)

    def query_stock_list(self):
        # query stock symbol list
        query = """select symbol from stock_hkse"""
        # execute query
        df = pd.read_sql_query(query, self.engine)
        # convert dataframe symbol column to list
        symbol_list = np.array(df['symbol']).tolist()
        return symbol_list

    def insert_stocks_intodb(self):
        # read stock list dataframe from csv
        df = self.read_stocks_df_from_csv()
        # insert stock list dataframe into database
        self.import_stocks_intoDB(df)

        # dbdf = query_stock_list()
        # new = anti_join(data, dbdf['symbol'], ['symbol'])
        # if len(new) == 0 :
        #     print("No New secruity listed!")
        #     return None
        # else:
        #     print(new.tail())
        #     write_stocks_into_db(new)
        # return new

# def anti_join(x, y, on):
#     """Return rows in x which are not present in y"""
#     ans = pd.merge(left=x, right=y, how='left', indicator=True, on=on)
#     ans = ans.loc[ans._merge == 'left_only', :].drop(columns='_merge')
#     return ans
#



if __name__ == "__main__":
    start = dt.datetime.now()
    updater = hkse_stocks_updater()
    # read stock list dataframe from csv and insert into database
    df = updater.insert_stocks_intodb()
    end = dt.datetime.now()

    print("Process Time Costs:%.6f seconds" % ((end - start).seconds))

