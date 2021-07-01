
from util_base.db import get_db_conn, get_multi_data
import pandas as pd
import numpy as np


class PointData:
    def __init__(self):
        self.db_conn = get_db_conn()

    def __del__(self):
        self.db_conn.close()

    def get_ts_code_interval_holding_data(self, ts_code, start_date, end_date):
        sql = '''
        select trade_date, broker, vol, vol_chg, long_hld, long_chg, short_hld, short_chg from future_holding_data 
        where symbol = %(symbol)s and trade_date >= %(start_date)s and trade_date <= %(end_date)s
        order by trade_date
        '''
        args = {"symbol": ts_code.split(".")[0], "start_date": start_date, "end_date": end_date}
        result_ori = get_multi_data(self.db_conn, sql, args)

        result = {}
        for trade_date, broker, vol, vol_chg, long_hld, long_chg, short_hld, short_chg in result_ori:
            result.setdefault(trade_date, []).append({
                "broker": broker,
                "vol": vol,
                "vol_chg": vol_chg,
                "long_hld": long_hld,
                "long_chg": long_chg,
                "short_hld": short_hld,
                "short_chg": short_chg
            })

        return result

    def get_future_interval_point_data(self, ts_code, start_date, end_date):
        sql = '''
        select ts_code, trade_date, open, high, low, close, settle, change1, change2, vol, amount from future_daily_point_data where 
        ts_code = %(ts_code)s and trade_date >= %(start_date)s and trade_date <= %(end_date)s
        order by trade_date
        '''
        args = {"ts_code": ts_code, "start_date": start_date, "end_date": end_date}
        result_ori = get_multi_data(self.db_conn, sql, args)

        df = pd.DataFrame(np.array(result_ori),
                          columns=['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'settle', 'change1', 'change2', 'vol', 'amount'])
        df.sort_values(by=['trade_date'])
        return df

    def get_future_interval_point_data_by_main_code(self, ts_code, start_date, end_date):
        sql = """select a.ts_code, a.trade_date, a.open, a.high, a.low, a.close, a.settle, a.change1, a.change2, a.vol, a.amount from 
        future_daily_point_data a inner join
            (select trade_date, mapping_ts_code from future_main_code_data where ts_code=(
            select ts_code from future_main_code_data where mapping_ts_code= %(ts_code)s or ts_code=%(ts_code)s order by length(ts_code) limit 1)) b 
        on a.ts_code=b.mapping_ts_code and a.trade_date=b.trade_date
        where a.trade_date >= %(start_date)s and a.trade_date <= %(end_date)s
        order by a.trade_date"""

        args = {"ts_code": ts_code, "start_date": start_date, "end_date": end_date}
        result_ori = get_multi_data(self.db_conn, sql, args)

        df = pd.DataFrame(np.array(result_ori),
                          columns=['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'settle', 'change1', 'change2', 'vol', 'amount'])
        df.sort_values(by=['trade_date'])
        return df
