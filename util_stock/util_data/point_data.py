import numpy as np
import pandas as pd

from util_base.db import get_multi_data, get_single_row


class PointData:
    def __init__(self, db_conn):
        self.db_conn = db_conn

    def __del__(self):
        # self.db_conn.close()
        pass

    def get_stock_interval_point_data(self, ts_code, start_date, end_date):
        sql = """
        select ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount from security_point_data 
        where ts_code = %(ts_code)s and trade_date between %(start_date)s and %(end_date)s
        """
        args = {"ts_code": ts_code, "start_date": start_date, "end_date": end_date}
        result_ori = get_multi_data(self.db_conn, sql, args)

        if result_ori:
            df = pd.DataFrame(np.array(result_ori),
                              columns=['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount'])
        else:
            df = pd.DataFrame(columns=['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount'])
        df.sort_values(by=['trade_date'])
        return df

    def get_stock_interval_basic_data(self, ts_code, data_date):
        sql = """
        select ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, total_share, float_share, free_share, total_mv, circ_mv from security_daily_basic
        where ts_code = %(ts_code)s and trade_date = %(data_date)s
        """
        args = {"ts_code": ts_code, "data_date": data_date}
        result_ori = get_single_row(self.db_conn, sql, args)
        if result_ori:
            df = pd.Series(np.array(result_ori),
                           index=['ts_code', 'trade_date', 'close', 'turnover_rate', 'turnover_rate_f', 'volume_ratio', 'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'total_share',
                                  'float_share', 'free_share', 'total_mv', 'circ_mv'])
        else:
            df = pd.Series(
                index=['ts_code', 'trade_date', 'close', 'turnover_rate', 'turnover_rate_f', 'volume_ratio', 'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'total_share', 'float_share',
                       'free_share', 'total_mv', 'circ_mv'])
        return df
