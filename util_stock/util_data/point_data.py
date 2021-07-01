from util_base.db import get_db_conn, get_multi_data
import pandas as pd
import numpy as np


class PointData:
    def __init__(self):
        self.db_conn = get_db_conn()

    def __del__(self):
        self.db_conn.close()

    def get_future_interval_point_data(self, ts_code, start_date, end_date):
        sql = """
        select ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount from security_point_data 
        where ts_code = %(ts_code)s and trade_date between %(start_date)s and %(end_date)s
        """
        args = {"ts_code": ts_code, "start_date": start_date, "end_date": end_date}
        result_ori = get_multi_data(self.db_conn, sql, args)
        df = pd.DataFrame(np.array(result_ori),
                          columns=['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount'])
        df.sort_values(by=['trade_date'])
        return df
