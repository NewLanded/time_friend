import datetime

from util_base.db import update_data, get_multi_data


class Result:
    def __init__(self, db_conn):
        self.db_conn = db_conn

    def __del__(self):
        # self.db_conn.close()
        pass

    def insert_strategy_result_data(self, ts_code, main_ts_code, strategy_code, freq_code, bs_flag, date):
        sql = """
        insert into strategy_result (ts_code, main_ts_code, strategy_code, freq_code, bs_flag, date, update_date) 
        values(%(ts_code)s, %(main_ts_code)s, %(strategy_code)s, %(freq_code)s, %(bs_flag)s, %(date)s, %(update_date)s)
        """
        args = {"ts_code": ts_code, "main_ts_code": main_ts_code, "strategy_code": strategy_code, "freq_code": freq_code, "bs_flag": bs_flag, "date": date,
                "update_date": datetime.datetime.now()}
        result = update_data(self.db_conn, sql, args)

        return result

    def get_strategy_result_data(self, strategy_code, date):
        sql = """
        select ts_code, main_ts_code, strategy_code, freq_code, bs_flag from strategy_result
        where strategy_code=%(strategy_code)s and date=%(date)s
        """
        args = {"strategy_code": strategy_code, "date": date}
        result = get_multi_data(self.db_conn, sql, args)

        return result

    def store_failed_message(self, code, index, error_message, date):
        sql = """
        insert into failed_code(code, index, error_message, date, update_date)
        values(%(code)s, %(index)s, %(error_message)s, %(date)s, %(update_date)s)
        """
        args = {
            "code": code,
            "index": index,
            "error_message": error_message,
            "date": date,
            "update_date": datetime.datetime.now()
        }
        update_data(self.db_conn, sql, args)
