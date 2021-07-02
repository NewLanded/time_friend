from util_base.db import (get_boolean_value, get_multi_data, get_single_value)


class BasicInfo:
    def __init__(self, db_conn):
        self.db_conn = db_conn

    def __del__(self):
        # self.db_conn.close()
        pass

    def get_active_ts_code(self):
        sql = """
        select ts_code, code, name, industry, market from s_info
        """
        result = get_multi_data(self.db_conn, sql)
        result = [i[0] for i in result if i[4] in ("主板", "中小板")]

        return result

    def get_active_trade_day(self, data_date):
        sql = """
        select max(date) from sec_date_info where date <= %(data_date)s and is_workday_flag=true;
        """
        args = {"data_date": data_date}
        date = get_single_value(self.db_conn, sql, args)
        return date

    def get_previous_trade_day(self, data_date):
        sql = """
        select max(date) from sec_date_info where date < %(data_date)s and is_workday_flag=true;
        """
        args = {"data_date": data_date}
        date = get_single_value(self.db_conn, sql, args)

        return date

    def get_next_trade_day(self, data_date):
        sql = """
        select min(date) from sec_date_info where date > %(data_date)s and is_workday_flag=true;
        """
        args = {"data_date": data_date}
        date = get_single_value(self.db_conn, sql, args)
        return date

    def is_trade_day(self, data_date):
        sql = """
        select 1 from sec_date_info where date=%(data_date)s and is_workday_flag=true
        """
        args = {"data_date": data_date}
        result = get_boolean_value(self.db_conn, sql, args)
        result = True if result else False

        return result
