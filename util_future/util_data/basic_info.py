import re
from itertools import count

from fastapi import HTTPException, status

from config import TRADE_CODE_LIST
from util_base.db import (get_boolean_value, get_db_conn, get_multi_data,
                          get_single_value)


class BasicInfo:
    def __init__(self):
        self.db_conn = get_db_conn()

    def __del__(self):
        self.db_conn.close()

    def get_ts_code_by_main_ts_code_with_date(self, main_ts_code, start_date, end_date):
        sql = """
        select trade_date, mapping_ts_code from future_main_code_data
        where (ts_code in (select ts_code from future_main_code_data where mapping_ts_code=%(main_ts_code)s) or ts_code = %(main_ts_code)s) and trade_date between %(start_date)s and %(end_date)s order by trade_date
        """
        args = {"main_ts_code": main_ts_code, "start_date": start_date, "end_date": end_date}
        result = get_multi_data(self.db_conn, sql, args)

        return result

    def get_ts_code_by_main_ts_code(self, main_ts_code, data_date):
        sql = """
        select mapping_ts_code from future_main_code_data
        where ts_code = %(main_ts_code)s and trade_date = %(data_date)s
        """
        args = {"main_ts_code": main_ts_code, "data_date": data_date}
        result = get_single_value(self.db_conn, sql, args)
        return result

    def get_main_ts_code_by_ts_code(self, ts_code):
        sql = """
        select ts_code from future_main_code_data
        where mapping_ts_code = %(ts_code)s
        """
        args = {"ts_code": ts_code}
        result = get_single_value(self.db_conn, sql, args)
        return result

    def get_active_ts_code_info(self, data_date):
        ts_code_list = self.get_active_ts_code(data_date)
        if not ts_code_list:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="未获取到有效的ts_code")

        sql = """
        select ts_code, exchange, name from future_basic_info_data
        where ts_code=ANY(%(ts_code_list)s::text[])
        """
        args = {"ts_code_list": ts_code_list}
        result = get_multi_data(self.db_conn, sql, args)
        return result

    def get_active_ts_code(self, data_date):
        data_date = self.get_active_trade_day(data_date)
        sql = """
        select ts_code, mapping_ts_code from future_main_code_data
        where trade_date = %(data_date)s
        """
        args = {"data_date": data_date}
        result = get_multi_data(self.db_conn, sql, args)

        result_new = []
        for row in result:
            symbol_main, symbol_ts = row[0].split('.')[0], row[1].split('.')[0]

            if re.match(r'[A-Z]*', symbol_ts).group(0) == re.match(r'[A-Z]*', symbol_main).group(0):
                result_new.append(row[1])

        result = list(set(result_new))
        result = [i for i in result if re.match(r"(\D+)([\d|\.]+)(\D*)", i).group(1) in TRADE_CODE_LIST]
        result.sort()

        return result

    def get_future_info_by_symbol(self, symbol_code_list):
        index_dict = dict(zip([i for i in symbol_code_list], count()))

        sql = """
        select ts_code, name from  future_basic_info_data 
        where symbol=ANY(%(symbol_code_list)s::text[])
        """
        args = {"symbol_code_list": symbol_code_list}
        result = get_multi_data(self.db_conn, sql, args)

        result = sorted(result, key=lambda x: index_dict[x[0].split('.')[0]])

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
