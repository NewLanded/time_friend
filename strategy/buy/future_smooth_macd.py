import datetime

import talib

from util_base.constant import FreqCode
from util_base.db import get_db_conn
from util_future.util_data.basic_info import BasicInfo
from util_base.result import Result
from util_future.util_module.point_module import get_main_code_interval_point_data_by_freq_code, \
    get_ts_code_interval_point_data_by_freq_code


def buy(security_point_data, data_num, max_hist_value, std_value):
    if not security_point_data.empty:
        benm_rate = security_point_data['close'].iloc[0] / 1000  # 基准倍数, 使每个品种的价格基准趋于一致
        security_point_data['close'] = security_point_data['close'] / benm_rate

        # 12日EMA的计算：EMA12 = 前一日EMA12 X 11/13 + 今日收盘 X 2/13
        #
        # 26日EMA的计算：EMA26 = 前一日EMA26 X 25/27 + 今日收盘 X 2/27
        #
        # 差离值（DIF）的计算： DIF = EMA12 - EMA26，即为talib-MACD返回值macd
        #
        # 根据差离值计算其9日的EMA，即离差平均值，是所求的DEA值。今日DEA = （前一日DEA X 8/10 + 今日DIF X 2/10），即为talib-MACD返回值signal
        #
        # DIF与它自己的移动平均之间差距的大小一般BAR =（DIF-DEA)2，即为MACD柱状图。但是talib中MACD的计算是bar=(dif-dea)1
        # macd 是快慢均线的差值(DIFF)，signal是macd的均线(DEA), hist是柱的长度(BAR)
        # 和软件中的数值不一样, 目前不清楚为什么, 但是不太影响使用, 只是看下形态
        macd, signal, hist = talib.MACD(security_point_data['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        hist.dropna(inplace=True)
        if not hist.empty:
            hist = hist.iloc[-data_num:]
            if (abs(hist) < max_hist_value).all():
                std = hist.std(ddof=1)
                if std <= std_value:
                    # 快线慢线互穿
                    if (macd.iloc[-2] >= signal.iloc[-2] and macd.iloc[-1] <= signal.iloc[-1]) or (
                            macd.iloc[-2] <= signal.iloc[-2] and macd.iloc[-1] >= signal.iloc[-1]):
                        # print(std)
                        return True

    return False


def buy_month(security_point_data):
    if not security_point_data.empty:
        benm_rate = security_point_data['close'].iloc[0] / 1000  # 基准倍数, 使每个品种的价格基准趋于一致
        security_point_data['close'] = security_point_data['close'] / benm_rate

        macd, signal, hist = talib.MACD(security_point_data['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        hist.dropna(inplace=True)
        if not hist.empty:
            if (macd.iloc[-2] >= signal.iloc[-2] and macd.iloc[-1] <= signal.iloc[-1]) or (
                    macd.iloc[-2] <= signal.iloc[-2] and macd.iloc[-1] >= signal.iloc[-1]):
                # print(std)
                return True

    return False


# 将最后一日的数据复制一份, 用来模拟下一周期的数据, 然后计算
def buy_pre(security_point_data, data_num, max_hist_value, std_value):
    if not security_point_data.empty:
        benm_rate = security_point_data['close'].iloc[0] / 1000  # 基准倍数, 使每个品种的价格基准趋于一致
        security_point_data['close'] = security_point_data['close'] / benm_rate
        security_point_data = security_point_data.append(security_point_data.iloc[-1])

        # 12日EMA的计算：EMA12 = 前一日EMA12 X 11/13 + 今日收盘 X 2/13
        #
        # 26日EMA的计算：EMA26 = 前一日EMA26 X 25/27 + 今日收盘 X 2/27
        #
        # 差离值（DIF）的计算： DIF = EMA12 - EMA26，即为talib-MACD返回值macd
        #
        # 根据差离值计算其9日的EMA，即离差平均值，是所求的DEA值。今日DEA = （前一日DEA X 8/10 + 今日DIF X 2/10），即为talib-MACD返回值signal
        #
        # DIF与它自己的移动平均之间差距的大小一般BAR =（DIF-DEA)2，即为MACD柱状图。但是talib中MACD的计算是bar=(dif-dea)1
        # macd 是快慢均线的差值(DIFF)，signal是macd的均线(DEA), hist是柱的长度(BAR)
        # 和软件中的数值不一样, 目前不清楚为什么, 但是不太影响使用, 只是看下形态
        macd, signal, hist = talib.MACD(security_point_data['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        hist.dropna(inplace=True)
        if not hist.empty:
            hist = hist.iloc[-data_num:]
            if (abs(hist) < max_hist_value).all():
                std = hist.std(ddof=1)
                if std <= std_value:
                    # 快线慢线互穿
                    if (macd.iloc[-2] >= signal.iloc[-2] and macd.iloc[-1] <= signal.iloc[-1]) or (
                            macd.iloc[-2] <= signal.iloc[-2] and macd.iloc[-1] >= signal.iloc[-1]):
                        # print(std)
                        return True

    return False


# 将最后一日的数据复制一份, 用来模拟下一周期的数据, 然后计算
def buy_month_pre(security_point_data):
    if not security_point_data.empty:
        benm_rate = security_point_data['close'].iloc[0] / 1000  # 基准倍数, 使每个品种的价格基准趋于一致
        security_point_data['close'] = security_point_data['close'] / benm_rate
        security_point_data = security_point_data.append(security_point_data.iloc[-1])

        macd, signal, hist = talib.MACD(security_point_data['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        hist.dropna(inplace=True)
        if not hist.empty:
            if (macd.iloc[-2] >= signal.iloc[-2] and macd.iloc[-1] <= signal.iloc[-1]) or (
                    macd.iloc[-2] <= signal.iloc[-2] and macd.iloc[-1] >= signal.iloc[-1]):
                # print(std)
                return True

    return False


def start(date_now=None):
    try:
        db_conn = get_db_conn()
        date_now = datetime.date.today() if date_now is None else date_now
        if BasicInfo(db_conn).is_trade_day(date_now):
            start_date, end_date = date_now - datetime.timedelta(days=100), date_now
            ts_code_list = BasicInfo(db_conn).get_active_ts_code(date_now)
            for ts_code in ts_code_list:
                # buy_flag = False
                try:
                    security_point_data = get_ts_code_interval_point_data_by_freq_code(db_conn, ts_code, start_date, end_date,
                                                                                       FreqCode("D"))
                    # 使用 玻璃, datetime.date(2021, 4, 13)为结束日, 20天数据, 计算出来标准差为1.5, hist最大值是4
                    buy_flag = buy(security_point_data, 20, 5, 2.5)
                    if buy_flag is True:
                        Result(db_conn).insert_strategy_result_data(ts_code, ts_code, "future_smooth_macd", "D", "B", date_now)
                except Exception as e:
                    Result(db_conn).store_failed_message(ts_code, "future_smooth_macd", str(e), date_now)

                # if buy_flag is not True:
                #     try:
                #         security_point_data = get_ts_code_interval_point_data_by_freq_code(db_conn, ts_code, start_date, end_date,
                #                                                                            FreqCode("D"))
                #         # 使用 玻璃, datetime.date(2021, 4, 13)为结束日, 20天数据, 计算出来标准差为1.5, hist最大值是4
                #         buy_flag = buy_pre(security_point_data, 20, 5, 2.5)
                #         if buy_flag is True:
                #             Result(db_conn).insert_strategy_result_data(ts_code, ts_code, "future_smooth_macd_pre", "D", "B", date_now)
                #     except Exception as e:
                #         Result(db_conn).store_failed_message(ts_code, "future_smooth_macd_pre", str(e), date_now)

            start_date, end_date = date_now - datetime.timedelta(days=365), date_now
            if BasicInfo(db_conn).get_next_trade_day(date_now) != date_now + datetime.timedelta(days=1):
                for ts_code in ts_code_list:
                    buy_flag = False
                    try:
                        security_point_data = get_main_code_interval_point_data_by_freq_code(db_conn, ts_code, start_date, end_date,
                                                                                             FreqCode("W"))
                        # 使用 纸浆, datetime.date(2020, 11, 13)为结束日, 20天数据, 计算出来标准差为4.3, hist最大值是10.9
                        buy_flag = buy(security_point_data, 20, 15, 6)
                        if buy_flag is True:
                            Result(db_conn).insert_strategy_result_data(ts_code, "main", "future_smooth_macd", "W", "B", date_now)
                    except Exception as e:
                        Result(db_conn).store_failed_message(ts_code, "future_smooth_macd", str(e), date_now)

                    if buy_flag is not True:
                        try:
                            security_point_data = get_main_code_interval_point_data_by_freq_code(db_conn, ts_code, start_date, end_date,
                                                                                                 FreqCode("W"))
                            # 使用 纸浆, datetime.date(2020, 11, 13)为结束日, 20天数据, 计算出来标准差为4.3, hist最大值是10.9
                            buy_flag = buy_pre(security_point_data, 20, 15, 6)
                            if buy_flag is True:
                                Result(db_conn).insert_strategy_result_data(ts_code, "main", "future_smooth_macd_pre", "W", "B", date_now)
                        except Exception as e:
                            Result(db_conn).store_failed_message(ts_code, "future_smooth_macd_pre", str(e), date_now)

            start_date, end_date = date_now - datetime.timedelta(days=1500), date_now
            if BasicInfo(db_conn).get_next_trade_day(date_now).month != date_now.month:
                for ts_code in ts_code_list:
                    buy_flag = False
                    try:
                        security_point_data = get_main_code_interval_point_data_by_freq_code(db_conn, ts_code, start_date, end_date,
                                                                                             FreqCode("M"))
                        buy_flag = buy_month(security_point_data)
                        if buy_flag is True:
                            Result(db_conn).insert_strategy_result_data(ts_code, "main", "future_smooth_macd", "M", "B", date_now)
                    except Exception as e:
                        Result(db_conn).store_failed_message(ts_code, "future_smooth_macd", str(e), date_now)

                    if buy_flag is not True:
                        try:
                            security_point_data = get_main_code_interval_point_data_by_freq_code(db_conn, ts_code, start_date, end_date,
                                                                                                 FreqCode("M"))
                            buy_flag = buy_month_pre(security_point_data)
                            if buy_flag is True:
                                Result(db_conn).insert_strategy_result_data(ts_code, "main", "future_smooth_macd_pre", "M", "B", date_now)
                        except Exception as e:
                            Result(db_conn).store_failed_message(ts_code, "future_smooth_macd_pre", str(e), date_now)
    except Exception as e:
        pass
    finally:
        db_conn.close()


if __name__ == "__main__":
    start(datetime.date(2021, 7, 2))
