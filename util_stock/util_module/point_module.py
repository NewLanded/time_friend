from util_base.date_util import (adjust_interval_all_date_list_by_exists_date,
                                 get_interval_date_list_by_freq_code)
from util_stock.util_data.point_data import PointData


def get_ts_code_interval_point_data_by_freq_code(db_conn, ts_code, start_date, end_date, freq_code):
    interval_date_list = get_interval_date_list_by_freq_code(start_date, end_date, freq_code)
    interval_point_data = PointData(db_conn).get_future_interval_point_data(ts_code, interval_date_list[0][0], interval_date_list[-1][-1])
    interval_date_list = adjust_interval_all_date_list_by_exists_date(interval_date_list, list(interval_point_data['trade_date'].values))

    groupby_index = []
    for index, date_list in enumerate(interval_date_list):
        for _ in date_list:
            groupby_index.append(index)
    interval_point_data['groupby_index'] = groupby_index
    interval_point_data = interval_point_data.groupby(by=["groupby_index"]).agg({
        "trade_date": lambda x: x.iloc[-1],
        "ts_code": lambda x: x.iloc[0],
        "open": lambda x: x.iloc[0],
        "high": 'max',
        "low": 'min',
        "close": lambda x: x.iloc[-1],
        "vol": lambda x: x.iloc[-1]
    })
    interval_point_data.dropna(inplace=True)
    return interval_point_data
