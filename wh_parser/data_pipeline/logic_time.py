# 处理主力合约
import os
from wh_parser.data_pipeline import SingleDataPipelineBase
import pickle
import datetime
import numpy as np
import pandas as pd
from wh_parser.util import split_order_book


class LogicTime(SingleDataPipelineBase):
    """
    处理任务 从 splte -> fin
    结构
    """

    name = '逻辑交易时间'

    DAY_TRADE_TIME_INFO = (6, 11)
    NIGHT_TRADE_TIME_INFO = (18, 11)

    def __init__(self, instruments_pk_data_path, daily_info_path,
                 **kwargs):
        super().__init__(**kwargs)
        self.daily_info_path = daily_info_path
        with open(instruments_pk_data_path, 'rb') as f:
            ins = pickle.load(f)
            self.instruments_store = {i['order_book_id']: i for i in ins}

    def do_task(self, task):
        file = task
        order_book_id = os.path.splitext(os.path.split(file)[1])[0]
        df = pd.read_csv(file)
        df.index = pd.to_datetime(df.timestamp.copy())
        print("处理合约: ", order_book_id)
        daily_info = self.process_logic_time(df)

        with open(os.path.join(self.daily_info_path, order_book_id + '.q'), 'wb') as f:
            pickle.dump(daily_info, f)

        if not os.path.exists(os.path.join(self.out_data_path, split_order_book(order_book_id)[0])):
            os.mkdir(os.path.join(self.out_data_path, split_order_book(order_book_id)[0]))
        df.to_csv(os.path.join(self.out_data_path, split_order_book(order_book_id)[0], order_book_id + '.csv'),
                  index=False)

    def process_logic_time(self, df: pd.DataFrame):
        """
        处理一个合约的 逻辑交易日
        :param df:
        :return:
        """
        df['trade_date'] = np.nan
        df['trade_time'] = np.nan
        if not len(df):
            return
        start_date = df.index[0].to_pydatetime()
        end_date = df.index[-1].to_pydatetime()
        day_num = (end_date - start_date).days

        daily_info = {}

        for i in range(-1, day_num):
            trade_mins = 0  # 开盘到现在的交易分钟数

            def consume_trade_mins(x):
                """
                闭包 计算开盘到现在的交易时长
                :param x:
                :return:
                """
                nonlocal trade_mins
                trade_mins = trade_mins + 1
                return int(trade_mins)

            cur_date = (start_date + datetime.timedelta(days=i)).replace(hour=0, minute=0, second=0, microsecond=0)
            night_open_time = cur_date + datetime.timedelta(minutes=60 * self.NIGHT_TRADE_TIME_INFO[0])
            night_close_time = night_open_time + datetime.timedelta(minutes=60 * self.NIGHT_TRADE_TIME_INFO[1])
            df.loc[str(night_open_time): str(night_close_time), 'trade_date'] = str(cur_date.date())
            df.loc[str(night_open_time): str(night_close_time), 'trade_time'] = df[str(night_open_time): str(
                night_close_time)].trade_date.apply(consume_trade_mins)

            offset_day = 1

            while (True):
                day_open_time = cur_date + datetime.timedelta(days=offset_day) + datetime.timedelta(
                    minutes=60 * self.DAY_TRADE_TIME_INFO[0])
                day_close_time = day_open_time + datetime.timedelta(minutes=60 * self.DAY_TRADE_TIME_INFO[1])
                if len(df[str(day_open_time): str(day_close_time)]):
                    df.loc[str(day_open_time): str(day_close_time), 'trade_date'] = str(day_close_time.date())
                    df.loc[str(day_open_time): str(day_close_time), 'trade_time'] = df[
                                                                                    str(day_open_time): str(
                                                                                        day_close_time)].trade_time.apply(
                        consume_trade_mins)
                    break
                offset_day = offset_day + 1
                daily_info['calendar'] = {}
                daily_info['trading'] = {}
            if len(df[str(night_open_time): str(day_close_time)]):
                daily_info[str(cur_date.date())] = {
                    'start_time': df[str(night_open_time): str(day_close_time)].index[0].to_pydatetime(),
                    'end_time': df[str(night_open_time):str(day_close_time)].index[-1].to_pydatetime(),
                    'trade_minutes': [i.to_pydatetime() for i in df[str(night_open_time): str(day_close_time)].index]
                }
        return daily_info


if __name__ == '__main__':
    p = LogicTime(in_data_path='/Users/suchang/Code/Quant/wh_parser/data/raw/spilt',
                  process_num=4,
                  out_data_path='/Users/suchang/Code/Quant/wh_parser/data/raw/fin',
                  instruments_pk_data_path='/Users/suchang/Code/Quant/wh_parser/data/bundle/instruments.pk',
                  daily_info_path='/Users/suchang/Code/Quant/wh_parser/data/bundle/daily_info')

    p.execute()
