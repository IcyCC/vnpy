# 原始数据拆分
import os
from wh_parser.data_pipeline import TaskPipelineBase
import pickle
from collections import defaultdict
import numpy as np
import pandas as pd


class SplitInstruments(TaskPipelineBase):
    """
    处理任务 从 raw -> split
    结构
    """
    name = '拆分合约'

    def __init__(self, instruments_pk_data_path, start_instruments, **kwargs):
        super().__init__(**kwargs)
        if not start_instruments:
            start_instruments = []
        self.START_INSTRUMENTS = start_instruments
        with open(instruments_pk_data_path, 'rb') as f:
            ins = pickle.load(f)
            self.instruments_store = {i['order_book_id']: i for i in ins}

    def task_gen(self):
        # 预读
        pre_file_set = defaultdict(lambda: None)
        for root, dirs, files in os.walk(self.in_data_path):
            for file in files:
                if file.endswith('.csv'):
                    # 是数据文件
                    stage = root.split(os.sep)[-1]
                    year = root.split(os.sep)[-2]
                    if not pre_file_set[year]:
                        pre_file_set[year] = defaultdict(lambda: None)
                    if not pre_file_set[year][stage]:
                        pre_file_set[year][stage] = []
                    pre_file_set[year][stage].append(os.path.join(root, file))
        for star in self.START_INSTRUMENTS:
            task = {
                'instrument': star,
                'data': []
            }
            for year, stage_set in pre_file_set.items():
                for stage, files in stage_set.items():
                    for file in files:
                        f = os.path.split(file)[1]
                        instrument = str(os.path.splitext(f)[0][2:]).upper()
                        month = instrument[-2:]  # 合约月份
                        variety = instrument[:-2]  # 品种
                        if star == variety:  ## 关注品种
                            task['data'].append({
                                'month': month,
                                'file': file
                            })
            yield task

    def split_df(self, df, ins, month):
        """
        拆分
        :param df:
        :param ins: 品种
        :param month: 月
        :return:
        """
        p = os.path.join(self.out_data_path, ins)
        if not os.path.exists(p):
            os.mkdir(p)
        start_year = df.index[0].to_pydatetime().year
        end_year = df.index[-1].to_pydatetime().year
        for year in range(start_year, end_year + 1):
            if month == 'MI':
                # df.to_csv(os.path.join(p, ins + '9999' + '.csv'), index=False)
                continue
            order_book_id = ins + str(year)[2:] + month
            instrument = self.instruments_store.get(order_book_id)
            if instrument is None:
                print("WARNNING: 没找到合约:{} 的信息".format(order_book_id))
                continue
            res = df[instrument['listed_date']: instrument['de_listed_date']]
            res['order_book_id'] = order_book_id
            res.to_csv(os.path.join(p, order_book_id + '.csv'), index=False)
            print("拆分成 {} 合约".format(order_book_id))

    def do_task(self, task):
        pre_data_set = defaultdict(lambda: None)
        star = task['instrument']
        for d in task['data']:
            month = d['month']
            if month == 'MI':
                continue
            df = pd.read_csv(
                d['file'],
                header=None,
                names=['date', 'time', 'open', 'high', 'low', 'close', 'volume',
                       'total_turnover'],
            )
            df['timestamp'] = df.date + ' ' + df.time

            df.index = pd.to_datetime(df.timestamp.copy())

            df['datetime'] = df.index.strftime("%Y%m%d%H%M%S")
            df.datetime = df.datetime.apply(int)

            del df['date']
            del df['time']
            df['open_interest'] = np.nan

            df['true_close'] = df.close

            df['pre_close'] = df.close.shift()
            if pre_data_set[month] is None:
                pre_data_set[month] = df
            else:
                pre_data_set[month] = pre_data_set[month].append(df, sort=True)

        for k in pre_data_set.keys():
            print("处理合约月份", k)
            pre_data_set[k] = pre_data_set[k].sort_index()
            self.split_df(pre_data_set[k], star, k)


if __name__ == '__main__':
    p = SplitInstruments(in_data_path='/Users/suchang/Code/Quant/wh_parser/data/raw/pre',
                         out_data_path='/Users/suchang/Code/Quant/wh_parser/data/raw/spilt',
                         process_num=4,
                         instruments_pk_data_path='/Users/suchang/Code/Quant/wh_parser/data/bundle/instruments.pk',
                         start_instruments=[
                             'AU',
                             'C',
                             'RB',
                             'CU'
                         ])
    p.execute()
