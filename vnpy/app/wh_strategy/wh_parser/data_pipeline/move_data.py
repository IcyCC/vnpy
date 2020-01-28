# 处理主力合约
import os
from wh_parser.data_pipeline import SingleDataPipelineBase
import pickle
import pandas as pd


class MoveData(SingleDataPipelineBase):
    """
    处理任务 从 splte -> fin
    结构
    """

    name = '移动数据'

    DAY_TRADE_TIME_INFO = (6, 11)
    NIGHT_TRADE_TIME_INFO = (18, 11)

    def __init__(self,
                 log_price_base=10,
                 scale_base=1,
                 **kwargs):
        super().__init__(**kwargs)
        self.LOG_PRICE_BASE = log_price_base
        self.SCALE_BASE = scale_base

    def do_task(self, task):
        file = task
        order_book_id = os.path.splitext(os.path.split(file)[1])[0]
        df = pd.read_csv(file)
        if not len(df):
            print("无数据： {}".format(file))
            return
        df.index = pd.to_datetime(df.timestamp.copy())
        with open(os.path.join(self.out_data_path, order_book_id+'.q'), 'wb') as f:
            pickle.dump(df, f)


if __name__ == '__main__':
    p = MoveData(in_data_path='/Users/suchang/Code/Quant/wh_parser/data/raw/iml',
                 process_num=10,
                 out_data_path='/Users/suchang/Code/Quant/wh_parser/data/clean/instruments', )
    p2 = MoveData(in_data_path='/Users/suchang/Code/Quant/wh_parser/data/raw/im',
                  process_num=10,
                  out_data_path='/Users/suchang/Code/Quant/wh_parser/data/clean/instruments', )

    p.execute()
    p2.execute()
