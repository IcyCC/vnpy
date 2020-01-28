# 处理主力合约
import os
from wh_parser.data_pipeline import SingleDataPipelineBase
import numpy as np
import pandas as pd


class LogPrice(SingleDataPipelineBase):
    """
    处理任务 从 splte -> fin
    结构
    """

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
        df = pd.read_csv(
            file
        )
        if not len(df):
            print("文件 {} 为空".format(file))
            return
        df.index = pd.to_datetime(
            df.timestamp.copy()
        )
        df_s = df.shift()

        df_s.close[0] = self.LOG_PRICE_BASE
        df_s.open[0] = self.LOG_PRICE_BASE
        df_s.high[0] = self.LOG_PRICE_BASE
        df_s.low[0] = self.LOG_PRICE_BASE

        df.close = np.cumsum(
            np.log2(
                df.close / df_s.close
            )
        ) * self.SCALE_BASE

        df.open = np.cumsum(
            np.log2(
                df.open / df_s.open
            )
        ) * self.SCALE_BASE

        df.high = np.cumsum(
            np.log2(
                df.high / df_s.high
            )
        ) * self.SCALE_BASE

        df.low = np.cumsum(
            np.log2(
                df.low / df_s.low
            )
        ) * self.SCALE_BASE

        if not os.path.exists(self.out_data_path):
            os.mkdir(self.out_data_path)
        df.to_csv(os.path.join(self.out_data_path, order_book_id + 'l' + '.csv'), index=False)


if __name__ == '__main__':
    p = LogPrice(in_data_path='/Users/suchang/Code/Quant/wh_parser/data/raw/im',
                 process_num=8,
                 out_data_path='/Users/suchang/Code/Quant/wh_parser/data/raw/iml', )

    p.execute()
