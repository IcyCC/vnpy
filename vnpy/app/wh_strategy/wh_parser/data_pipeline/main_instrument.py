# 处理主力合约
import os
from wh_parser.data_pipeline import TaskPipelineBase
import pickle
import datetime
from wh_parser.util import trans_str_to_datetime
import numpy as np
import pandas as pd


class MainInstrument(TaskPipelineBase):
    """
    处理任务 从 fin -> im
    结构
    """
    name = '主力合约提取'

    def __init__(self, instruments_pk_data_path, over_limit=0.01, ignore_up_days=2, process_rank=2, **kwargs):
        super().__init__(**kwargs)
        self.OVER_LIMIT = over_limit
        self.IGNORE_UP_DAYS = ignore_up_days
        self.PROCESS_RANK = process_rank
        with open(instruments_pk_data_path, 'rb') as f:
            ins = pickle.load(f)
            self.instruments_store = {i['order_book_id']: i for i in ins}

    def race(self, c, pre):
        """

        :param c:
        :param pre:
         """
        if c["id"] == pre["id"]:
            return True
        if c["id"] > pre["id"]:
            if c["total_turnover"] > (1 + self.OVER_LIMIT) * pre["total_turnover"]:
                print("竞争成功")
                return True
            else:
                print("竞争失败 阈值不足")
                return False
        else:
            print("竞争失败 不倒退")
            return False

    def do_task(self, task):
        data_set = {}
        daily_turnover = {}
        start_date = None
        end_date = None
        instrument = task['instrument']
        for order_id, path in task['data_paths'].items():

            df = pd.read_csv(
                path
            )
            if not len(df):
                continue
            data_set[order_id] = df
            data_set[order_id].index = pd.to_datetime(data_set[order_id].timestamp.copy())

            if start_date is None or data_set[order_id].index[0].to_pydatetime() < start_date:
                start_date = data_set[order_id].index[0].to_pydatetime()
            if end_date is None or data_set[order_id].index[0].to_pydatetime() > end_date:
                end_date = data_set[order_id].index[-1].to_pydatetime()
            data_set[order_id].timestamp = pd.to_datetime(data_set[order_id].timestamp)
            daily_turnover[order_id] = data_set[order_id].total_turnover.resample('1D').sum().replace(0, np.nan)
            daily_turnover[order_id] = daily_turnover[order_id].dropna()
        allow_instruments = sorted(list(data_set.keys()))
        total_ranks = {}
        if start_date is None or end_date is None:
            raise Exception
        for d in range(0, (end_date - start_date).days):
            cur_date = start_date + datetime.timedelta(days=d)
            print("处理 {} 排序处理日期: {}".format(instrument, str(cur_date)))
            day_rank = []
            # 遍历所有合约
            for ins in allow_instruments:
                order_book_id = ins

                if not len(data_set[ins].index):
                    # 空数据
                    continue
                # 过滤不计入的
                if cur_date < daily_turnover[ins].index[0] or cur_date > daily_turnover[ins].index[
                    -self.IGNORE_UP_DAYS]:
                    # 不在交易时间
                    continue
                total_turnover = daily_turnover[ins].get(str(cur_date.date()), 0)
                if total_turnover <= 0.1:
                    continue
                if cur_date + datetime.timedelta(days=self.IGNORE_UP_DAYS) > trans_str_to_datetime(
                        self.instruments_store[order_book_id]["maturity_date"]):
                    # 快要到期
                    continue

                day_rank.append({
                    "id": ins,
                    "total_turnover": total_turnover,
                    "order_book_id": order_book_id
                })
                day_rank.sort(key=lambda item: item['total_turnover'], reverse=True)
                if day_rank:
                    total_ranks[str(cur_date.date())] = day_rank.copy()

        total_main = {}
        days = sorted([trans_str_to_datetime(d) for d in total_ranks.keys()])

        for i in range(1, len(days)):
            cur_day = str(days[i].date())
            pre_day = str(days[i - 1].date())
            print("处理 {} 主力 日期: {}".format(instrument, cur_day))

            cur_ranks = {i["id"]: i for i in total_ranks[pre_day]}
            if total_main.get(pre_day) is None:
                total_main[cur_day] = total_ranks[cur_day][0]
                continue
            for c in total_ranks[cur_day]:
                if self.race(c, total_main[pre_day]):
                    total_main[cur_day] = c
                    break
                else:
                    pre_main_id = total_main[pre_day]["id"]
                    if cur_ranks.get(pre_main_id) is None:
                        total_main[cur_day] = {
                            **total_main[pre_day],
                            "total_turnover": -1
                        }
                    else:
                        total_main[cur_day] = cur_ranks[pre_main_id]
        # 处理主力连续
        main_df = pd.DataFrame()
        for day, ins in total_main.items():
            print("拼接主力: {}  {}".format(day, ins["id"]))
            main_df = main_df.append(
                data_set[ins["id"]][day]
            )

        # 处理剩余的合约
        total_sub = {}
        for i in range(1, len(days)):
            cur_day = str(days[i].date())
            pre_day = str(days[i - 1].date())
            print("处理 {} 次主力 日期: {}".format(instrument, cur_day))
            day_sub = []
            for ins in total_ranks[pre_day]:
                if ins["id"] == total_main[cur_day]["id"]:
                    # 等于当日主力 跳过
                    continue
                day_sub.append(ins)
                if len(day_sub) >= self.PROCESS_RANK:
                    break
            total_sub[cur_day] = day_sub.copy()

        sub_dfs = []

        for i in range(self.PROCESS_RANK):
            sub_dfs.append(pd.DataFrame())

        for day, ins in total_sub.items():
            for i in range(len(ins)):
                print("次主力拼接:{}, id: {}".format(day, ins[i]["id"]))
                sub_dfs[i] = sub_dfs[i].append(data_set[ins[i]["id"]][day])

        print("持久化到硬盘")

        main_df.to_csv(os.path.join(self.out_data_path, instrument + '99' + '.csv'),
                       index=False)

        for i in range(len(sub_dfs)):
            sub_dfs[i].to_csv(
                os.path.join(self.out_data_path, instrument + '9' + str(i) + '.csv'),
                index=False)

    def task_gen(self):
        instruments = os.listdir(self.in_data_path)
        for ins in instruments:
            task = {
                'instrument': ins,
                'data_paths': {}
            }
            for root, dirs, files in os.walk(os.path.join(self.in_data_path, ins)):
                for file in files:
                    if file.endswith('.csv'):
                        # 是数据文件
                        order_book_id = os.path.splitext(file)[0]
                        order_id = int(order_book_id[-4:])
                        if order_id == 9999:
                            continue
                        task['data_paths'][order_book_id] = os.path.join(root, file)

            yield task


if __name__ == '__main__':
    p = MainInstrument(in_data_path='/Users/suchang/Code/Quant/wh_parser/data/raw/fin',
                       process_num=4,
                       out_data_path='/Users/suchang/Code/Quant/wh_parser/data/raw/im',
                       instruments_pk_data_path='/Users/suchang/Code/Quant/wh_parser/data/bundle/instruments.pk')

    p.execute()
