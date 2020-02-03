import os
import pickle
import hashlib
from dateutil.parser import parse
from wh_parser.clean2cache_pipeline.methods import methods_map


class Clean2CachePipeline(object):

    def __init__(self, clean_data_path, out_data_path, **config):
        """
        config: {
            freq:{
             '1m': xxx
            }
        }
        :param clean_data_path:
        :param out_data_path:
        :param config:
        :param cache_id: 缓存的id
        """
        if config is None:
            config = {}
        self.cache_id = hashlib.md5(pickle.dumps(config)).hexdigest()
        self._config = config
        self.clean_data_path = clean_data_path
        self.out_data_path = os.path.join(out_data_path, self.cache_id)
        self._date_clean_set = {}
        self._data_path = {}
        self.load_data()
        self._exist = True

        if not os.path.exists(os.path.join(self.out_data_path)):
            os.mkdir(self.out_data_path)
            self._exist = False

        if not os.path.exists(os.path.join(self.out_data_path, 'instruments')):
            os.mkdir(os.path.join(self.out_data_path, 'instruments'))
            self._exist = False

        if not os.path.exists(os.path.join(self.out_data_path, 'info')):
            os.mkdir(os.path.join(self.out_data_path, 'info'))
            self._exist = False

    def load_data(self):
        """
        加载数据
        :return:
        """
        bundle_path = os.path.join(self.clean_data_path, 'instruments')
        for root, dirs, files in os.walk(bundle_path):
            for i in files:
                if i.endswith('.q'):
                    order_book_id = i.split('.')[0]
                    self._data_path[order_book_id] = os.path.join(root, i)

        for k, v in self._data_path.items():
            with open(v, 'rb') as f:
                df = pickle.load(f)
            if not len(df):
                continue
            self._date_clean_set[k] = df

    def process_freq(self):
        """
        按频率处理数据 结果放入cache
        :return:
        """

        freq_info = self._config.get('freq', {})
        for freq, method in freq_info.items():
            out_path = os.path.join(self.out_data_path, 'instruments', freq)
            if not os.path.exists(out_path):
                os.mkdir(out_path)

            func = methods_map[method]
            for order_book_id in self._date_clean_set.keys():
                print("处理频率 {} 处理方法 {} 合约: {}".format(freq, method, order_book_id))
                res = func(self._date_clean_set[order_book_id], freq)
                res.to_pickle(os.path.join(out_path, order_book_id + '.q'))

    def process_trade_date(self):
        """
        处理交易天数
        :return:
        """
        dates = []
        for order_book_id, df in self._date_clean_set.items():
            dates.append(parse(df.trade_date[0]).date())
            dates.append(parse(df.trade_date[-1]).date())
        res = [min(dates), max(dates)]
        out_path = os.path.join(self.out_data_path, 'info')
        if not os.path.exists(out_path):
            os.mkdir(out_path)
        with open(os.path.join(out_path, 'trade_date.q'), 'wb') as f:
            pickle.dump(res, f)

    def process(self):
        self.process_freq()
        self.process_trade_date()

    @property
    def exist(self):
        return self._exist
