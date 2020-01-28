import pandas as pd
from wh_parser.util import convert_freq_to_pd

methods_map = {}


def add_methods(name=''):
    """
    注册函数
    :param name:
    :return:
    """

    def wrapper(func):
        methods_map[name] = func
        return func

    return wrapper


@add_methods('default')
def default_process(df, freq):
    """
    默认的转换频率的方式
    :param df:
    :param freq:
    :return:
    """
    # 删除夜盘的数据
    if freq == '1m':
        return df

    df = df.resample(convert_freq_to_pd(freq), label='right', closed='right')
    res = pd.DataFrame({
        'close': df.close.last(),
        'open': df.open.first(),
        'high': df.high.max(),
        'low': df.high.min(),
        'volume': df.volume.sum(),
        'open_interest': df.open_interest.last(),
        'total_turnover': df.total_turnover.sum(),
        'order_book_id': df.order_book_id.last(),
    }
    )
    res = res.dropna(how='any', thresh=3)
    return res
