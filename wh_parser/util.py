import datetime
import dateparser


def get_int_date(date):
    if isinstance(date, int):
        return date

    try:
        return int(datetime.datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m%d"))
    except:
        pass

    try:
        return int(datetime.datetime.strptime(date, "%Y%m%d").strftime("%Y%m%d"))
    except:
        pass

    if isinstance(date, (datetime.date)):
        return int(date.strftime("%Y%m%d"))

    raise ValueError("unknown date {}".format(date))


def convert_freq_to_pd(frequency):
    """
    转换系统频率到pandas频率
    :param frequency:
    :return:
    """
    freq = frequency[-1]
    num = frequency[:-1]

    if freq == 'm':
        return num + 'min'
    elif freq == 'd':
        return num + 'D'
    elif freq == 'h':
        return num + 'H'


def trans_str_to_datetime(datetime_str):
    """ 使用dataparser把 datetime 字符串转换成 datetime.datetime 对象

    :param datetime_str: dateutil 支持多种格式，比如 2012-08-29 11:38:22
    """
    return dateparser.parse(datetime_str.strip())


def deep_update_dict(src: dict, obj: dict):
    """
    深度更新dict 数组合并
    :param src:
    :param obj:
    :return:
    """
    for k in obj.keys():
        if type(obj[k]) is dict:
            if src.get(k) is None:
                src[k] = {}
            deep_update_dict(src[k], obj[k])
        elif type(obj[k]) is list:
            if src.get(k) is None:
                src[k] = list()
            src[k].extend(obj[k])
        else:
            src[k] = obj[k]

def split_order_book(order_book_id: str):
    """
    对合约拆分成品种和年份俩个部分
    :param order_book_id:
    :return:
    """
    return order_book_id[:-4], order_book_id[-4:]
