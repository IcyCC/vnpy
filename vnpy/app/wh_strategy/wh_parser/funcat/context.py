# -*- coding: utf-8 -*-
#

import datetime

import six


from .utils import get_int_date, parser_str_to_time, parser_time_to_str,parser_freq


class ExecutionContext(object):
    stack = []

    def __init__(self, date=None, order_book_id=None, data_backend=None, freq="1d", start_date="1990-01-01"):
        self._current_date = parser_str_to_time(date)
        self._start_date = parser_str_to_time(start_date)
        self._order_book_id = order_book_id
        self._data_backend = data_backend
        self._freq = freq

    def _push(self):
        self.stack.append(self)

    def _pop(self):
        popped = self.stack.pop()
        if popped is not self:
            raise RuntimeError("Popped wrong context")
        return self

    def __enter__(self):
        self._push()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._pop()

    def _convert_date_to_int(self, date):
        if isinstance(date, six.string_types):
            date = get_int_date(date)
        elif isinstance(date, datetime.date):
            date = int(date.strftime("%Y%m%d"))
        return date

    def _set_current_date(self, date):
        self._current_date = parser_str_to_time(date)

    def _set_start_date(self, date):
        self._start_date = parser_str_to_time(date)

    @classmethod
    def get_active(cls):
        return cls.stack[-1]

    @classmethod
    def set_current_date(cls, date):
        """set current simulation date
        :param date: string date, "2016-01-04"
        """
        cls.get_active()._set_current_date(date)

    @classmethod
    def get_current_date(cls):
        return cls.get_active()._current_date

    @classmethod
    def set_start_date(cls, date):
        cls.get_active()._set_start_date(date)

    @classmethod
    def get_start_date(cls):
        return cls.get_active()._start_date

    @classmethod
    def set_current_security(cls, order_book_id):
        """set current watching order_book_id
        :param order_book_id: "000002.XSHE"
        """
        cls.get_active()._order_book_id = order_book_id

    @classmethod
    def get_current_freq(cls):
        return cls.get_active()._freq

    @classmethod
    def set_current_freq(cls, freq):
        cls.get_active()._freq = freq

    @classmethod
    def get_current_security(cls):
        return cls.get_active()._order_book_id

    @classmethod
    def set_data_backend(cls, data_backend):
        """set current watching order_book_id
        :param order_book_id: "000002.XSHE"
        """
        cls.get_active()._data_backend = data_backend

    @classmethod
    def get_data_backend(cls):
        return cls.get_active()._data_backend

    @classmethod
    def set_attr(cls, attr, value):
        setattr(cls.get_active(), attr, value)

    @classmethod
    def get_attr(cls, attr):
        return getattr(cls.get_active(), attr, None)

    def copy(self):
        return ExecutionContext(date=parser_time_to_str(self._current_date), order_book_id=self._order_book_id,
                                data_backend=self._data_backend,
                                freq=self._freq,
                                start_date=parser_time_to_str(self._start_date))
    @classmethod
    def diff(cls, diff=None):
        ctx = cls.get_active().copy()
        if diff:
            if diff.get("order_book_id"):
                ctx._order_book_id = diff.get("order_book_id")
            if diff.get("freq"):
                ctx._freq = diff.get("freq")
            return ctx
        return ctx


def set_data_backend(backend):
    ExecutionContext.set_data_backend(backend)


def set_current_security(order_book_id):
    ExecutionContext.set_current_security(order_book_id)


def set_start_date(date):
    ExecutionContext.set_start_date(date)


def set_current_date(date):
    ExecutionContext.set_current_date(date)


def set_current_freq(freq):
    ExecutionContext.set_current_freq(freq)


def symbol(order_book_id):
    """获取股票代码对应的名字
    :param order_book_id:
    :returns:
    :rtype:
    """
    data_backend = ExecutionContext.get_data_backend()
    return data_backend.symbol(order_book_id)
