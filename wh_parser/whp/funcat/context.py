# -*- coding: utf-8 -*-
#

import datetime

import six


from .utils import get_int_date, parser_str_to_time, parser_time_to_str,parser_freq
from typing import List



class ExecutionContext(object):

    def __init__(self, date=None, order_book_id=None, data_backend=None, freq="1d", start_date="1990-01-01"):
        self._current_date = parser_str_to_time(date)
        self._start_date = parser_str_to_time(start_date)
        self._order_book_id = order_book_id
        self._data_backend = data_backend
        self._freq = freq



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

    def copy(self):
        return ExecutionContext(date=parser_time_to_str(self._current_date), order_book_id=self._order_book_id,
                                data_backend=self._data_backend,
                                freq=self._freq,
                                start_date=parser_time_to_str(self._start_date))




class ExecutionContextStack(object):

    def __init__(self, stack:List[ExecutionContext] = None):
        if stack is None:
            stack = list()
        self.stack = stack

    def get_active(self):
        return self.stack[-1]

    def set_current_date(self, date):
        """set current simulation date
        :param date: string date, "2016-01-04"
        """
        self.get_active()._set_current_date(date)

    def get_current_date(self):
        return self.get_active()._current_date

    def set_start_date(self, date):
        self.get_active()._set_start_date(date)

    
    def get_start_date(self):
        return self.get_active()._start_date

    def set_current_security(self, order_book_id):
        """set current watching order_book_id
        :param order_book_id: "000002.XSHE"
        """
        self.get_active()._order_book_id = order_book_id

    def get_current_freq(self):
        return self.get_active()._freq

    def set_current_freq(self, freq):
        self.get_active()._freq = freq

    def get_current_security(self):
        return self.get_active()._order_book_id

    def set_data_backend(self, data_backend):
        """set current watching order_book_id
        :param order_book_id: "000002.XSHE"
        """
        self.get_active()._data_backend = data_backend

    def get_data_backend(self):
        return self.get_active()._data_backend

    def set_attr(self, attr, value):
        setattr(self.get_active(), attr, value)

    def get_attr(self, attr):
        return getattr(self.get_active(), attr, None)


    def diff(self, diff=None):
        ctx = self.get_active().copy()
        if diff:
            if diff.get("order_book_id"):
                ctx._order_book_id = diff.get("order_book_id")
            if diff.get("freq"):
                ctx._freq = diff.get("freq")
            return ctx
        return ctx

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

