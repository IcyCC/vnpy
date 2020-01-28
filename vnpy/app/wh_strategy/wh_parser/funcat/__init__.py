# -*- coding: utf-8 -*-
#

import pkgutil

__version__ = '0.0.1'

version_info = tuple(int(v) if v.isdigit() else v for v in __version__.split('.'))

__main_version__ = "%s.%s.x" % (version_info[0], version_info[1])

del pkgutil

from .api import *
from .indicators import *

from .data.tushare_backend import TushareDataBackend
from .context import ExecutionContext
from .context import ExecutionContext as funcat_execution_context

funcat_execution_context(date="2017-01-04",
                         order_book_id="000001.XSHG",
                         freq='1d',
                         data_backend=TushareDataBackend())._push()

from wh_parser.funcat.rqlapha_mod import *
from wh_parser.funcat.trader_mod.api import *

account = Account()