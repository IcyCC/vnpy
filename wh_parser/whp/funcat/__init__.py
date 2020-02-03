# -*- coding: utf-8 -*-
#

import pkgutil

__version__ = '0.0.1'

version_info = tuple(int(v) if v.isdigit() else v for v in __version__.split('.'))

__main_version__ = "%s.%s.x" % (version_info[0], version_info[1])

del pkgutil

from .api import *
from .indicators import *
from .context import ExecutionContext, ExecutionContextStack