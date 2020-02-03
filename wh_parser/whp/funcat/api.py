# -*- coding: utf-8 -*-
import numpy as np

from .time_series import MarketDataSeries
from .func import (
    SumSeries,
    AbsSeries,
    StdSeries,
    SMASeries,
    MovingAverageSeries,
    WeightedMovingAverageSeries,
    ExponentialMovingAverageSeries,
    CrossOver,
    minimum,
    maximum,
    every,
    count,
    hhv,
    llv,
    Ref,
    iif,
)

from typing import Dict, List
from .context import ExecutionContextStack
from .helper import select


def registe_sys_data_api(ctx:ExecutionContextStack, api_env:Dict):
    
    # create open high low close volume datetime
    for name in ["open", "high", "low", "close", "volume", "datetime"]:
        dtype = np.float64 if name != "datetime" else np.uint64
        cls = type("{}Series".format(name.capitalize()), (MarketDataSeries, ), {"name": name, "dtype": dtype, 'ctx':ctx})
        for var in [name[0], name[0].upper(), name.upper()]:
            api_env[var] = cls
    api_env['VOL'] = api_env['VOLUME']


def registe_sys_func_api(ctx:ExecutionContextStack, api_env:Dict):
    api_env['MA'] = MovingAverageSeries
    api_env['WMA'] = WeightedMovingAverageSeries
    api_env['EMA'] = ExponentialMovingAverageSeries
    api_env['SMA'] = SMASeries

    api_env['SUM'] = SumSeries
    api_env['ABS'] = AbsSeries
    api_env['STD'] = StdSeries

    api_env['CROSS'] = CrossOver
    api_env['REF'] = Ref
    api_env['MIN'] = minimum
    api_env['MAX'] = maximum
    api_env['EVERY'] = every
    api_env['COUNT'] = count
    api_env['HHV'] = hhv
    api_env['LLV'] = llv
    api_env['IF'] = iif
    api_env['IIF'] = iif