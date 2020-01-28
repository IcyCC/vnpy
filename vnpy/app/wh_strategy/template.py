""""""
from abc import ABC
from copy import copy
from typing import Any, Callable, List, Dict

from vnpy.trader.constant import Interval, Direction, Offset
from vnpy.trader.object import BarData, TickData, OrderData, TradeData
from vnpy.trader.utility import virtual, BarGenerator
from vnpy.app.cta_strategy.base import StopOrder, EngineType


from .wh_parser import funcat
from .base import WhBarTriggerMode

def Singals2PosAlgoTemplate(ABC):
    """
    信号组到仓位的转换
    """

    def __init__(
            singal_engine: Any,
            strategy_name: str,
            setting: dict):
        pass

    @virtual
    def cover(self, signals_pos: List[float]) -> List[float]:
        pass


class MultiSingalStrategyTamplate(ABC):
    """
    通用策略模板， 输入多种信号输出多种信号
    """

    def __init__(
        self,
        author: str,
        singal_engine: Any,
        strategy_name: str,
        setting: dict,
        parameters: List[str],
        sigal2pos_algo: Singals2PosAlgoTemplate,
        vt_symbols: List[str],
        bar_trigger_model: WhBarTriggerMode,
        default_vt_symbols_idx: 0
    ):

        self.author = author
        self.singal_engine = singal_engine
        self.strategy_name = strategy_name
        self.inited = False
        self.trading = False
        self.pos = 0
        self.sigal2pos_algo = sigal2pos_algo
        self.bar_trigger_model = bar_trigger_model

        # Copy a new variables list here to avoid duplicate insert when multiple
        # strategy instances are created with the same strategy class.
        self.variables = list()
        self.variables.insert(0, "inited")
        self.variables.insert(1, "trading")
        self.variables.insert(2, "pos")

        self.parameters = list()
        if parameters:
            self.parameters = parameters
        self.update_setting(setting)

        self.default_vt_symbols_idx = default_vt_symbols_idx
        self.vt_symbols = vt_symbols
        self.default_vt_symbols = ''
        self.singals_pos = list([0 for v in vt_symbols])
        self.positions = list([0 for v in vt_symbols])
        self.vt_orderids = list()

    @property
    def default_vt_symbol(self):
        return self.vt_symbols[self.default_vt_symbols_idx]

    def update_setting(self, setting: dict):
        """
        Update strategy parameter wtih value in setting dict.
        """
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    def get_class_parameters(self):
        """
        Get default parameters dict of strategy class.
        """
        class_parameters = {}
        for name in self.parameters:
            class_parameters[name] = getattr(self, name)
        return class_parameters

    def get_parameters(self):
        """
        Get strategy parameters dict.
        """
        strategy_parameters = {}
        for name in self.parameters:
            strategy_parameters[name] = getattr(self, name)
        return strategy_parameters

    def get_variables(self):
        """
        Get strategy variables dict.
        """
        strategy_variables = {}
        for name in self.variables:
            strategy_variables[name] = getattr(self, name)
        return strategy_variables

    def get_data(self):
        """
        Get strategy data.
        """
        strategy_data = {
            "strategy_name": self.strategy_name,
            "class_name": self.__class__.__name__,
            "author": self.author,
            "parameters": self.get_parameters(),
            "variables": self.get_variables(),
        }
        return strategy_data

    def get_engine_type(self):
        """
        Return whether the cta_engine is backtesting or live trading.
        """
        return self.singal_engine.get_engine_type()


    @virtual
    def on_init(self):
        """
        Callback when strategy is inited.
        """
        pass

    @virtual
    def on_start(self):
        """
        Callback when strategy is started.
        """
        pass

    @virtual
    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        pass

    @virtual
    def on_tick(self, tick: Dict[str,TickData]):
        """
        Callback of new tick data update.
        """
        if self.trading:
            self.trade()


    def on_bars(self, bars: Dict[str,BarData]):
        """
        Callback of new bar data update.
        """
        if self.bar_trigger_model == WhBarTriggerMode.SINGLE:
            self.handle_bars(bars)
        
        if self.trading:
            self.trade()

    
    @virtual
    def handle_bars(self, bars: Dict[str,BarData]):
        pass
    

    @virtual
    def on_trade(self, trade: TradeData):
        """
        Callback of new trade data update.
        """
        pass

    @virtual
    def on_order(self, order: OrderData):
        """
        Callback of new order data update.
        """
        pass

    @virtual
    def on_stop_order(self, stop_order: StopOrder):
        """
        Callback of stop order update.
        """
        pass

    def send_order(
        self,
        vt_symbol: str,
        direction: Direction,
        offset: Offset,
        volume: float,
        stop: bool = False,
        lock: bool = False
    ):
        """
        Send a new order.
        """
        if self.trading:
            vt_orderids = self.singal_engine.send_order(
                self, direction, offset, volume, stop, lock
            )
            return vt_orderids
        else:
            return []

    def buy(self, vt_symbol: str, volume: float, stop: bool = False, lock: bool = False):
        """
        Send buy order to open a long position.
        """
        return self.send_order(vt_symbol, Direction.LONG, Offset.OPEN, volume, stop, lock)

    def sell(self, vt_symbol: str, volume: float, stop: bool = False, lock: bool = False):
        """
        Send sell order to close a long position.
        """
        return self.send_order(vt_symbol, Direction.SHORT, Offset.CLOSE, volume, stop, lock)

    def short(self, vt_symbol: str, volume: float, stop: bool = False, lock: bool = False):
        """
        Send short order to open as short position.
        """
        return self.send_order(vt_symbol, Direction.SHORT, Offset.OPEN,  volume, stop, lock)

    def cover(self, vt_symbol: str,
             volume: float, stop: bool = False, lock: bool = False):
        """
        Send cover order to close a short position.
        """
        return self.send_order(vt_symbol, Direction.LONG, Offset.CLOSE, volume, stop, lock)

    def cancel_order(self, vt_orderid: str):
        """
        Cancel an existing order.
        """
        if self.trading:
            self.singal_engine.cancel_order(self, vt_orderid)

    def cancel_all_orders(self):
        if self.trading:
            self.singal_engine.cancel_all(self)


    def trade(self):
        self.cancel_all_orders()
        target_pos = self.sigal2pos_algo.cover(self.singals_pos)

        for vt_idx, target in enumerate(target_pos):
            pos_change = target - self.positions[vt_idx]
            if not pos_change:
                continue
            
            vt_symbol = self.vt_symbols[vt_idx]
            
            if self.get_engine_type() == EngineType.BACKTESTING:
                if pos_change > 0:
                    vt_orderids = self.buy(vt_symbol, abs(pos_change))
                else:
                    vt_orderids = self.short(vt_symbol, abs(pos_change))
                self.vt_orderids.extend(vt_orderids)

            else:
                if self.vt_orderids:
                    return

                if pos_change > 0:
                    if self.pos < 0:
                        if pos_change < abs(self.pos):
                            vt_orderids = self.cover(vt_symbol,pos_change)
                        else:
                            vt_orderids = self.cover(vt_symbol,abs(self.pos))
                    else:
                        vt_orderids = self.buy(vt_symbol, abs(pos_change))
                else:
                    if self.pos > 0:
                        if abs(pos_change) < self.pos:
                            vt_orderids = self.sell(vt_symbol, abs(pos_change))
                        else:
                            vt_orderids = self.sell(vt_symbol, abs(self.pos))
                    else:
                        vt_orderids = self.short(vt_symbol, abs(pos_change))
                self.vt_orderids.extend(vt_orderids)


class WhMultiSingalStrategy(MultiSingalStrategyTamplate):

    def __init__(
        self,
        author ,
        singal_engine: Any,
        strategy_name: str,
        setting: dict,
        parameters: List[str],
        sigal2pos_algo: Singals2PosAlgoTemplate,
        vt_symbols: List[str],
        main_indicator: funcat.Indicator
    ):

        super(WhMultiSingalStrategy, self).__init__(
            author=author,singal_engine=singal_engine,strategy_name=strategy_name,
            setting=setting, 
            parameters=parameters,
            sigal2pos_algo=sigal2pos_algo,
            vt_symbols = vt_symbols
        )
        self.main_indicator = main_indicator

    def handle_bars(self, bars:Dict[str, BarData]):
        for vt_symbol, bar in bars.items():
            funcat.S(vt_symbol)
            funcat.S(bar.datetime)
            self.main_indicator.compute()
            self.main_indicator.call_cond()

