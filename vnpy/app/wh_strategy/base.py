from enum import Enum
from vnpy.trader.object import BarData, TickData, OrderData, TradeData, ContractData
from vnpy.trader.constant import (Direction, Offset, Exchange,
                                  Interval, Status)
from typing import Dict, List

from vnpy.app.cta_strategy.backtesting import DailyResult


class WhBarTriggerMode(Enum):

    SINGLE = 'single'  # 每个品种触发一次
    ALL = 'all'  # 搜集完成触发一次


class MultiDailyResult:
    """"""

    def __init__(self, date: date, close_prices: Dict[str,float], contracts: Dict[str, ContractData]):
        """"""
        self.date = date
        self.close_prices = close_prices
        self.pre_close = 0

        self.trades: List[TradeData] = []
        self.trade_count = 0

        self.start_poses = 0
        self.end_poses = 0

        self.turnover = 0
        self.commission = 0
        self.slippage = 0

        self.trading_pnl = 0
        self.holding_pnl = 0
        self.total_pnl = 0
        self.net_pnl = 0

        self.contracts = contracts

        self.daily_results: Dict[str,DailyResult] = {vt_symbol: DailyResult(
            self.date, close_prices[vt_symbol]) for vt_symbol in contracts.keys()}

    def add_trade(self, trade: TradeData):
        """"""
        self.trades.append(trade)

    def calculate_pnl(
        self,
        pre_closes: Dict[str,float],
        start_poses: Dict[str,float],
        slippage: float,
    ):
        """"""
        # If no pre_close provided on the first day,
        # use value 1 to avoid zero division error

        self.start_poses = start_poses
        self.end_poses = start_poses
        self.slippage = slippage

        for trade in self.trades:
            self.daily_results[trade.vt_symbol].add_trade(trade)

        for vt_symbol, result in self.daily_results.items():
            result.calculate_pnl(
                pre_close=pre_closes[vt_symbol], 
                start_pos=start_poses[vt_symbol],
                size=self.contracts[ vt_symbol].size, 
                rate=self.contracts[vt_symbol].rate, 
                slippage=self.slippage, 
                inverse=self.contracts[vt_symbol].inverse
            )
            self.trading_pnl = self.trading_pnl + result.trading_pnl
            self.holding_pnl = self.holding_pnl + result.holding_pnl
            self.total_pnl = self.total_pnl + result.total_pnl
            self.net_pnl = self.net_pnl + result.net_pnl
            self.turnover = self.turnover + result.turnover
            self.commission = self.commission + result.commission

            self.end_poses[vt_symbol] = result.end_pos