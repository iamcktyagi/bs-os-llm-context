from __future__ import annotations
from typing import TYPE_CHECKING
import numpy as np
from collections import OrderedDict, deque
from itertools import islice

from blueshift.lib.common.types import MaxSizedOrderedDict
from blueshift.lib.trades._order_types import PositionSide, OrderSide
from blueshift.lib.common.constants import PRECISION as _PRECISION
from blueshift.lib.exceptions import DataException
from blueshift.lib.common.constants import Frequency

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd

def compute_eod_per_trade_report(round_trips:list, frequency:str|Frequency = "minute", 
                                 max_len:int|None=None) -> dict:
    """
        Function to compute per-trade statistics.

        Note:
            `frequency` is used to report the holding period, defaults to
            `minute`. Other allowable values are "daily" or "second". The
            parameter `max_len`, if specified and less than the length of
            the `round_trips` list, will cause the list to be truncted and
            last `max_len` number of elements will be retained for further
            calculation.

        Args:
            ``round_trips (list)``: A list of completed trades

            ``frequency (str)``: Unit of holding periods calculation

            ``max_len (int)``: Maximum number of iterms to consider.

        Returns:
            Dict. A dictionary object with performance metrics as below.

            :total_trades: Total number of completed round trips.
            :profit_factor: Ratio of gross profit to gross losses.
            :per_trade: A dict containing further per trade stats.
            :min_profit: Minimum profit (in `per_trade`).
            :25p_profit: 25 percentile profit (in `per_trade`).
            :median_profit: Median profit (in `per_trade`).
            :75p_profit: 75 percentile profit (in `per_trade`).
            :max_profit: Maximum profit (in `per_trade`).
            :holding_time: A dict containing further holding_time metrics.
            :avg_holding_time: Maximum holding time (in `avg_holding_time`).
            :min_holding_time: Maximum holding time (in `avg_holding_time`).
            :25p_holding_time: Maximum holding time (in `avg_holding_time`).
            :median_profit: Maximum holding time (in `avg_holding_time`).
            :75p_holding_time: Maximum holding time (in `avg_holding_time`).
            :max_holding_time: Maximum holding time (in `avg_holding_time`).

    """
    n_trades = len(round_trips)
    if n_trades < 1:
        return {}

    if max_len:
        if max_len < n_trades:
            start_i = n_trades - max_len
            end_i = n_trades
            n_trades = max_len
            round_trips = list(islice(round_trips,start_i,end_i))

    pnls = []
    holding_periods = []
    out = {}
    gross_profit = gross_loss = net_profit = per_trade_profit = 0
    winning_trade = losing_trade = 0
    frequency = Frequency(frequency)

    for trade in round_trips:
        pnl = trade["pnl"]
        entry_dt = pd.Timestamp(trade["entry_time"])
        exit_dt = pd.Timestamp(trade["timestamp"])
        td = pd.Timedelta(exit_dt-entry_dt)
        if frequency == Frequency('1d'):
            td = td.days
        elif frequency == Frequency('1m'):
            td = int(td.total_seconds()/60)
        else:
            td = td.total_seconds()

        if td < 0:
            msg = f"entry:{entry_dt} before exit:{exit_dt}! "
            msg = msg + f"trade:{str(trade)}"
            raise DataException(msg=msg)

        if pnl > 0:
            gross_profit = gross_profit + pnl
            winning_trade = winning_trade + 1
        else:
            gross_loss = gross_loss - pnl
            losing_trade = losing_trade + 1

        pnls.append(pnl)
        holding_periods.append(td)

    net_profit = gross_profit - gross_loss
    per_trade_profit = net_profit/n_trades
    
    with np.errstate(invalid='ignore', divide='ignore'):
        percentile = np.percentile(pnls, [0, 25, 50, 75, 100])

    out["total_trades"] = n_trades

    if gross_loss > 0:
        out["profit_factor"] = round(gross_profit/gross_loss, _PRECISION)
    else:
        out["profit_factor"] = 0

    if losing_trade > 0:
        out["hit_ratio"] = round(winning_trade/n_trades, _PRECISION)
    else:
        out["hit_ratio"] = 0

    out["per_trade"] = {}
    out["per_trade"]["per_trade_profit"] = round(per_trade_profit, _PRECISION)

    if winning_trade > 0:
        out["per_trade"]["avg_winner"] = round(gross_profit/winning_trade, _PRECISION)
    else:
        out["per_trade"]["avg_winner"] = 0
    if losing_trade > 0:
        out["per_trade"]["avg_loser"] = -round(gross_loss/losing_trade, _PRECISION)
    else:
        out["per_trade"]["avg_loser"] = 0

    out["per_trade"]["min_profit"] = round(percentile[0], _PRECISION)
    out["per_trade"]["25p_profit"] = round(percentile[1], _PRECISION)
    out["per_trade"]["median_profit"] = round(percentile[2], _PRECISION)
    out["per_trade"]["75p_profit"] = round(percentile[3], _PRECISION)
    out["per_trade"]["max_profit"] = round(percentile[4], _PRECISION)

    average_holding_time = round(sum(holding_periods)/n_trades, _PRECISION)
    
    with np.errstate(invalid='ignore', divide='ignore'):
        percentile = np.percentile(holding_periods, [0, 25, 50, 75, 100])
    
    out["holding_time"] = {}
    out["holding_time"]["avg_holding_time"] = average_holding_time
    out["holding_time"]["min_holding_time"] = round(percentile[0], _PRECISION)
    out["holding_time"]["25p_holding_time"] = round(percentile[1], _PRECISION)
    out["holding_time"]["median_holding_time"] = round(percentile[2], _PRECISION)
    out["holding_time"]["75p_holding_time"] = round(percentile[3], _PRECISION)
    out["holding_time"]["max_holding_time"] = round(percentile[4], _PRECISION)

    return out

def compute_eod_txn_report(transactions:dict, n_sessions:int, max_len:int|None = None) -> dict:
    """
        A function to generate transaction reports

        Args:
            ``transactions (dict)``: A dict of transactoins keyed by dates.

            ``n_sessions (int)``: Number of sessions passed so far.

            ``max_len (int)``: Maximum number of elements to keep.

        Returns:
            Dict. A dictionary object with performance metrics as below.

            :days_traded: Total number of days with any trading.
            :trades_per_day: Average trades per day when there was at least one trade.
            :max_trade_per_day: Max trades done in a day.
            :avg_wait_time: Average wait time before fill.
            :complete_ratio: Percent of completed vs total orders.

    """
    trading_days = len(transactions)
    if trading_days < 1:
        return {}

    transactions = OrderedDict(transactions)
    if max_len:
        if max_len < trading_days:
            trading_days = max_len

    if n_sessions < 1:
        raise DataException(msg="too few sessions {n_sessions}.")
    biz_days = n_sessions - 1 if n_sessions > 1 else n_sessions

    complete_trades = total_orders = total_wait = 0
    n_trades = []
    count = 1
    for dt in reversed(transactions):
        if count > trading_days:
            break

        txns = transactions[dt]
        n_trades.append(len(txns))

        for txn in txns:
            if txn.is_done():
                complete_trades = complete_trades + 1

            try:
                exec_time = pd.Timestamp(txn.exchange_timestamp)
                sent_time = pd.Timestamp(txn.timestamp)
                dt = pd.Timedelta(exec_time-sent_time)
                #dt = max(0, dt.total_seconds())
                dt = dt.total_seconds()/60
                total_wait = total_wait + dt
            except Exception:
                pass

        count = count + 1

    total_orders = sum(n_trades)

    out = {}
    out['trading_days'] = biz_days
    out['days_traded'] = trading_days
    out['trades_per_day'] = round(sum(n_trades)/biz_days, _PRECISION)
    out['max_trade_per_day'] = max(n_trades)
    out['avg_wait_time'] = round(total_wait/total_orders, _PRECISION)
    out['fill_ratio'] = round(complete_trades/total_orders, _PRECISION)

    return out

def create_txns_frame(transactions:dict, tz:str, max_txns:int=50000, chunk_size:int=5000) -> pd.DataFrame:
    """ 
        Create a dataframe of transactions from dated transaction dict.
    """
    txns = MaxSizedOrderedDict({}, max_size=max_txns, chunk_size=chunk_size)
    for dt in transactions:
        txns_list = transactions[dt]
        for txn in txns_list:
                txns[txn.oid] = txn
    
    try:
        dt = [txns[key].exchange_timestamp \
              if txns[key].exchange_timestamp not in (None, 'None') \
              else txns[key].timestamp for key in txns]
    except Exception:
        dt = [txns[key].timestamp for key in txns]
    
    amount = [txns[key].filled for key in txns]
    sign = [1 if txns[key].side == OrderSide.BUY else -1 for key in txns]
    amount = [sign*amount for sign, amount in zip(sign, amount)]
    price = [txns[key].average_price for key in txns]
    symbol = [txns[key].asset.exchange_ticker for key in txns]
    remark = [txns[key].remark for key in txns]
    
    slippages = []
    for key in txns:
        o = txns[key]
        if o.price_at_entry == 0 or np.isnan(o.price_at_entry) or\
            o.filled==0:
            slippages.append(np.nan)
            continue
        if o.side == OrderSide.BUY:
            slippage = o.average_price - o.price_at_entry
        else:
            slippage = o.price_at_entry - o.average_price
        slippages.append(slippage/o.price_at_entry)
        
    
    dt = pd.to_datetime(dt, utc=True)
    dt = dt.tz_convert(tz)
    df = pd.DataFrame({'amount':amount, 
                       'price':price, 
                       'symbol':symbol,
                       'slippage_pct':slippages,
                       'remark':remark},
                      index = dt)
    df = df[df.amount != 0]
    df.index.name = 'timestamp'
    return df
    
def create_positions_frame(positions:dict, max_txns:int=50000, chunk_size:int=5000) -> pd.DataFrame:
    """ 
        Create a dataframe of positions from dated positions dict.
    """
    pos = MaxSizedOrderedDict({}, max_size=max_txns, chunk_size=chunk_size)
    for dt in positions:
        pos_dict = positions[dt]
        data = {symbol:pos_dict[symbol]['quantity'] for symbol in pos_dict}
        pos[dt] = data
        
    df = pd.DataFrame(pos).T
    df.index.name = 'date'
    return df
    
def create_round_trips_frame(round_trips:list|deque, portfolio_value:pd.Series|None=None, 
                             max_txns:int=50000, chunk_size:int=5000) -> pd.DataFrame:
    """ 
        Create a dataframe of round trips.
    """
    entry = [pd.Timestamp(t['entry_time']) for t in round_trips]
    exit_ = [pd.Timestamp(t['timestamp']) for t in round_trips]
    symbol = [t['asset'] for t in round_trips]
    amount = [t['buy_quantity'] for t in round_trips]
    buy = [t['buy_price'] for t in round_trips]
    sell = [t['sell_price'] for t in round_trips]
    long = [True if t['position_side'] in (
            PositionSide.LONG,'LONG','long') else False \
            for t in round_trips]
    
    df = pd.DataFrame({'open_dt':entry, 'close_dt':exit_, 'symbol':symbol,
                       'amount':amount,'buy':buy,'sell':sell, 'long':long})
    df.index.name = 'trade'
    
    df['pnl'] = df.amount*(df.sell - df.buy)
    df['rt_returns'] = df.pnl/(df.amount*df.buy)
    df['duration'] = pd.to_datetime(df.close_dt, utc=True) - \
                        pd.to_datetime(df.open_dt, utc=True)
    
    if portfolio_value is not None:
        pv = pd.DataFrame(portfolio_value.values, 
                          index=portfolio_value.index)
        pv.columns = ['portfolio_value']
        if isinstance(pv.index, pd.DatetimeIndex):
            pv.index = pv.index.tz_convert('UTC').normalize()
        
        df['date'] = pd.to_datetime(
                df.open_dt.values, utc=True).normalize()
        df = df.set_index('date')
        
        tmp = df.join(pv).reset_index()
        df = df.reset_index().drop('date', axis=1)
        df['returns'] = tmp.pnl / tmp.portfolio_value
    
    return df
    