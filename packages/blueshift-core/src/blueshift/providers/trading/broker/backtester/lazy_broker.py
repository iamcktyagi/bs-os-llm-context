from __future__ import annotations
from typing import TYPE_CHECKING

import logging
from collections import deque
import datetime
import os
import math
import json


from blueshift.calendar import get_calendar, register_calendar, TradingCalendar
from blueshift.lib.trades._order_types import (
        OrderValidity, OrderFlag, OrderType, ProductType, OrderSide)
from blueshift.lib.trades._trade import Trade
from blueshift.lib.trades._order import Order
from blueshift.lib.trades._position import Position
from blueshift.lib.trades._accounts import BlotterAccount
from blueshift.lib.common.enums import (
        ExecutionMode, BlotterType, BrokerType, AlgoMode, AlgoCallBack)
from blueshift.lib.exceptions import (
        BrokerError, InitializationError, ExceptionHandling, 
        InsufficientFund, OrderAlreadyProcessed, DataWriteException, DataReadException, 
        ValidationError)
from blueshift.lib.common.types import MaxSizedOrderedDict, MaxSizedList
from blueshift.lib.common.constants import Frequency, CCY, ALMOST_ZERO, Currency

from blueshift.interfaces.assets._assets import Asset, Equity, Forex, InstrumentType
from blueshift.interfaces.assets.assets import asset_factory
from blueshift.interfaces.data.store import DataStore
from blueshift.interfaces.trading._simulation import (
    ABCChargesModel, ABCCostModel, ABCMarginModel, ABCSlippageModel)
from blueshift.interfaces.trading.broker import IBroker, register_broker, AccountType
from blueshift.interfaces.trading.simulations import (
        charge_model_factory, cost_model_factory,
        margin_model_factory, slippage_model_factory)

from blueshift.finance.commission import PerDollar
from blueshift.finance.slippage._slippage import CryptoSlippage # type: ignore
from blueshift.finance.margin import FlatMargin

from blueshift.providers.data.library import Library
from blueshift.lib.serialize.serialize import (
        save_portfolio_to_disk, save_orderbook_to_disk, 
        save_blotter_account_to_disk, save_roundtrips_to_disk,
        save_transactions_to_disk, read_portfolio_from_disk, 
        read_orderbook_from_disk, read_roundtrips_from_disk, 
        read_blotter_account_from_disk, read_transactions_from_disk)
from blueshift.finance import *

from blueshift.lib.common.constants import MAX_COLLECTION_SIZE

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd

class LazyBacktestBroker(IBroker):
    '''
        An implementation of the broker interface for backtest.
    '''
    
    def __init__(self, initial_capital:float|None, frequency:Frequency|str, library:Library|str, 
                 calendar:TradingCalendar|str|None, ccy:Currency|None=None, 
                 intraday_cutoff:tuple|None=None, logger:logging.Logger|None=None, 
                 charge_model:ABCChargesModel|str='no-charge', cost_model:ABCCostModel|str='no-cost', 
                 margin_model:ABCMarginModel|str='flat', slippage_model:ABCSlippageModel|str='volume', 
                 active_store:DataStore|None=None, pipeline_store:DataStore|None=None,
                 fx_rate_store:DataStore|None=None, *args, **kwargs):
        """
            An implementation of the broker interface for backtest. The 
            broker object takes in a library object and queries it 
            based on the frequency (using the asset-frequency dispatch 
            built-in in the library implementation). It also takes in 
            three models - cost model for simulating brokerage and other 
            charges, maring model for simulating margins and slippage 
            model for simulating order fills and slippage.
            
            Args:
                `initial_capital (float)`: Initial capital.
                
                `frequency (str)`: Data frequency (1m or 1d).
                
                `library (obj)`: The library object.
                
                `calendar (obj)`: The calendar object.
                
                `ccy (obj or str)`: The account currency.
                
                `intraday_cutoff (tuple)`: A tuple of (h,m,s).
                
                `charge_model (obj or str)`: The charge model.
                
                `cost_model (obj or str)`: The cost model.
                
                `margin_model (obj or str)`: The margin model.
                
                `slippage_model (obj or str)`: The slippage model.
                
                `active_store (str)`: Optionally specify active store.
                
                `pipeline_store (str)`: Optionally specify pipeline store.
                
                `fx_rate_store (str)`: The fx rate store (for fx conversion).
        """
        self._name = 'backtester'
        IBroker.__init__(self, self.name)
        
        self._type = BrokerType.BACKTESTER
        self._modes_supported = [AlgoMode.BACKTEST]
        self._exec_modes_supported = [ExecutionMode.AUTO]
        self._blotter_type = BlotterType.EXCLUSIVE
        self._mode = AlgoMode.BACKTEST
        self._streaming_update = False
        self._account_type = AccountType.BACKTEST
        
        self._is_paper = kwargs.pop('is_paper', False)
        
        self.set_currency(ccy)
        
        # library must have a daily data store if var margin models
        # or volatility dependent slippage models are used.
        if isinstance(library, str):
            # assume a pathname to the library location
            library = os.path.expanduser(library)
            if os.path.exists(library) and os.path.isdir(library):
                library = Library(
                        library, active_store=active_store,
                        pipeline_store=pipeline_store,
                        fx_rate_store=fx_rate_store)
        if not isinstance(library, Library):
            raise InitializationError(f'not a valid library.')
        self._library = library
        self._library.set_cache_policy(policy='sliding')
        
        try:
            self._freq = Frequency(frequency)
        except Exception:
            InitializationError('not a valid frequency.')
        
        self._adj_handler = None
        self._store_last_date = self._library.active.metadata['end_date']
        self._dataset_end_date = self._store_last_date.tz_localize(
                None).normalize()
        
        try:
            equity_store = self._library[Equity, self._freq]
            self._adj_handler = equity_store.adjustments_handler
        except KeyError:
            pass
        
        if initial_capital is not None:
            self._initial_capital = float(initial_capital)
        else:
            self._initial_capital = initial_capital
        
        if calendar is None:
            calendar = self._library.active.calendar
        elif isinstance(calendar, str):
            calendar = get_calendar(calendar)
        if not isinstance(calendar, TradingCalendar):
            raise InitializationError('not a valid calendar.')
        
        self._calendar = calendar
        register_calendar(calendar.name, calendar)
        
        if logger:
            self._logger = logger
        else:
            self._logger = logging.getLogger('BACKTESTER')
        
        self._tid = 0
        self._max_reject = 10
        self._account_frozen = False
        self._account_frozen_reason = ''
        self._intraday_cutoff = None
        self._intraday_cutoff_dt = None
        self._intraday_reject = False
        
        if intraday_cutoff:
            roll_day = kwargs.get('roll_day',0)
            roll_time = intraday_cutoff
            self.set_roll_policy(roll_day, roll_time)
        
        self.setup_model('cost', cost_model, charge_model=charge_model,
                          *args, **kwargs)
        self.setup_model('slippage', slippage_model, *args, **kwargs)
        self.setup_model('margin', margin_model, *args, **kwargs)
        
        # trackers for simulation
        self._reset_orders_trackers()
        self._reset_positions()
        self._day_pnl = 0
        self._last_day_pnl = 0
        self._round_trips_realized = 0
        starting_cash = initial_capital or 1
        self._account = BlotterAccount(
                    name=self._name, cash=starting_cash, 
                    starting_cash=starting_cash, currency=self._ccy)
        self._transactions = MaxSizedOrderedDict(
                max_size=MAX_COLLECTION_SIZE, chunk_size=500)
        self._round_trips = deque([], maxlen=MAX_COLLECTION_SIZE)
        self._slippage_report = {}
        
        self._timestamp:pd.Timestamp|None = None
        self._current_dt = None
        self._current_dt_normalized = None
        
        # the realized pnl calculation is approximation in position.pyx
        # we need to add back realized pnls for unfunded trades to cashflow
        # as they are shown with 0 cashflow. For funded trades the unwinding
        # cashflow automatically captures it.
        self._roundtrips_unfunded_adj = 0
        self._unfunded_adj = 0
        self._last_unfunded_adj = 0
        
        # tracker to roll assets if applicable
        self._assets_roll_tracker = {}
        # tracker for rolling asset order fills
        self._orders_roll_tracker = {}
        
        # track benchmark membership
        self._membership = self.store.fetch_membership()
        if self._membership is None:
            self._membership = pd.DataFrame(
                    columns=['symbol','start_date','end_date'])
        self._todays_exits = []
        
        # update accounts and positions lazily
        self._last_lazy_update = None
        self._can_update_positions = False
        self._last_bts_timestamp:pd.Timestamp = pd.NaT # type: ignore
        
        # track how much asset traded from the slippage model at the
        # current time-stamp. This is required if there are multiple 
        # orders for the same asset at the same cycle.
        self._asset_traded = {}
        
        if not self._cost_model or not self._margin_model or \
            not self._simulator:
            msg = f'Model initialization failed for backtester.'
            raise InitializationError(msg)
            
        # set the event flags (e.g. AlgoCallback)
        self._event_flags = {}
        
        # order warning tracker
        self._order_warning = set()
        
    def _reset_trade_trackers(self):
        self._cash_outflow = 0
        self._cash_change = 0
        self._margin_change = 0
        self._commission_change = 0
        self._charges_change = 0
        
        self._skip_positions_updates = set()
        self._acct_needs_update = False
        
    def _reset_orders_trackers(self, open_orders:dict[str, Order]|None = None):
        if not open_orders:
            open_orders = {}
            
        self._reject_order_count = 0
        self._reset_trade_trackers()
        self._open_orders = open_orders
        self._trades_by_orders = {}
        self._closed_orders:dict[str,Order] = MaxSizedOrderedDict(
                max_size=MAX_COLLECTION_SIZE, chunk_size=500)
        
        self._open_orders_by_assets:dict[Asset,set[Order]] = {}
        
        if self._open_orders:
            for oid in self._open_orders:
                order = self._open_orders[oid]
                asset = self._get_asset_from_order(order)
                if asset in self._open_orders_by_assets:
                    self._open_orders_by_assets[asset].add(order)
                else:
                    self._open_orders_by_assets[asset] = set([order])
    
    def _reset_positions(self, open_positions:dict[Asset,Position]|None=None, 
                         intraday_positions:dict[Asset,Position]|None=None):
        if not open_positions:
            open_positions = {}
        if not intraday_positions:
            intraday_positions = {}
            
        self._open_positions = open_positions
        self._open_intraday_positions = intraday_positions
        self._closed_positions = MaxSizedList(
                max_size=MAX_COLLECTION_SIZE, chunk_size=500)
        
    def reset(self, open_orders:dict[str,Order]|None = None, open_positions:dict[Asset,Position]|None=None, 
              intraday_positions:dict[Asset,Position]|None=None, *args, **kwargs):
        starting_cash = self._initial_capital or 1
        self._account = BlotterAccount(
                    name=self._name, cash=starting_cash, 
                    starting_cash=starting_cash, currency=self._ccy)
        self._reset_orders_trackers(open_orders)
        self._reset_positions(open_positions, intraday_positions)
        if open_positions or intraday_positions:
            self._acct_needs_update = True
            self._update_position_account()
            
        self.library.reset()
        
    def save(self, run_path, *args, **kwargs):
        path = os.path.join(run_path, 'open_positions.json')
        save_portfolio_to_disk(self._open_positions, path)
            
        path = os.path.join(run_path, 'open_intraday_positions.json')
        save_portfolio_to_disk(self._open_intraday_positions, path)
        
        if self._open_orders:
            path = os.path.join(run_path, 'open_orders.json')
            save_orderbook_to_disk(self._open_orders, path)
            
        if self._round_trips:
            path = os.path.join(run_path, 'roundtrips.json')
            save_roundtrips_to_disk(self._round_trips, path)
            
        if self._transactions:
            path = os.path.join(run_path, 'transactions.json')
            save_transactions_to_disk(self._transactions, path)
            
        path = os.path.join(run_path, 'account.json')
        save_blotter_account_to_disk(self._account, path)
        
        if self._orders_roll_tracker:
            out = {}
            for oid in self._orders_roll_tracker:
                out[oid] = self._orders_roll_tracker[oid].to_dict()
            
            try:
                path = os.path.join(run_path, 'orders_roll_tracker.json')
                with open(path, 'w') as fp:
                    json.dump(out, fp)
            except Exception as e:
                msg = f'Failed to persist orders roll tracker:{str(e)}.'
                raise DataWriteException(msg)
                
        if self._assets_roll_tracker:
            out = []
            for asset in self._assets_roll_tracker:
                current_asset, dt = self._assets_roll_tracker[asset]
                data = (asset.to_dict(), current_asset.to_dict(), dt)
                out.append(data)
            
            try:
                path = os.path.join(run_path, 'assets_roll_tracker.json')
                with open(path, 'w') as fp:
                    json.dump(out, fp)
            except Exception as e:
                msg = f'Failed to persist assets roll tracker:{str(e)}.'
                raise DataWriteException(msg)
        
        bt = {}
        bt['round_trips_realized'] = self._round_trips_realized
        bt['roundtrips_unfunded_adj'] = self._roundtrips_unfunded_adj
        bt['unfunded_adj'] = self._unfunded_adj
        bt['last_unfunded_adj'] = self._last_unfunded_adj
        
        try:
            path = os.path.join(run_path, 'trackers.json')
            with open(path, 'w') as fp:
                json.dump(bt, fp)
        except Exception as e:
            msg = f'Failed to persist simulator trackers data:{str(e)}.'
            raise DataWriteException(msg)
            
    def read(self, run_path, *args, **kwargs):
        open_positions = {}
        intraday_positions = {}
        
        path = os.path.join(run_path, 'open_positions.json')
        if os.path.exists(path):
            open_positions = read_portfolio_from_disk(path, asset_finder=self)
        path = os.path.join(run_path, 'open_intraday_positions.json')
        if os.path.exists(path):
            intraday_positions = read_portfolio_from_disk(path, asset_finder=self)
        self._reset_positions(open_positions, intraday_positions)
        
        path = os.path.join(run_path, 'open_orders.json')
        if os.path.exists(path):
            open_orders = read_orderbook_from_disk(path, asset_finder=self)
            self._reset_orders_trackers(open_orders)
            
        path = os.path.join(run_path, 'roundtrips.json')
        if os.path.exists(path):
            round_trips = read_roundtrips_from_disk(path)
            if not isinstance(round_trips, list):
                round_trips = [round_trips]
            self._round_trips = deque(round_trips, maxlen=MAX_COLLECTION_SIZE)
            
        path = os.path.join(run_path, 'transactions.json')
        if os.path.exists(path):
            transactions = read_transactions_from_disk(path, asset_finder=self)
            self._transactions = MaxSizedOrderedDict(transactions,
                max_size=MAX_COLLECTION_SIZE, chunk_size=500)
            
        path = os.path.join(run_path, 'account.json')
        if os.path.exists(path):
            self._account = read_blotter_account_from_disk(path)
            
        path = os.path.join(run_path, 'orders_roll_tracker.json')
        if os.path.exists(path):
            orders_roll_tracker = {}
            try:
                with open(path) as fp:
                    data = json.load(fp)
                for oid, item in data.items():
                    asset = asset_factory(**item)
                    orders_roll_tracker[oid] = asset
            except Exception as e:
                msg = f'Failed to read orders roll tracker data:{str(e)}.'
                raise DataReadException(msg)
            else:
                self._orders_roll_tracker = orders_roll_tracker
                
        path = os.path.join(run_path, 'assets_roll_tracker.json')
        if os.path.exists(path):
            assets_roll_tracker = {}
            try:
                with open(path) as fp:
                    data = json.load(fp)
                for item in data:
                    asset = asset_factory(**item[0])
                    current_asset = asset_factory(**item[1])
                    roll_dt = pd.Timestamp(item[2])
                    assets_roll_tracker[asset] = (current_asset, roll_dt)
            except Exception as e:
                msg = f'Failed to read assets roll tracker data:{str(e)}.'
                raise DataReadException(msg)
            else:
                self._assets_roll_tracker = assets_roll_tracker
            
        path = os.path.join(run_path, 'trackers.json')
        if os.path.exists(path):
            props = {}
            try:
                with open(path) as fp:
                    props = json.load(fp)
            except Exception as e:
                msg = f'Failed to read simulator trackers data:{str(e)}.'
                raise DataReadException(msg)
            else:
                self._round_trips_realized = props.get('round_trips_realized', 0)
                self._roundtrips_unfunded_adj = props.get('roundtrips_unfunded_adj', 0)
                self._unfunded_adj = props.get('unfunded_adj', 0)
                self._last_unfunded_adj = props.get('last_unfunded_adj', 0)
        
    @property
    def cost_model(self):
        return self._cost_model
    
    @property
    def margin_model(self):
        return self._margin_model
    
    @property
    def simulator(self):
        return self._simulator
    
    @property
    def events(self):
        return self._event_flags
    
    @property
    def assets(self):
        return {}
    
    @property
    def intraday_cutoff(self):
        return self._intraday_cutoff_dt
        
    def setup_model(self, model_type, model, *args, **kwargs):
        if model_type == 'margin':
            if isinstance(model, ABCMarginModel):
                self._margin_model = model
            elif isinstance(model, str):
                margin_model = margin_model_factory(
                        model, library =self._library, *args, **kwargs)
                self._margin_model = margin_model
            if not isinstance(self._margin_model, ABCMarginModel):
                raise InitializationError('not a valid margin model.')
            self._margin_model.set_library(self._library)
            self._supported_products = self._margin_model.supported_products()
        elif model_type == 'slippage':
            if isinstance(model, ABCSlippageModel):
                self._simulator = model
            elif isinstance(model, str):
                slippage_model = slippage_model_factory(
                        model, frequency=self._freq, 
                        library =self._library, *args, **kwargs)
                self._simulator = slippage_model
            if not isinstance(self._simulator, ABCSlippageModel):
                raise InitializationError('not a valid slippage model.')
            self._simulator.set_library(self._library)
            self._simulator.set_frequency(self._freq)
        elif model_type == 'cost':
            if 'charge_model' in kwargs and kwargs['charge_model'] is not None:
                charge_model = kwargs.pop('charge_model')
            else:
                charge_model = 'no_charge'
            if isinstance(charge_model, str):
                charge_model = charge_model_factory(
                        charge_model, *args, **kwargs)
            if not isinstance(charge_model, ABCChargesModel):
                raise InitializationError('illegal type for charge model.')
                
            if isinstance(model, ABCCostModel):
                self._cost_model = model
            elif isinstance(model, str):
                cost_model = cost_model_factory(
                        model, charge_model=charge_model, 
                        *args, **kwargs)
                self._cost_model = cost_model
            if not isinstance(self._cost_model, ABCCostModel):
                raise InitializationError('not a valid cost model.')
                
    def set_currency(self, ccy=None):
        if ccy:
            if isinstance(ccy, str):
                try:
                    ccy = CCY[ccy.upper()] # type: ignore
                    self._ccy = ccy
                except Exception:
                    raise InitializationError(
                            f'not a valid currency {ccy}.')
            else:
                self._ccy = ccy
        else:
            self._ccy = CCY.LOCAL # type: ignore
        if not isinstance(self._ccy, CCY): # type: ignore
            raise InitializationError(
                    f'not a valid currency {ccy}.')
    
    def login(self, *args, **kwargs):
        if 'timestamp' in kwargs:
            self._timestamp = kwargs['timestamp']
    
    def logout(self, *args, **kwargs):
        if 'timestamp' in kwargs:
            self._timestamp = kwargs['timestamp']
    
    def trading_bar(self, timestamp):
        self._timestamp = timestamp
        if self._account_frozen:
            msg = f'account frozen for trading:{self._account_frozen_reason}'
            raise BrokerError(msg, handling=ExceptionHandling.TERMINATE)
        
        if self._intraday_cutoff is not None:
            elapsed_second = timestamp.hour*3600 + timestamp.minute*60 + timestamp.second
            if not self._intraday_reject and \
                elapsed_second > self._intraday_cutoff:
                self._lazy_update(timestamp)
                self._run_intraday_cutoff(timestamp)
                self._roll_assets(timestamp)
        
        #self._event_flags = {}
        self._asset_traded = {}
        traded = self._execute_orders(timestamp)
        self._update_position_account(forced=True)
        self._asset_traded = {}
        
        if traded:
            self._event_flags[AlgoCallBack.TRADE] = True
            
    def backtest_simulate(self, timestamp):
        return self.paper_simulate(timestamp)
            
    def paper_simulate(self, timestamp):
        """ this is similar to trading_bar, except we update position
            only if required. This must be implemented for a paper broker.
        """
        if not self._open_orders:
            return
        
        self._timestamp = timestamp
        if self._account_frozen:
            msg = f'account frozen for trading:{self._account_frozen_reason}'
            raise BrokerError(msg, handling=ExceptionHandling.TERMINATE)
        
        if self._intraday_cutoff is not None:
            elapsed_second = timestamp.hour*3600 + timestamp.minute*60 + timestamp.second
            if not self._intraday_reject and \
                elapsed_second > self._intraday_cutoff:
                self._lazy_update(timestamp)
                self._run_intraday_cutoff(timestamp)
                self._roll_assets(timestamp)
        
        #self._event_flags = {}
        self._asset_traded = {}
        traded = self._execute_orders(timestamp)
        
        if traded:
            self._update_position_account(forced=True)
        self._asset_traded = {}
        
        if traded:
            self._event_flags[AlgoCallBack.TRADE] = True
            
    def paper_reconcile(self, timestamp):
        """ this is similar to trading_bar, except we update position
            only if required. This must be implemented for a paper broker.
        """
        self._timestamp = timestamp
        self._update_position_account(forced=True)

    def before_trading_start(self, timestamp):
        self._last_bts_timestamp = timestamp
        self._timestamp = timestamp
        self._last_day_pnl = self._account.pnls
        self._day_pnl = 0
        self._intraday_reject = False
        self._order_warning = set()
        
        current_dt = self._timestamp.normalize() # type: ignore
        if self._current_dt and self._current_dt == current_dt:
            # in case of 24X7 clock where open time > close time
            self._current_dt = current_dt + pd.Timedelta(days=1)
        else:
            self._current_dt = current_dt
            
        self._current_dt_normalized = self._current_dt.tz_localize(None)
        
        # compute today's exits list
        if self._current_dt_normalized == self._dataset_end_date:
            # dataset last date is not exit really
            exits =[]
        else:
            exits = self._membership[self._membership.end_date \
                                         == self._current_dt_normalized]
            exits = exits.symbol.tolist()
        self._todays_exits = exits
        
        # scan open order list for possible cancellations
        self._sod_order_cancel()
        
        # set position update flag
        self._can_update_positions = True

    def after_trading_hours(self, timestamp):
        self._lazy_update(self._timestamp)
            
        self._eod_order_cancel()
        
        # check and auto-exit expired assets
        self._close_expired_assets()
        
        # update positions and account for corporate action next day.
        self._apply_corporate_actions()
        
        # convert open intraday positions
        # we ignore margin_block as we calculate overnight 
        # margin requirements on existing positions after
        # the intraday conversions later anyway
        self._unwind_intraday_positions()
        
        # calculate MTM cash settlement amount and the overnight 
        # maring requirements for open positions
        mtm, overnight_margin = self._eod_positions_settlements()
        mtm = 0
        
        # settle roll costs and margins for funded products 
        # like currency positions or shorts (borrow costs)
        rollcosts, roll_margin = self._cost_model.rollcost(
                self._open_positions)
        
        # total cash change is cash release in intraday conversion
        # plus MTM settlements for open positions less roll costs.
        cash_change = mtm - rollcosts
        
        # total margin change is the overnight margin requirement
        # plust the roll maring, less current margin already 
        # bloked in the account.
        margin = overnight_margin + roll_margin
        margin_change = margin - self._account.margin
        cash_change = cash_change - margin_change
        
        if abs(cash_change) > ALMOST_ZERO or abs(margin_change) > ALMOST_ZERO:
            self._account.settle_cash(cash_change, margin_change)
        
        self._update_position_account()
        
        if self._account.net < 0:
            if self._initial_capital is None:
                amount = max(-self._account.net, -self._account.cash)
                self._account.fund_transfer(amount)
            else:
                self._account_frozen = True
                self._account_frozen_reason = 'insufficient fund.'
                msg = 'Account equity balanace negative: account frozen.'
                msg += ' will exit backtest.'
                msg += ' Try running the simulation with a lower leverage.'
                raise InsufficientFund(msg,handling=ExceptionHandling.TERMINATE)
        
        self._open_intraday_positions = {}
        self._timestamp = timestamp

    def algo_start(self, timestamp):
        self._timestamp = timestamp

    def algo_end(self, timestamp):
        self._timestamp = timestamp

    def heart_beat(self, timestamp):
        self._timestamp = timestamp
    
    ######## ASSET FINDER INTERFACE METHODS ##########################
    def symbol(self, symbol, dt=None, *args, **kwargs):
        return self._library.symbol(symbol, dt=dt, *args, **kwargs)
        
    def symbol_to_asset(self, symbol, dt=None, *args, **kwargs):
        return self.symbol(symbol, dt=dt, *args, **kwargs)
    
    def exists(self, syms):
        """ Check if a symbol exists. """
        return self._library.exists(syms)
    
    def fetch_asset(self, sid):
        """ SID based asset search not implemented. """
        return self._library.fetch_asset(sid)
        
    def sid(self, sid):
        return self.fetch_asset(sid)
        
    def refresh_data(self, *args, **kwargs):
        pass
    
    def retrieve_asset(self, sid):
        """ Resolve and return asset given an asset object. """
        return self._library.retrieve_asset(sid)
        
    def retrieve_all(self, sids):
        """ Resolve and return assets given an iterable of asset objects. """
        return self._library.retrieve_all(sids)
        
    def lifetimes(self, dates, assets=None):
        """ lifetime matrix not implemented. """
        return self._library.lifetimes(dates, assets=assets)
        
    def can_trade(self, asset, dt):
        return self._library.can_trade(asset, dt)
    
    ######### Data fetch INTERFACE METHODS ######################
    
    def has_streaming(self) -> bool:
        """ 
            if streaming data supported. True for backtest - although
            we do not have actual streaming, this signifies quering 
            data.current is efficient.
        """
        return True
    
    def getfx(self, ccy_pair, dt:pd.Timestamp):
        if not dt:
            dt = self._timestamp
        return self._library.getfx(ccy_pair, dt)
    
    def option_chain(self, underlying, series, columns, strikes=None, 
                     relative=True, **kwargs):
        return self._library.option_chain(
                underlying, series, columns, strikes=strikes, 
                relative=relative, dt=self._timestamp)
    
    def quote(self, asset, column=None, **kwargs):
        return self._library.quote(asset, column)
    
    def get_expiries(self, asset, start_dt, end_dt, offset=None):
        return self._library.get_expiries(
                asset, start_dt, end_dt, offset=offset)
    
    def history(self, assets, columns, nbars, frequency, **kwargs):
        dt = kwargs.pop('dt', self._timestamp)
            
        return self._library.history(
                assets, columns, nbars, frequency, dt=dt, **kwargs)
    
    def current(self, assets, columns:list[str]|str='close', **kwargs):
        return self._library.current(
                assets, columns, dt=self._timestamp,
                frequency=self._freq, last_known=False)
        
    def fundamentals(self, assets, metrics, nbars, frequency, **kwargs):
        return self._library.fundamentals(
                assets, metrics, nbars, frequency, dt=self._timestamp)
    
    ########### Broker INTERFACE METHODS ##########################
    
    @property
    def account_type(self):
        return self._account_type
    
    @property
    def tz(self):
        return self._calendar.tz
    
    @property
    def calendar(self):
        return self._calendar
        
    @property
    def library(self):
        return self._library
    
    @property
    def store(self):
        return self._library.active
    
    @property
    def profile(self):
        self._lazy_update(self._timestamp)
        return self._account.to_dict()
    
    @property
    def account(self):
        self._lazy_update(self._timestamp)
        return self._account.to_dict()
    
    def get_account(self, *args, **kwargs):
        self._lazy_update(self._timestamp)
        return self._account
    
    def get_asset_from_order(self, order):
        return self._get_asset_from_order(order)

    @property
    def positions(self):
        self._lazy_update(self._timestamp)
        return self._open_positions.copy()
        
    @property
    def closed_positions(self):
        self._lazy_update(self._timestamp)
        return self._closed_positions.copy()
    
    def position_by_asset(self, asset, *args, **kwargs):
        self._lazy_update(self._timestamp)
        return self._open_positions.get(asset, None)

    @property
    def orders(self):
        return {**self._open_orders, **self._closed_orders}
    
    @property
    def open_orders(self):
        return self._open_orders.copy()
    
    @property
    def round_trips(self):
        self._lazy_update(self._timestamp)
        return self._round_trips
    
    @property
    def transactions(self):
        return self._transactions
    
    def get_order(self, order_id, *args, **kwargs):
        if order_id in self._open_orders:
            return self._open_orders[order_id]
        
        if order_id in self._closed_orders:
            return self._closed_orders[order_id]
        
    def open_orders_by_asset(self, asset, *args, **kwargs):
        orders = self._open_orders_by_assets.get(asset)
        if orders:
            return list(orders).copy()
        return []
    
    def _get_asset_from_order(self, order):
        # we cannot track options with rolling assets
        if hasattr(order.asset,'option_type'):
            return order.asset
        
        if order.oid in self._orders_roll_tracker:
            return self._orders_roll_tracker[order.oid]
        return order.asset
    
    def _check_membership(self, asset, dt=None):
        if len(self._todays_exits) ==0:
            return True
        
        return asset.symbol not in self._todays_exits
    
    def _check_rolling_asset(self, asset):
        if not asset.is_rolling():
            return asset
        
        # we do not roll options
        if asset.is_opt():
            return self.library.symbol(asset.exchange_ticker, dt=self._timestamp)
        
        # expiry handling are without tz-info
        dt = self._timestamp
        
        near_expiry = self.library.get_expiry(asset, dt=dt, offset=0) # type: ignore
        
        if not near_expiry or pd.isna(near_expiry):
            msg = f'No expiry data from library, cannot handle rolling asset {asset} for {dt}.'
            msg += ' Dated asset will be traded instead.'
            self._logger.warning(msg)
            return asset
        
        roll_dt = near_expiry - pd.Timedelta(asset.roll_day)
        current_asset = self.library.symbol(asset.exchange_ticker, dt=dt)
        
        # track the dated asset and the next roll date
        self._assets_roll_tracker[asset] = (current_asset, roll_dt)
        return current_asset
    
    def is_risk_reducing(self, order):
        if order.asset not in self._open_positions:
            return False
        
        pos = self._open_positions[order.asset]
        side = 1 if order.side == OrderSide.BUY else -1
        exposure = pos.quantity
        order_exposure = order.quantity*side
        
        if abs(exposure) > abs(exposure + order_exposure):
            return True
        
        return False
    
    def place_order(self, order:Order, *args, **kwargs):
        if order.product_type not in self.supported_products:
            raise BrokerError(
                    f'product type {order.product_type} not supported.')
            
        if order.oid in self._open_orders:
            raise BrokerError('existing order, cannot create new.')
        
        if order.quantity<=0:
            reason = f'order quantity must be positive.'
            self._reject_order(order, reason, accepted_before=False)
            raise BrokerError(reason)
            
        if order.fractional and not order.asset.fractional:
            reason = f'asset does not support fractional trading.'
            self._reject_order(order, reason, accepted_before=False)
            raise BrokerError(
                    reason, handling=ExceptionHandling.TERMINATE)
        
        if not order.fractional and order.quantity%order.asset.mult!=0:
            reason = f'order quantity must be multiple of lot size.'
            self._reject_order(order, reason, accepted_before=False)
            raise BrokerError(reason)
        
        if order.order_validity in [OrderValidity.CLS, OrderValidity.OPG]: # type: ignore
            reason = f'market on open or market on close orders not supported.'
            self._reject_order(order, reason, accepted_before=False)
            raise BrokerError(
                    reason, handling=ExceptionHandling.TERMINATE)
            
        if order.order_flag == OrderFlag.AMO:
            reason = f'advance market order not supported.' 
            self._reject_order(order, reason, accepted_before=False)
            raise BrokerError(
                    reason, handling=ExceptionHandling.TERMINATE)
            
        if order.order_type in [
            OrderType.STOPLOSS_MARKET,OrderType.STOPLOSS] and order.trigger_price==0: # type: ignore
            reason = f'trigger price missing for stop order.'
            self._reject_order(order, reason, accepted_before=False)
            raise BrokerError(reason)
            
        if order.order_flag == OrderType.STOPLOSS and order.stoploss_price==0: # type: ignore
            reason = f'stoploss limit price missing for stop-limit order.'
            self._reject_order(order, reason, accepted_before=False)
            raise BrokerError(reason)
            
        if not self._intraday_cutoff and order.product_type == ProductType.INTRADAY:
            reason = f'intraday order type not supported.'
            self._reject_order(order, reason, accepted_before=False)
            raise BrokerError(
                    reason, handling=ExceptionHandling.TERMINATE)
            
        if self._intraday_reject:
            if order.product_type == ProductType.INTRADAY:
                reason = f'intraday orders not accpeted after cut-off time {self._intraday_cutoff_time}.'
                self._reject_order(order, reason, accepted_before=False)
                raise BrokerError(reason)
            
            if order.asset.is_rolling() and not self.is_risk_reducing(order):
                reason = f'Rolling asset orders not accepted after cut-off time {self._intraday_cutoff_time}.'
                self._reject_order(order, reason, accepted_before=False)
                raise BrokerError(reason)
            
        if not self.library.is_alive(order.asset, self._timestamp): # type: ignore
            reason = f'the asset {order.asset} has stopped trading.'
            self._reject_order(
                    order, reason, accepted_before=False, log=self._is_paper,
                    update_transaction=False)
            # silently fail for expired asset
            return
        
        if not self._check_membership(order.asset):
            reason = f'the asset {order.asset} no longer in benchmark.'
            self._reject_order(
                    order, reason, accepted_before=False, log=self._is_paper,
                    update_transaction=False)
            # silently fail if asset in not in the universe
            return
        
        order = self.rms.pretrade(order)
        
        try:
            current_asset = self._check_rolling_asset(order.asset)
        except Exception as e:
            reason = f'Error getting dated asset for {order.asset} or it '
            reason += f'does not exists:{str(e)}.'
            self._reject_order(
                    order, reason, accepted_before=False, log=self._is_paper,
                    update_transaction=False)
            # silently fail if we fail to get the dated asset
            # this maybe because of missing data!
            return
            
        if current_asset.exchange_ticker != order.asset.exchange_ticker:
            # we have a rolling asset here, update the order 
            # for the dated asset for the current roll period
            self._orders_roll_tracker[order.oid] = order.asset
            order.update_from_dict({"asset":current_asset})
            
        self._open_orders[order.oid] = order
        
        asset = self._get_asset_from_order(order)
        if asset in self._open_orders_by_assets:
            self._open_orders_by_assets[asset].add(order)
        else:
            self._open_orders_by_assets[asset] = set([order])
        
        return order.oid

    def update_order(self, order, *args, **kwargs):
        if isinstance(order, str):
            order = self._open_orders.get(order, None)
            if not order:
                raise OrderAlreadyProcessed('order not found, cannot update.')
                
        if order.oid not in self._open_orders:
            raise OrderAlreadyProcessed('order not found, cannot update.')
            
        if 'quantity' in kwargs:
            quantity = kwargs['quantity']
            if not order.fractional and quantity%order.asset.mult!=0:
                reason = f'order quantity must be multiple of lot size.'
                raise BrokerError(
                        reason, handling=ExceptionHandling.TERMINATE)
                
        if 'trigger_price' in kwargs:
            trigger_price = kwargs['trigger_price']
            if trigger_price == 0 and order.order_type in [
                    OrderType.STOPLOSS_MARKET,OrderType.STOPLOSS]: # type: ignore
                reason = f'trigger price missing for stop order.'
                raise BrokerError(reason)
                
        if 'stoploss_price' in kwargs:
            stoploss_price = kwargs['stoploss_price']
            if stoploss_price == 0 and order.order_type == OrderType.STOPLOSS: # type: ignore
                reason = f'stoploss price missing for stop-limit order.'
                raise BrokerError(reason)
            
        self._open_orders[order.oid].user_update(kwargs) # type: ignore
        order.set_timestamp(self._timestamp) # type: ignore
        return order.oid

    def cancel_order(self, order, *args, **kwargs):
        if isinstance(order, str):
            order = self._open_orders.get(order, None)
            if not order:
                raise OrderAlreadyProcessed(
                        'order not found or completed already, cannot cancel.')
            
        if order.oid not in self._open_orders:
            raise OrderAlreadyProcessed(
                    'order not found or completed already, cannot cancel.')
        
        if order.pending < order.asset.mult:
            raise BrokerError('order completed already, cannot cancel.')
            
        self._cancel_order(order, 'Cancelled by user')
        return order.oid
    
    def _pop_position(self, asset, pos, intraday=False):
        if intraday:
            self._open_intraday_positions.pop(asset, None)
        else:
            self._open_positions.pop(asset, None)
            
        self._round_trips.append(pos.to_json())
        self._round_trips_realized += pos.realized_pnl
        
        if not pos.is_funded() and not \
            pos.asset.instrument_type==InstrumentType.OPT:
            self._roundtrips_unfunded_adj += pos.realized_pnl
            
    def _update_position_account(self, forced=False):
        if not self._acct_needs_update:
            return
        
        net_exposure = gross_exposure = realized_pnl = 0
        unrealized_pnls = cost_basis = holdings = 0
        longs_count = long_exposure = 0
        shorts_count = short_exposure = 0
        unfunded_adj = 0
        
        open_positions = list(self._open_positions.keys())
        for asset in open_positions:
            pos = self._open_positions[asset]
            
            if pos.if_closed():
                self._pop_position(asset, pos)
                continue
            
            if forced and self._can_update_positions:
                last_price = self.library.get_spot_value(
                        pos.asset, self._timestamp, 'close', self._freq) # type: ignore
                last_fx = pos.asset.fx_rate(self.ccy, self, self._timestamp) # type: ignore
                
                underlying_px = 0
                if pos.asset.instrument_type == InstrumentType.OPT:
                    underlying_px = self.library.get_spot_value(
                            pos.asset, self._timestamp, 'atmf', self._freq) # type: ignore
                
                if not math.isnan(last_price) and not math.isnan(last_fx) \
                    and not math.isnan(underlying_px):
                    # update if we have valid prices
                    pos.update_price(last_price, last_fx, underlying_px) # type: ignore
            
            exp = pos.get_exposure()
            net_exposure += exp
            gross_exposure += abs(exp)
            realized_pnl += pos.realized_pnl
            unrealized_pnls += pos.unrealized_pnl
            cost_basis += pos.cost_basis
            holdings += pos.value
            
            if not pos.is_funded() and not \
                pos.asset.instrument_type==InstrumentType.OPT:
                unfunded_adj += pos.realized_pnl
            
            if pos.quantity > 0:
                longs_count += 1
                long_exposure += abs(exp)
            else:
                shorts_count += 1
                short_exposure += abs(exp)
                
        total_realized = self._round_trips_realized + realized_pnl
        total_pnl = total_realized + unrealized_pnls
        self._day_pnl = total_pnl - self._last_day_pnl
        self._unfunded_adj = self._roundtrips_unfunded_adj + unfunded_adj
        
        self._cash_change = self._cash_change - \
                        (self._unfunded_adj - self._last_unfunded_adj)
        
        self._account.blotter_update( # type: ignore
                cash=-self._cash_change, # cash change is added
                margin=self._margin_change,
                commissions=self._commission_change,
                charges=self._charges_change,
                realized_pnl=total_realized,
                unrealized_pnls=unrealized_pnls,
                gross_exposure=gross_exposure,
                net_exposure=net_exposure,
                holdings=holdings,
                cost_basis=cost_basis,
                long_exposure=long_exposure,
                short_exposure=short_exposure,
                longs_count=longs_count,
                shorts_count=shorts_count,
                adj=0, day_pnl=self._day_pnl)
        
        self._acct_needs_update = False
        self._last_unfunded_adj = self._unfunded_adj
        self._reset_trade_trackers()
        
    def _lazy_update(self, timestamp:pd.Timestamp|None):
        if self._last_lazy_update and timestamp and \
            self._last_lazy_update >= timestamp:
            return
        
        if timestamp is not None and timestamp <= self._last_bts_timestamp:
            # avoid update before market open -> else corporate actioned positions will go wild
            return
        
        self._acct_needs_update = True
        self._update_position_account(forced=True)
        self._last_lazy_update = timestamp
    
    def _execute_orders(self, timestamp):
        traded = False
        open_orders = list(self._open_orders.keys())
        
        for oid in open_orders:
            order = self._open_orders[oid]
            if order.is_open():
                status, trades = self._simulate_fill(order, timestamp)
                
                if not status:
                    # we got no fill, kill the order if FOK
                    if order.order_validity in (OrderValidity.FOK, OrderValidity.IOC):
                        self._cancel_order(order, 'IOC or FOK order not filled.')
                        msg = f'order {order.oid} for {order.pending} units of {order.asset}'
                        msg = msg + f' could not be filled and was cancelled.'
                        self._logger.warning(
                                msg, mode=self.mode, timestamp=timestamp) # type: ignore
                    continue
                
                for trade in trades:
                    if self._pre_trade_check(trade):
                        self._fill_order(order, trade)
                        self._skip_positions_updates.add(
                                self._get_asset_from_order(order))
                        self._acct_needs_update = True
                        traded = True
                    else:
                        reason = 'insufficient fund.'
                        self._reject_order_count += 1
                        self._reject_order(
                                order, reason, accepted_before=True, log=self._is_paper)
                        break # no point looping through more trades
        return traded
                
    def _pre_trade_check(self, trade):
        # given a trade, check if we have enough cash in account to 
        # execute the trade. Also if true, update the cashflow etc. 
        # in this order fill cycle to update account at the cycle end
        outflow = trade.get_cash_outlay()
        outflow += self._cash_outflow  # the cumulative outflow this cycle
        
        if self._account.cash < outflow:
            if self._initial_capital is None:
                amount = max(outflow - self._account.cash, -self._account.cash)
                self._account.fund_transfer(amount)
            else:
                return False
        
        self._cash_outflow = outflow
        # cashflow excluding margin charge
        self._cash_change += trade.cash_flow
        self._margin_change += trade.margin
        self._commission_change += trade.commission
        self._charges_change += trade.charges
        
        return True
                
    def _simulate_fill(self, order, timestamp):
        # check if we can get a fill. This method must take care 
        # of slippage as well as order validity (e.g. FOK). Also
        # check stop triggers must be checked here
        traded, price, last, max_volume = self._simulator.simulate(
                order, timestamp)
        
        if traded == 0:
            return False, []
        
        exposure = 0
        pos = None
        asset = self._get_asset_from_order(order)
        
        if asset in self._asset_traded:
            used_limit = self._asset_traded[asset]
            traded = min(traded,max_volume - used_limit)
        
        traded = round(traded/order.asset.mult)*order.asset.mult
        if abs(traded) < ALMOST_ZERO:
            # we already traded the asset in the current cycle and 
            # left with no available amount to trade
            return False, []
        
        if order.order_validity == OrderValidity.FOK and abs(traded) < order.pending:
            return False, []
        
        # brokerage and charges for this trade
        last_fx = asset.fx_rate(self.ccy, self, timestamp) 
        if math.isnan(last_fx):
            return False, []
        
        underlying_px = 0
        if asset.instrument_type == InstrumentType.OPT:
            underlying_px = self.library.get_spot_value(
                        asset, timestamp, 'atmf', self._freq)
            if math.isnan(underlying_px):
                return False, []
        
        commission, charges = self._cost_model.calculate(
                order, traded, price, last_fx)
        
        # intraday product gets exposure based on intraday positions
        if order.product_type==ProductType.INTRADAY and \
            asset in self._open_intraday_positions:
            pos = self._open_intraday_positions[asset]
            #pos.update_price(last, last_fx, underlying_px)
            exposure = pos.quantity
        
        if asset in self._open_positions:
            pos = self._open_positions[asset]
            if order.product_type!=ProductType.INTRADAY:
                exposure = pos.quantity
                
        if pos and pos.if_closed():
            self._pop_position(asset, pos)
            pos = None
            
        # check if we have an unwind
        sign = 1 if order.side == OrderSide.BUY else -1
        amount = traded*sign
        if abs(exposure+amount) < abs(amount) and abs(exposure) < abs(amount):
            # we got an unwind and flip in sign in position
            # we create two trades, one for the full unwind, another
            # for the new exposure. This is required, else the cashflow
            # for margin products will be wrong
            traded1 = -exposure
            traded2 = amount - traded1
            if traded2 < ALMOST_ZERO:
                traded2 = 0
        else:
            traded1 = amount
            traded2 = 0
            
        if traded2:
            # cashflow and margin to block (release if negative)
            cashflow1, margin1 = self._margin_model.calculate(
                    order, abs(traded1), price, exposure, timestamp, last_fx,
                    underlying_px, pos)
            cashflow2, margin2 = self._margin_model.calculate(
                    order, abs(traded2), price, exposure, timestamp, last_fx,
                    underlying_px, None)
        else:
            # cashflow and margin to block (release if negative)
            cashflow1, margin1 = self._margin_model.calculate(
                    order, abs(traded1), price, exposure, timestamp, last_fx,
                    underlying_px, pos)
            cashflow2 = margin2 = 0
        
        self._tid += 1
        trades = []
        trade1 = Trade(self._tid, order.asset, abs(traded1), price, order.side, 
                      order.oid, broker_order_id=order.oid, 
                      exchange_order_id=order.oid, 
                      instrument_id=order.asset.sid, 
                      product_type=order.product_type, 
                      cash_flow=cashflow1, margin=margin1, 
                      commission=commission, charges=charges, 
                      exchange_timestamp=timestamp, timestamp=timestamp,
                      last_fx=last_fx, underlying_price=underlying_px)
        trades.append(trade1)
        
        if traded2:
            self._tid += 1
            trade2 = Trade(self._tid, order.asset, abs(traded2), price, order.side, 
                          order.oid, broker_order_id=order.oid, 
                          exchange_order_id=order.oid, 
                          instrument_id=order.asset.sid, 
                          product_type=order.product_type, 
                          cash_flow=cashflow2, margin=margin2, 
                          commission=commission, charges=charges, 
                          exchange_timestamp=timestamp, timestamp=timestamp,
                          last_fx=last_fx, underlying_price=underlying_px)
            trades.append(trade2)
        
        return True, trades
    
    def _fill_order(self, order, trade):
        if order.oid not in self._open_orders:
            # we should not be here
            return
        
        order.partial_execution(trade)
        order.set_exchange_timestamp(self._timestamp)
        asset = self._get_asset_from_order(order)
        
        if order.is_final():
            self._open_orders.pop(order.oid, None)
            if order.oid in self._orders_roll_tracker:
                self._orders_roll_tracker.pop(order.oid, None)
            self._closed_orders[order.oid] = order
            self._update_transactions(order)
            self._open_orders_by_assets[asset].remove(order)
            if not self._open_orders_by_assets[asset]:
                self._open_orders_by_assets.pop(asset, None)
                
            if AlgoCallBack.RECONCILIATION in self._event_flags:
                self._event_flags[AlgoCallBack.RECONCILIATION].add(order.oid)
            else:
                self._event_flags[AlgoCallBack.RECONCILIATION] = set([order.oid])
            
        if order.oid in self._trades_by_orders:
            self._trades_by_orders[order.oid].add(trade)
        else:
            self._trades_by_orders[order.oid] = set([trade])
        
        pos = None
        if asset in self._open_positions:
            pos = self._open_positions[asset]
            pos.update(trade) # type: ignore
        else:
            pos = Position.from_trade(trade) # type: ignore
            self._open_positions[asset] = pos
            
        if pos and pos.if_closed():
            self._pop_position(asset, pos)
            
        if order.product_type==ProductType.INTRADAY:
            if asset in self._open_intraday_positions:
                pos = self._open_intraday_positions[asset]
                pos.update(trade) # type: ignore
            else:
                pos = Position.from_trade(trade) # type: ignore
                self._open_intraday_positions[asset] = pos
                
        # update the volume limit used for this cycle in case 
        # there are multiple orders on the same asset
        if asset not in self._asset_traded:
            self._asset_traded[asset] = trade.quantity
        else:
            self._asset_traded[asset] += trade.quantity
            
    def _cancel_order(self, order, msg):
        self._open_orders.pop(order.oid, None)
        order.partial_cancel(msg)
        order.set_exchange_timestamp(self._timestamp)
        asset = self._get_asset_from_order(order)
        
        self._closed_orders[order.oid] = order
        self._update_transactions(order)
        self._open_orders_by_assets[asset].remove(order)
        
        if AlgoCallBack.RECONCILIATION in self._event_flags:
            self._event_flags[AlgoCallBack.RECONCILIATION].add(order.oid)
        else:
            self._event_flags[AlgoCallBack.RECONCILIATION] = set([order.oid])
        
    def _reject_order(self, order, reason, accepted_before=True, 
                      log=True, update_transaction=True):
        asset = self._get_asset_from_order(order)
        if accepted_before:
            self._open_orders.pop(order.oid, None)
            self._open_orders_by_assets[asset].remove(order)
            
            if AlgoCallBack.RECONCILIATION in self._event_flags:
                self._event_flags[AlgoCallBack.RECONCILIATION].add(order.oid)
            else:
                self._event_flags[AlgoCallBack.RECONCILIATION] = set([order.oid])
        
        order.reject(reason)
        order.set_exchange_timestamp(self._timestamp)
        
        msg = f'order {order.oid} for {order.pending} units of {order.asset}'
        msg = msg + f' was rejected:{reason}'
        if log:
            self._logger.warning(
                    msg, mode=self.mode, timestamp=self._timestamp) # type: ignore
        elif asset not in self._order_warning:
            self._order_warning.add(asset)
            self._logger.warning(
                    msg, mode=self.mode, timestamp=self._timestamp) # type: ignore
        
        if update_transaction:
            # rejected orders are not transactions, unless it was 
            # rejected after partial fill
            self._closed_orders[order.oid] = order
            if order.filled > 0:
                self._update_transactions(order)
        
    def _update_transactions(self, order):
        dt = self._current_dt
        order_list = self._transactions.get(
                dt,deque([], maxlen=MAX_COLLECTION_SIZE))
        order_list.append(order)
        self._transactions[dt] = order_list
        
    def _run_intraday_cutoff(self, timestamp):
        """ 
            stop accepting intraday order, cancel all open intraday 
            orders. Place unwinding orders for existing positions.
        """
        self._intraday_reject = True
        open_intraday_orders = [self._open_orders[oid] for oid in \
                self._open_orders if \
                self._open_orders[oid].product_type==ProductType.INTRADAY]
        for order in open_intraday_orders:
            self._cancel_order(order, 'Intraday cut-off cancel')
            
        for asset in self._open_intraday_positions:
            pos = self._open_intraday_positions[asset]
            quantity = abs(pos.quantity)
            if quantity == 0:
                continue
            side = OrderSide.BUY if pos.quantity < 0 else OrderSide.SELL
            order = Order(quantity, side, asset,  # type: ignore
                          product_type=ProductType.INTRADAY,
                          order_flag=OrderFlag.NORMAL,
                          order_type=OrderType.MARKET,
                          order_validity = OrderValidity.DAY,
                          remark='intraday-unwind')
            self._open_orders[order.oid] = order
            
            if asset in self._open_orders_by_assets:
                self._open_orders_by_assets[asset].add(order)
            else:
                self._open_orders_by_assets[asset] = set([order])
                
    def _unwind_position(self, pos, timestamp):
        """ 
            non market position unwinds - timestamped at EOD and executed 
            on the last updated price for that position. This is typically 
            used for expired assets or assets moving out of benchmark.
        """
        if pos.quantity == 0:
            return 0, 0
        
        last_fx = pos.last_fx
        price = pos.last_price
        underlying_px = pos.underlying_price
        
        if timestamp.time() > self._calendar.close_time:
            close_dt = datetime.datetime.combine(
                    timestamp.date(), self._calendar.close_time)
            close_dt = pd.Timestamp(close_dt, tz=self.tz)
        else:
            close_dt = timestamp
        
        if math.isnan(price):
            raise BrokerError(
                    'cannot unwind position for {asset.symbol}, no data.', 
                    handling=ExceptionHandling.TERMINATE)
        
        exposure = pos.get_exposure()
        quantity = pos.quantity
        
        # reverse trade to estimate cash and margin release
        order = pos.get_unwind_order(
                close_dt, product_type=self._supported_products[0],
                remark='close-out')
        cashflow, margin = self._margin_model.calculate(
            order, abs(quantity), price, exposure, timestamp, last_fx,
            underlying_px, pos)
        
        order.execute(price, order.quantity, close_dt)
        self._update_transactions(order)
        
        return -cashflow, -pos.margin
            
    def _unwind_intraday_positions(self):
        """ 
            Convert remaining intraday positions at EOD to normal positions.
            At intraday cut-off we place orders to unwind positions. In case
            anything left open, handle it here.
        """
        if not self._open_intraday_positions:
            return 0, 0
        
        total_cash = total_margin = 0
        assets = list(self._open_intraday_positions.keys())
        
        for asset in assets:
            pos = self._open_intraday_positions.pop(asset, None)
            if pos is None:
                continue
            cash, margin = self._unwind_position(pos, self._timestamp)
            total_cash += cash
            total_margin += margin
            # add the unrealized pnl as this will not go through 
            # the normal order settlement process.
            self._round_trips_realized += (pos.unrealized_pnl + \
                                            pos.realized_pnl)
            if not pos.is_funded() and not \
                pos.asset.instrument_type==InstrumentType.OPT:
                self._roundtrips_unfunded_adj += (pos.unrealized_pnl +\
                                                  pos.realized_pnl)
            self._acct_needs_update = True
            msg = f'closing out intraday position for {asset.symbol} '
            msg = msg + f'and releasing margin {margin}.'
            self._logger.warning(
                    msg, mode=self.mode, timestamp=self._timestamp) # type: ignore
        
        if abs(total_cash) > ALMOST_ZERO or abs(total_margin) > ALMOST_ZERO:
            cash_change = total_cash - total_margin
            self._account.settle_cash(cash_change, total_margin)
    
    def _eod_order_cancel(self):
        """ cancel outstanding orders at day close (unless GTC). """
        retain = {}
        
        # cancel all open orders wich are not GTC
        open_orders = list(self._open_orders.keys())
        for oid in open_orders:
            order = self._open_orders[oid]
            if order.pending < order.asset.mult:
                # fractional orders that cannot be filled, drop them
                # ideally, we should not be here!
                order.partial_cancel('EOD order cancel') # type: ignore
                self._closed_orders[order.oid] = order
                self._update_transactions(order)
            elif order.order_validity == OrderValidity.GTC: # type: ignore
                # carry over the GTC orders
                retain[oid] = order
            # else we cancel, but no need to do it explicitly as we 
            # reset the open orders and closed orders on market close
            else:
                # issue warning for unfilled orders
                msg = f'order {oid} for {order.pending} units of {order.asset}'
                msg = msg + f' could not be filled by EOD, and was cancelled.'
                self._logger.warning(
                        msg, mode=self.mode, timestamp=self._timestamp) # type: ignore
                order.partial_cancel('EOD order cancel') # type: ignore
                self._closed_orders[order.oid] = order
                self._update_transactions(order)
        
        self._reset_orders_trackers(retain)
        
    def _sod_order_cancel(self):
        """ 
            cancel overnight orders at start of the day. this is required
            in case of a restart and we loaded the last saved open orders.
        """
        retain = {}
        
        # cancel all open orders wich are not GTC
        open_orders = list(self._open_orders.keys())
        for oid in open_orders:
            order = self._open_orders[oid]
            if order.pending < order.asset.mult:
                # fractional orders that cannot be filled, drop them
                # ideally, we should not be here!
                order.partial_cancel('Overnight order cancel') # type: ignore
                self._closed_orders[order.oid] = order
                self._update_transactions(order)
            elif order.order_validity == OrderValidity.GTC: # type: ignore
                # carry over the GTC orders
                retain[oid] = order
            # else we cancel, but no need to do it explicitly as we 
            # reset the open orders and closed orders on market close
            else:
                # issue warning for unfilled orders
                msg = f'order {oid} for {order.pending} units of {order.asset}'
                msg = msg + f' is expired and will be cancelled.'
                self._logger.warning(
                        msg, mode=self.mode, timestamp=self._timestamp) # type: ignore
                order.partial_cancel('Overnight order cancel') # type: ignore
                self._closed_orders[order.oid] = order
                self._update_transactions(order)
        
        self._reset_orders_trackers(retain)
    
    def _eod_positions_settlements(self):
        """ settle overnight MTM and margins. """
        mtm = 0
        margins = 0
        
        for asset in self._open_positions:
            pos = self._open_positions[asset]
            if pos.is_funded():
                continue
            
            if pos.quantity == 0:
                # just in case intraday conversion made it zero
                continue
            
            mtm += pos.unrealized_pnl
            if asset.instrument_type != InstrumentType.OPT:
                exposure = pos.get_exposure()
            else:
                price = self.library.get_spot_value(
                        asset, self._timestamp, 'atmf', self._freq) # type: ignore
                exposure = pos.quantity*price*pos.last_fx
            margin = self._margin_model.exposure_margin( # type: ignore
                asset, exposure, pos, self._timestamp)
            
            margin = max(pos.margin, margin)
            pos.update_from_dict({'margin':margin})
            margins += margin
        
        return mtm, margins
    
    def _roll_assets(self, timestamp):
        """ check and roll assets if applicable. """
        #dt = timestamp.tz_localize(None).normalize()
        dt = timestamp.normalize()
        
        for key in self._assets_roll_tracker:
            if not key.is_rolling():
                continue
            
            current_expiry = None
            next_expiry = None
            
            asset, next_roll_dt = self._assets_roll_tracker[key]
            # something went wrong, may be the origninal order or 
            # a subsequent roll order did not get a fill
            if key not in self._open_positions:
                continue
            
            # roll date is without tz-info          
            if not current_expiry:
                current_expiry = self.library.get_expiry(key, dt=dt, offset=0)
                if not current_expiry or pd.isna(current_expiry):
                    msg = f'No expiry data from library. Cannot roll {key}.'
                    msg = f' Dated asset {asset} will expire instead.'
                    self._logger.warning(msg)
                    return
            
            if not next_expiry:
                next_expiry = self.library.get_expiry(key, dt=dt, offset=1)
                if not next_expiry or pd.isna(next_expiry):
                    msg = f'No expiry data from library. Cannot roll {key}.'
                    msg = f' Dated asset {asset} will expire instead.'
                    self._logger.warning(msg)
                    return
            
            if current_expiry < next_roll_dt or dt < next_roll_dt:
                continue
            
            next_asset = self.library.symbol(key.symbol, dt=next_expiry)
            next_roll_dt = next_expiry - pd.Timedelta(key.roll_day)
            self._assets_roll_tracker[key] = (next_asset, next_roll_dt)
            
            pos = self._open_positions[key]
            exit_side = OrderSide.BUY if pos.quantity < 0 else OrderSide.SELL
            entry_side = OrderSide.BUY if pos.quantity > 0 else OrderSide.SELL
            
            # full exit from last asset and equal size on next asset
            exit_quantity = abs(pos.quantity)
            entry_quantity = abs(pos.quantity)
            
            exit_order = Order(
                    exit_quantity, exit_side, asset,  # type: ignore
                    product_type=self._supported_products[0],
                    order_flag=OrderFlag.NORMAL,
                    order_type=OrderType.MARKET,
                    order_validity = OrderValidity.DAY,
                    timestamp=self._timestamp,
                    remark='roll-exit')
            
            entry_order = Order(
                    entry_quantity, entry_side, next_asset,  # type: ignore
                    product_type=self._supported_products[0],
                    order_flag=OrderFlag.NORMAL,
                    order_type=OrderType.MARKET,
                    order_validity = OrderValidity.DAY,
                    timestamp=self._timestamp,
                    remark='roll-entry')
            
            # update stoplosses/ takeprofits if any
            if self.exit_handler:
                self.exit_handler.copy_stoploss(asset, next_asset)
                self.exit_handler.copy_takeprofit(asset, next_asset)
            
            # update the asset order mapping
            self._orders_roll_tracker[exit_order.oid]=key
            self._orders_roll_tracker[entry_order.oid]=key
            
            # place the orders
            self.place_order(exit_order)
            self.place_order(entry_order)
            
            # update the asset symbol in position
            pos.update_from_dict({'asset':next_asset})
            self._logger.info(
                    f'Rolling position from {asset} to {next_asset}',
                    mode=self.mode, timestamp=self._timestamp) # type: ignore
    
    def _close_expired_assets(self):
        """ close assets that stopped trading. """
        if not self._store_last_date:
            return
        
        dt = self._calendar.next_open(self._timestamp)      # type: ignore 
        if dt >= self._store_last_date:
            return
        
        total_cash = total_margin = 0
        assets = list(self._open_positions.keys())
        
        for asset in assets:
            reason = ''
            if not self._check_membership(asset):
                should_close_out = True
                reason = f'as it has moved out of the benchmark. '
            elif not self.library.is_alive(asset, dt):
                should_close_out = True
                reason = f'as it is no longer alive. '
            else:
                should_close_out = False
            
            if should_close_out:
                pos = self._open_positions.pop(asset, None)
                if pos is None:
                    continue
                cash, margin = self._unwind_position(pos, self._timestamp)
                total_cash += cash
                total_margin += margin
                # add the unrealized pnl as this will not go through 
                # the normal order settlement process.
                self._round_trips_realized += (pos.unrealized_pnl + \
                                               pos.realized_pnl)
                if not pos.is_funded() and not \
                    pos.asset.instrument_type==InstrumentType.OPT:
                    self._roundtrips_unfunded_adj += (pos.unrealized_pnl +\
                                                      pos.realized_pnl)
                self._acct_needs_update = True
                msg = f'closing out position for {asset.symbol} '
                msg = msg + reason + f'Returning cash {cash} '
                msg = msg + f'and releasing margin {margin}.'
                self._logger.warning(
                        msg, mode=self.mode, timestamp=self._timestamp) # type: ignore
                
        
        if abs(total_cash) > ALMOST_ZERO or abs(total_margin) > ALMOST_ZERO:
            cash_change = total_cash - total_margin
            self._account.settle_cash(cash_change, total_margin)
    
    def _apply_corporate_actions(self):
        """ apply splits and dividends to equity assets. """
        if not self._adj_handler:
            return
        
        dt = self._calendar.next_open(
                self._timestamp).normalize().tz_convert( # type: ignore
                        'Etc/UTC').tz_localize(None)
        
        divs = self._adj_handler.dividend_for_assets(dt)
        splits = self._adj_handler.split_for_assets(dt)
        mergers = self._adj_handler.merger_for_assets(dt)
        assets = list(self._open_positions.keys())
        
        split_cash = div_cash = 0
        for asset in assets:
            pos = self._open_positions[asset]
            
            if asset.symbol in splits:
                last_fx = pos.last_fx
                if math.isnan(last_fx):
                    raise BrokerError(
                            'cannot compute split adjustment, no data.', 
                            handling=ExceptionHandling.TERMINATE)
                
                cash = pos.apply_split(1.0/splits[asset.symbol], last_fx) # type: ignore
                msg = f'A split of {splits[asset.symbol]} was applied to '
                msg = msg + f'{asset.symbol}. Returned cash of {cash}.'
                self._logger.info(
                        msg, mode=self.mode, timestamp=self._timestamp) # type: ignore
                split_cash += cash
                self._acct_needs_update = True
                self._can_update_positions = False
                
            if asset.symbol in mergers:
                last_fx = pos.last_fx
                if math.isnan(last_fx):
                    raise BrokerError(
                            'cannot compute mergers adjustment, no data.', 
                            handling=ExceptionHandling.TERMINATE)
                
                cash = pos.apply_split(1.0/mergers[asset.symbol], last_fx) # type: ignore
                msg = f'A merger of {mergers[asset.symbol]} was applied to '
                msg = msg + f'{asset.symbol}. Returned cash of {cash}.'
                self._logger.info(
                        msg, mode=self.mode, timestamp=self._timestamp) # type: ignore
                split_cash += cash
                self._acct_needs_update = True
                self._can_update_positions = False
            
            if asset.symbol in divs:
                px = self.library.get_spot_value(
                        asset, self._timestamp, 'close', self._freq) # type: ignore
                if math.isnan(px):
                    px = pos.last_price
                last_fx = pos.last_fx                   
                dividend = (1-divs[asset.symbol])*px*last_fx*pos.quantity
                if math.isnan(dividend):
                    msg = f'illegal dividend amount for {asset.symbol} '
                    msg = msg + f' for {dt}.'
                    raise BrokerError(
                            msg, handling=ExceptionHandling.TERMINATE)
                div_cash += dividend
                msg = f'A cash dividend of {div_cash} was applied to {asset.symbol}.'
                self._logger.info(
                        msg, mode=self.mode, timestamp=self._timestamp) # type: ignore
        
        if abs(div_cash) > ALMOST_ZERO:
            self._account.settle_dividends(div_cash)
            self._round_trips_realized += div_cash
        
        if abs(split_cash) > ALMOST_ZERO:
            self._account.settle_cash(split_cash, 0)
            self._round_trips_realized += split_cash
    
    def set_commissions(self, *args, **kwargs):
        pass
    
    def set_slippage(self, *args, **kwargs):
        pass
    
    def set_roll_policy(self, roll_day, roll_time):
        self._library.set_roll_policy(roll_day, roll_time)
        if not isinstance(roll_time, datetime.time):
            cutoff = datetime.time(*roll_time)
        else:
            cutoff=roll_time
        self._intraday_cutoff_time = str(cutoff)
        self._intraday_cutoff_dt = cutoff
        cutoff = cutoff.hour*3600+cutoff.minute*60+cutoff.second
        self._intraday_cutoff = cutoff
        
def _handle_simulator_library(library):
    if isinstance(library, Library):
        return library
    
    if isinstance(library, str):
        library = os.path.expanduser(library)
        
    try:
        Library(library)
    except Exception:
        from blueshift.providers.data.store.dataframe_store import DataFrameStore
        end_date = pd.Timestamp.now().normalize() + pd.Timedelta(days=252)
        store = DataFrameStore()
        store.metadata['end_date'] = end_date
        return Library(active_store=store)
    else:
        return library
    
class FXBacktester(LazyBacktestBroker):
    """ specialized backtester for Forex trading. """
    def __init__(self, initial_capital, frequency, library, calendar=None, 
                 ccy=CCY.USD, intraday_cutoff=None, logger =None, # type: ignore
                 cost_model='FX', margin_model='FX', 
                 slippage_model='FX', simulator=False, *args, **kwargs):
        if simulator:
            library = _handle_simulator_library(library)
            
        super(FXBacktester, self).__init__(
                initial_capital, frequency, library, calendar, ccy, 
                intraday_cutoff, logger, cost_model=cost_model, 
                margin_model=margin_model, slippage_model=slippage_model, 
                *args, **kwargs)
        
        if self._ccy == CCY.LOCAL:  # type: ignore
            self._ccy = CCY.USD     # type: ignore
            cash = self._account.cash
            self._account = BlotterAccount(
                    name=self._name, cash=cash,starting_cash=cash, 
                    currency=self._ccy)
            
        if simulator:
            self._name = 'fx-simulator'
        else:
            self._name = 'fx-backtester'
            self._library.active = self._library[(Forex, '1m', 'FX')]
        
class IndiaEqBacktester(LazyBacktestBroker):
    """ specialized backtester for NSE/ BSE trading. """
    def __init__(self, initial_capital, frequency, library, calendar=None, 
                 ccy=None, intraday_cutoff=(15,27), logger =None, 
                 cost_model='no_cost', margin_model='NSE', 
                 slippage_model='volume', simulator=False, 
                 is_paper=False, *args, **kwargs):
        if simulator:
            # we can afford to have a dummy library as the active store
            # and the library itself will be replaced
            library = _handle_simulator_library(library)
        
        super(IndiaEqBacktester, self).__init__(
                initial_capital, frequency, library, calendar, ccy, 
                intraday_cutoff, logger, cost_model=cost_model, 
                margin_model=margin_model, slippage_model=slippage_model, 
                is_paper=is_paper, *args, **kwargs)
        
        if simulator:
            self._name = 'india-equity-simulator'
        else:
            self._name = 'india-equity-backtester'
            self._library.active = self._library[(Equity,'1m','NSE')]
            
class IndiaEqPaperSimulator(IndiaEqBacktester):
    """ specialized backtester for NSE/ BSE paper trading. """
    def __init__(self, initial_capital, frequency, library, calendar=None, 
                 ccy=None, intraday_cutoff=(15,27), logger =None, 
                 cost_model='no_cost', margin_model='NSE', 
                 slippage_model='no-slippage', simulator=True, *args, **kwargs):
        super(IndiaEqPaperSimulator, self).__init__(
                initial_capital, frequency, library, calendar=calendar, 
                ccy=ccy, intraday_cutoff=intraday_cutoff, logger =logger, 
                cost_model=cost_model, margin_model=margin_model, 
                slippage_model=slippage_model, simulator=simulator, 
                is_paper=True, *args, **kwargs)
        
class USCashBacktester(LazyBacktestBroker):
    """ specialized backtester for US cash trading. """
    def __init__(self, initial_capital, frequency, library, calendar=None, 
                 ccy=None, intraday_cutoff=None, logger =None, 
                 cost_model='no_cost', margin_model='no_margin', 
                 slippage_model='no_slippage', simulator=False, *args, **kwargs):
        if simulator:
            library = _handle_simulator_library(library)
            
        super(USCashBacktester, self).__init__(
                initial_capital, frequency, library, calendar, ccy, 
                intraday_cutoff, logger, cost_model=cost_model, 
                margin_model=margin_model, slippage_model=slippage_model, 
                *args, **kwargs)
        
        if simulator:
            self._name = 'us-cash-simulator'
            self._library.active = self._library[(Equity,'1m')]
        else:
            self._name = 'us-cash-backtester'
            self._library.active = self._library[(Equity,'1m')]
        
class RegTBacktester(LazyBacktestBroker):
    """ specialized backtester for US margin trading. """
    def __init__(self, initial_capital, frequency, library, calendar=None, 
                 ccy=None, intraday_cutoff=None, logger =None, 
                 cost_model='no_cost', margin_model='regT', 
                 slippage_model='volume', simulator=False, 
                 is_paper=False, *args, **kwargs):
        if simulator:
            library = _handle_simulator_library(library)
            
        super(RegTBacktester, self).__init__(
                initial_capital, frequency, library, calendar, ccy, 
                intraday_cutoff, logger, cost_model=cost_model, 
                margin_model=margin_model, slippage_model=slippage_model, 
                simulator=simulator, is_paper=is_paper, *args, **kwargs)
        
        if simulator:
            self._name = 'regT-simulator'
        else:
            self._name = 'regT-backtester'
            self._library.active = self._library[(Equity,'1m')]
            
class RegTPaperSimulator(RegTBacktester):
    """ specialized backtester for US margin paper trading. """
    def __init__(self, initial_capital, frequency, library, calendar=None, 
                 ccy=None, intraday_cutoff=None, logger =None, 
                 cost_model='no_cost', margin_model='regT', 
                 slippage_model='NYSE', simulator=True, *args, **kwargs):
        super(RegTPaperSimulator, self).__init__(
                initial_capital, frequency, library, calendar=calendar, 
                ccy=ccy, intraday_cutoff=intraday_cutoff, logger =logger, 
                cost_model=cost_model, margin_model=margin_model, 
                slippage_model=slippage_model, simulator=simulator, 
                is_paper=True, *args, **kwargs)
        
class CryptoBacktester(LazyBacktestBroker):
    """ 
        Specialized backtester for crypto trading. The default 
        cost model is fixed commission of 0.10%. The default 
        slippage model is. Fractional ordering is default (i.e. 
        order function does not need to include fractional=True).
        
    """
    def __init__(self, initial_capital, frequency, library, calendar=None, 
                 ccy=CCY.USDT, intraday_cutoff=None, logger =None,  # type: ignore
                 cost_model=PerDollar(0.001), 
                 margin_model=FlatMargin(),
                 slippage_model=CryptoSlippage(50),
                 active_store=None, pipeline_store=None,
                 fx_rate_store=None, *args, **kwargs):
            
        super(CryptoBacktester, self).__init__(
                initial_capital, frequency, library, calendar, ccy, 
                intraday_cutoff, logger, cost_model=cost_model, 
                margin_model=margin_model, slippage_model=slippage_model, 
                active_store=active_store, pipeline_store=pipeline_store,
                fx_rate_store=fx_rate_store, *args, **kwargs)
        self._name = 'crypto'
        self._fractional_default = True
        
    def symbol(self, symbol, dt=None, *args, **kwargs):
        """ convert any XXX/USD query to XXX/USDT. """
        parts = symbol.split('/')
        if len(parts)==2 and parts[1] == 'USD':
            symbol = parts[0] + '/' + 'USDT'
        elif len(parts) == 1:
            symbol = parts[0] + '/' + 'USDT'
        
        return self._library.symbol(symbol, dt=dt, *args, **kwargs)

registered_brokers = {
    'backtest':(LazyBacktestBroker, None),
    'forex':(FXBacktester, 'FX'),
    'india-eq-broker':(IndiaEqBacktester, 'NSE'),
    'nse':(IndiaEqBacktester, 'NSE'),
    'bse':(IndiaEqBacktester, 'BSE'),
    'india-eq-paper':(IndiaEqPaperSimulator, 'NSE'),
    'nse-paper':(IndiaEqPaperSimulator, 'NSE'),
    'bse-paper':(IndiaEqPaperSimulator, 'BSE'),
    'regT':(RegTBacktester, 'NYSE'),
    'us-cash':(USCashBacktester, 'NYSE'),
    'crypto':(CryptoBacktester, 'CRYPTO'),
}

for name in registered_brokers:
    broker, calendar = registered_brokers[name]
    register_broker(name, broker, calendar)
