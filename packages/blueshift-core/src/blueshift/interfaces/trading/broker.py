from __future__ import annotations
from enum import Enum
from abc import ABC, abstractmethod
from typing import cast, TYPE_CHECKING, Type, Callable, Any
from logging import Logger
import datetime
from collections import deque

from blueshift.lib.trades._order_types import ProductType
from blueshift.lib.trades._position import Position
from blueshift.lib.trades._order import Order
from blueshift.lib.trades._accounts import Account, BlotterAccount
from blueshift.config.defaults import get_config
from blueshift.config import LOCK_TIMEOUT
from blueshift.lib.common.ctx_mgrs import TimeoutRLock
from blueshift.lib.common.types import PartialOrder
from blueshift.lib.common.constants import Frequency
from blueshift.lib.common.enums import (
        ExecutionMode, BlotterType, BrokerType, AlgoMode, AlgoCallBack)
from blueshift.lib.common.constants import CCY, Currency
from blueshift.lib.common.decorators import with_lock
from blueshift.lib.exceptions import (
        InitializationError, BrokerError)
from blueshift.calendar.trading_calendar import TradingCalendar
from blueshift.interfaces.data.library import get_library

from ..assets.assets import IAssetFinder
from ..assets._assets import Asset
from ..trading._simulation import ABCCostModel, ABCMarginModel, ABCSlippageModel
from ..data.data_portal import DataPortal
from ..data.store import DataStore
from ..data.library import ILibrary
from ..plugin_manager import load_plugins

from .rms import NoRMS, IRMS

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.interfaces.data.ingestor import StreamingIngestor

class AccountType(Enum):
    BACKTEST=0
    PAPER=1
    SIMULATED=2
    LIVE=3

class AbstractBrokerAPI(ABC):
    '''
        Broker abstract interface. This implements the expected broker 
        behaviour for blueshift engine. The overall behavirous can be 
        broker down as follows.
        
        - implements orders and positions related APIs. This includes 
            placing, updating and cancelling trades; fetching positions 
            and orders (including open and closed order and positions) and 
            optionally, estimation of trading costs and margins
            
        - implements authorization and login/ logout methods.
        
        - implements and tracks a host of properties that identifies the 
            type of the brokers, the running and execution modes it supports,
            etc.
            
        The broker interface implementation involves real-time operations, 
        and requires performance and memory optimization whenever required 
        (this is especially true in terms of data structures that it must 
        maintain over which we do no have direct control - as it is dictated 
        by users strategy code e.g. the size of the orders or positions 
        dictionaries). In addition, it must handle robust error handling 
        and error recovery with very clear error messages.
    '''
    DEFAULT_CCY = CCY.LOCAL # type: ignore

    def __init__(self, *args, **kwargs):
        self._name = kwargs.get('name', 'broker')
        self._type = kwargs.pop('broker_type', BrokerType.RESTBROKER)

        try:
            self._type = BrokerType(self._type)
        except Exception as e:
            raise InitializationError(f'invalid broker type {self._type}') from e

        self._modes_supported = kwargs.pop(
                'supported_modes', [AlgoMode.LIVE, AlgoMode.PAPER])
        try:
            self._modes_supported = [AlgoMode(mode) for mode in self._modes_supported]
        except Exception as e:
            raise InitializationError(f'invalid algo mode(s) in {self._modes_supported}') from e

        self._exec_modes_supported = kwargs.pop(
                'execution_modes',
                [ExecutionMode.AUTO, ExecutionMode.ONECLICK])
        try:
            self._exec_modes_supported = [ExecutionMode(mode) for mode in self._exec_modes_supported]
        except Exception as e:
            raise InitializationError(f'invalid execution mode(s) in {self._modes_supported}') from e
        
        self._supported_products = cast(list[ProductType], [ProductType.DELIVERY])
        
        self._ccy = kwargs.pop('currency','LOCAL')
        try:
            self._ccy = CCY[self._ccy]                      # type: ignore
            if self._ccy == CCY.LOCAL:                      # type: ignore
                self._ccy = self.DEFAULT_CCY
        except Exception as e:
            raise InitializationError(f'invalid currency {self._ccy}') from e
            
        self._blotter_type = kwargs.pop(
                'blotter_type', BlotterType.EXCLUSIVE)
        try:
            self._blotter_type = BlotterType(self._blotter_type)
        except Exception as e:
            raise InitializationError(f'invalid blotter type {self._blotter_type}') from e
        
        self._mode = kwargs.pop('mode', AlgoMode.LIVE)
        try:
            self._mode = AlgoMode(self._mode)
        except Exception as e:
            raise InitializationError(f'invalid current algo mode {self._mode}') from e

        if self._mode not in self._modes_supported:
            raise InitializationError(
                    f'mode {self._mode} not supported.')
        
        self._streaming_update = kwargs.pop('streaming_update', False)
        
        # trackers,  need this for round-trip calcs for exclusive blotter
        self._closed_positions = []
        self._blotter_closed_positions = []
        self._processed_closed_pos = {}
        
        # is fractional orering the default mode?
        self._fractional_default = False
        
        # reference to algo exit handler
        self._exit_handler = None
        
        # algo user for shared broker
        self._algo_user = kwargs.pop('algo_user', None)

    def __str__(self):
        return "Blueshift Broker [name:%s]" % (self._name)

    def __repr__(self):
        return self.__str__()
    
    @property
    def name(self) -> str:
        """ 
            Returns name of the broker. Must be unique for all registered
            brokers.
        """
        return self._name
    
    @property
    def logger(self) -> Logger:
        """ Returns the logger of the broker. """
        return self._logger
    
    @logger.setter
    def logger(self, value):
        """ Set the logger of the broker. """
        self._logger = value
        
    @property
    def broker_logger(self) -> Logger:
        """ original logger for broker for shared broker. """
        return self._logger
    
    @property
    def type(self) -> BrokerType:
        """ 
            Returns the type of the broker. Must be of enum type of 
            ``BrokerType``.
        """
        return self._type
    
    @property
    def blotter_type(self) -> BlotterType:
        """
            Returns the blotter type. Must be of enum type of 
            ``BlotterType``.
        """
        return self._blotter_type
    
    @property
    def mode(self) -> AlgoMode:
        """
            Returns the running mode of the broker. Must be of enum 
            type of ``AlgoMode``.
        """
        return self._mode
    
    @property
    def ccy(self) -> Currency:
        """
            Returns the currency of the broker account.
        """
        return self._ccy
    
    @property
    def is_default_fractional(self) -> bool:
        """
            Returns true if the default ordering method is fractional.
        """
        return self._fractional_default
    
    @property
    def streaming_update(self) -> bool:
        """ True if the broker supports streaming update. """
        return self._streaming_update
    
    @property
    def supported_modes(self) -> list[AlgoMode]:
        """ Supported running mode (an iterable of ``AlgoMode``). """
        return self._modes_supported.copy()
    
    @property
    def supported_products(self) -> list[ProductType]:
        """ Supported products (an iterable of ``ProductType``). """
        return self._supported_products
    
    @property
    def events(self) -> dict:
        """ Set of event flags set during the last trading_bar call. """
        return {}
    
    @property
    def execution_modes(self) -> list[ExecutionMode]:
        """ Supported execution modes (an iterable of ``ExecutionMode``). """
        return self._exec_modes_supported.copy()
    
    @property
    def algo_user(self) -> str|None:
        """ Current user for the broker interface. """
        return self._algo_user
    
    @algo_user.setter
    def algo_user(self, value:str):
        self._algo_user = value
    
    @property
    def exit_handler(self):
        """ A reference to algo exit handler. """
        return self._exit_handler
    
    @exit_handler.setter
    def exit_handler(self, handler):
        """ update the reference to algo exit handler. """
        self._exit_handler = handler
    
    def setup_model(self, *args, **kwargs) -> None:
        pass
    
    def set_currency(self, *args, **kwargs) -> None:
        pass
    
    def set_commissions(self, *args, **kwargs) -> None:
        pass
    
    def set_slippage(self, *args, **kwargs) -> None:
        pass
    
    def set_margin(self, *args, **kwargs) -> None:
        pass
    
    def set_roll_policy(self, *args, **kwargs) -> None:
        pass
    
    @property
    def cost_model(self) -> ABCCostModel|None:
        pass
    
    @property
    def margin_model(self) -> ABCMarginModel|None:
        pass
    
    @property
    def simulator(self) -> ABCSlippageModel|None:
        pass

    @abstractmethod
    def login(self, *args, **kwargs) -> None:
        """ Login method - called when required. """
        raise NotImplementedError

    @abstractmethod
    def logout(self, *args, **kwargs) -> None:
        """ Logout method - called when required. """
        raise NotImplementedError

    @property
    @abstractmethod
    def account_type(self) -> AccountType:
        """ Account type - enum of type ``AccountType``. """
        raise NotImplementedError
    
    @property
    @abstractmethod
    def calendar(self) -> TradingCalendar:
        """ The trading calendar associated with the broker. """
        raise NotImplementedError
        
    @property
    @abstractmethod
    def store(self) -> DataStore:
        """ The default store object associated with the broker. """
        raise NotImplementedError
        
    @property
    def library(self) -> ILibrary|None:
        """ The default library object associated with the broker. """
        library = getattr(self, '_library', None)
        if isinstance(library, str):
            try:
                library = get_library(root=library)
            except Exception:
                return
            if not isinstance(library, ILibrary):
                return
            self._library = library
        
        return library
    
    def get_library_root(self) -> str|None:
        """
            Method to fetch root without invoking the library build to 
            speed things up. Fall back to library properties if the attr 
            "_library" not present or not a string.
        """
        library = getattr(self, '_library', None)
        if isinstance(library, str):
            return library
        
        library = self.library
        if library:
            return library.root

    @property
    @abstractmethod
    def profile(self) -> dict:
        """ Fetch the user profile. """
        raise NotImplementedError

    @property
    @abstractmethod
    def account(self) -> dict:
        """ 
            Fetch the account details, returns a ``dict`` representation
            of an ``Account`` object.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def positions(self) -> dict[Asset, Position]:
        """
            Fetch open positions. Returns a ``dict`` with keys as 
            ``Asset`` object and values as ``Position`` object.
        """
        raise NotImplementedError
        
    @property
    def intraday_positions(self) -> dict[Asset, Position]:
        """
            Fetch open intraday positions. Returns a ``dict`` with 
            keys as ``Asset`` object and values as ``Position`` 
            object. Only applicable for brokers diffirentiating 
            intraday vs positional trading.
        """
        return {}
        
    @property
    def closed_positions(self) -> list[Position]:
        """
            Fetch a list of closed positions. The max history depends on 
            the broker and interface implementation, but at minimum must 
            include the same for the current trading session (day).
        """
        return self._closed_positions.copy()
    
    @abstractmethod
    def position_by_asset(self, asset, *args, **kwargs) -> Position|None:
        """
            Returns open position for a given asset.
        """
        raise NotImplementedError
        
    def get_positions(self, *args, **kwargs) -> dict[Asset, Position]:
        """
            Implement alternative versions of positions
        """
        return self.positions

    @property
    @abstractmethod
    def orders(self) -> dict[str, Order]:
        """
            Fetch all open and closed positions in the record. Must 
            include all open positions. Closed position history depends 
            on the broker implementation but at minimum should include 
            the same for the current trading session (day). Returns a 
            ``dict`` keyed by order id (str) and values as ``Order`` 
            object.
        """
        raise NotImplementedError
    
    @property
    def synthetic_orders(self) -> dict[str, Order]:
        """ 
            Required for certain brokers where unwinding of existing 
            positions are handled not through a new order.
        """
        return {}
    
    @property
    @abstractmethod
    def open_orders(self) -> dict[str, Order]:
        """
            Fetch all open orders (not filled or partially filled). 
            Returns a ``dict`` with keys as order IDs and values as 
            ``Order`` objects.
        """
        raise NotImplementedError
    
    @abstractmethod
    def get_order(self, order_id, *args, **kwargs) -> Order|None:
        """
            Fetch an order - returns an ``Order`` object.
        """
        raise NotImplementedError
        
    def get_all_orders(self, *args, **kwargs) -> dict[str, Order]:
        """
            Return all orders - useful if we need to pass any extra 
            arguments to filter the fetch.
        """
        return self.orders
        
    def fetch_all_orders(self, *args, **kwargs) -> dict[str, Order]:
        """
            Force a fetch of all orders from the broker API. This 
            is a fallback for brokers with streaming order updates
            and we have missed updates for a particular order due 
            to some reasons (e.g. packet lost, malfunction on the 
            broker side etc.). A connection drop should not be 
            handled by this method, and broker implementation should 
            automatically refresh order data on reconnect.
        """
        return self.orders
        
    @abstractmethod
    def open_orders_by_asset(self, asset, *args, **kwargs) -> list[Order]:
        """
            Returns an iterable of ``Order`` objects given an asset. 
            Returns all open orders for that asset or an empty list.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def tz(self) -> str:
        """ Returns the time zone of the broker, must match the calendar. """
        raise NotImplementedError

    @abstractmethod
    def place_order(self, order, *args, **kwargs) -> str|list[PartialOrder|str]|None:
        """
            Place a new order. Must accept an ``Order`` object and 
            optional keyword arguments specific to the broker. It must 
            validate orders before placing and returns the order id to 
            the caller. Broker specific implementation (e.g. different
            order types or validity types) and error handling must be 
            done. At minimum, should support all blueshift order types, 
            validity types and product types. If any of these is not 
            supported by the broker, must raise appropriate exception.
        """
        raise NotImplementedError

    @abstractmethod
    def update_order(self, *args, **kwargs) -> str:
        """
            Update an existing order. Must accept an ``Order`` object 
            and at minimum support updating of order quantity, order 
            limit price and order validty. If any of these is not 
            supported by the underlying broker, must raise appropriate 
            exception. Should return the order id (same as the existing
            order id).
        """
        raise NotImplementedError

    @abstractmethod
    def cancel_order(self, *args, **kwargs) -> str:
        """
            Cancel an existing order. Must accept an ``Order`` object 
            or an order ID. Returns the order ID on success.
        """
        raise NotImplementedError
        
    def fund_transfer(self, *args, **kwargs) -> float|None:
        """
            This is at present not supported, but raise 
            NotImplementedError.
        """
        raise NotImplementedError

    def trading_bar(self, timestamp):
        """
            Methods that is called every minute by the blueshift engine. 
            Add handlers here, if required. In most cases, for live 
            brokers, this should be as is or should have very light. One
            good example to place here is account solvency or margin.
        """
        pass
    
    def paper_simulate(self, timestamp):
        raise NotImplementedError('Paper Simulation is not implemented.')
        
    def paper_reconcile(self, timestamp):
        raise NotImplementedError('Paper reconciliation is not implemented.')

    def before_trading_start(self, timestamp):
        """
            Implemet start of the day handlers here. Must reset the 
            streaming data store, and any additional handling reqiured.
            Also must re-establish streaming connection (usually by 
            calling own's ``login`` method) if disconnected.
            
            Note: refresh_data method to refresh the list of tradeable 
            assets for the day is automatically called by the engine.
        """
        pass

    def after_trading_hours(self, timestamp):
        """
            Implement end of day routines. Usually disconnect the 
            streaming connection (by calling own's ``logout`` method).
        """
        pass

    def algo_start(self, timestamp):
        """
            Implements any specific routines for start of the execution.
        """
        pass

    def algo_end(self, timestamp):
        """
            Implements any specific routines for end of the execution.
        """
        pass

    def heart_beat(self, timestamp):
        """
            Routines for hearbeat, called every minute during non-market 
            hours. Should be left as is in most cases.
        """
        pass

    def get_trading_costs(self, order, *args, **kwargs) -> tuple[float, float]:
        """
            Accept an ``Order`` object (before actual execution) and 
            returns estimated trading costs as a tuple (commissions, 
            charges), where commissions refers to that levied by the 
            brokers and charges refers to that levied by the venue (the 
            exchange) or authorities. Returns a tuple of (0,0) if not 
            available (the default implementation). This must return NaN
            if there is any error.
        """
        fees = 0                # what the broker charges
        tax_and_charges = 0     # taxes and exchange charges - third party

        return fees, tax_and_charges

    def get_trading_margins(self, orders, positions, *args, **kwargs) -> tuple[bool, float]:
        """
            This should returns the margin requirement given the orders 
            and the positions. If incremental, the first value must be 
            true, else false.
        """
        is_incremental = True
        return is_incremental, 0
    
    def get_account(self, *args, **kwargs) -> Account:
        """ Returns the underlying ``Account`` object. """
        raise NotImplementedError
        
    @property
    def intraday_cutoff(self) -> tuple|datetime.time|None:
        pass
        
    def get_asset_from_order(self, order) -> Asset:
        """ 
            Returns the asset object for the position generated by a 
            given order. Usually the asset of the order is same as the 
            asset for the position, but maybe different in case of a 
            rolling asset and the broker implementation tracks the position
            differently.
        """
        return order.asset
        
    def save(self, path, *args, **kwargs):
        """ Save the object data to disk. """
        pass
    
    def read(self, path, *args, **kwargs):
        """ Read saved object data from disk. """
        pass
    
    def reset(self, *args, **kwargs):
        """ Reset object data. """
        pass
    
    def add_algo(self,algo, algo_user,  *args, **kwargs):
        """ Add algo name and user to the broker. """
        pass
    
    def remove_algo(self,algo,  *args, **kwargs):
        """ Remove algo from the broker. """
        pass

class IBroker(AbstractBrokerAPI, IAssetFinder, DataPortal):
    
    def __init__(self, name, *args, **kwargs):
        AbstractBrokerAPI.__init__(self, name, *args, **kwargs)
        IAssetFinder.__init__(self, name, *args, **kwargs)
        
        self._rms = kwargs.get('rms', None)
        if not self._rms:
            self._rms = NoRMS(self)
        else:
            if not isinstance(self._rms, IRMS):
                raise InitializationError(f'invalid RMS type specified, got {type(self._rms)}')
            
        self._subscribed_assets = set()
        self._lock = TimeoutRLock(timeout=LOCK_TIMEOUT)
        
    def __hash__(self):
        return hash(self.id())
    
    def id(self) -> str:
        return self.__str__()
    
    @property
    def wants_to_quit(self) -> bool:
        # if the broker is done, i.e. no need to auto-reconnect and can 
        # give up any ongoing api retry
        return False
    
    @property
    def data_portal(self) -> DataPortal:
        """ default data portal is the broker itself. """
        return self
    
    @property
    def subscribed_assets(self) -> set:
        return self._subscribed_assets
    
    @property
    def rms(self) -> IRMS:
        """ A reference to the RMS. """
        self._rms = cast(IRMS, self._rms)
        return self._rms
    
    def pretrade(self, order:Order, *args, **kwargs) -> Order:
        """ Apploy pre-trade check on the order. """
        return self.rms.pretrade(order, *args, **kwargs)
    
    @with_lock()
    def initialize(self, *args, **kwargs):
        """ initialize the broker object. """
        pass
    
    @with_lock()
    def finalize(self, *args, **kwargs):
        """ finalize the broker object. """
        pass

class IBacktestBroker(IBroker):
    @property
    def library(self) -> ILibrary:
        return self._library
    
    @property
    @abstractmethod
    def round_trips(self) -> list|deque:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def transactions(self) -> dict:
        raise NotImplementedError
    
    @abstractmethod
    def place_order(self, order, *args, **kwargs) -> str|None:
        raise NotImplementedError
    
    @abstractmethod
    def get_account(self, *args, **kwargs) -> BlotterAccount:
        """ Returns the underlying ``Account`` object. """
        raise NotImplementedError
    
    @abstractmethod
    def paper_simulate(self, timestamp:pd.Timestamp):
        raise NotImplementedError
    
    @abstractmethod
    def backtest_simulate(self, timestamp:pd.Timestamp):
        raise NotImplementedError
    
class ILiveBroker(IBroker):
    @property
    @abstractmethod
    def is_connected(self) -> bool:
        raise NotImplementedError
    
    @property
    def ingestor(self) -> StreamingIngestor|None:
        pass

    @property
    def streamer(self) -> StreamingIngestor|None:
        pass
    
    def set_algo_callback(self, callback:Callable, *args, **kwargs):
        pass

class IPaperBroker(IBacktestBroker):
    def __init__(self, broker:ILiveBroker, backtester:IBacktestBroker, initial_capital:float|None, 
                 frequency:Frequency|str, **kwargs):
        pass
            
    @property
    @abstractmethod
    def is_connected(self) -> bool:
        raise NotImplementedError
    
    def set_algo_callback(self, callback:Callable, *args, **kwargs):
        pass

class IBrokerCollection:
    """A broker collections interface for dispatch amongst multiple brokers. """
        
    def get_broker(self, name:str, key:str):
        """ returns IBroker based on a name and a key. """
        raise NotImplemented
    
    def init_broker(self, name, key, *args, **kwargs):
        """ initialize a broker with a name and a key, and broker specific args/kwatgs. """
        raise NotImplementedError
                
    def add_algo(self, algo, algo_user, name, key):
        """ maps an algo/user to a broker via name and key pair. """
        raise NotImplementedError
                
    def remove_algo(self, algo):
        """ 
            remove an algo -> the underlying broker may choose to terminate if no 
            algo remains connected.
        """
        raise NotImplementedError
        
_broker_registry:dict[str,Type[IBroker]] = {}
_paper_broker_registry:dict[str,Type[IPaperBroker]] = {}
_broker_calendar_mapping = {}
_custom_factory_methods:dict[str,Callable[...,IBroker]] = {}

def read_blueshift_broker_config(path=None):
    brokers = {}
        
    try:
        config = get_config().brokers
        data_portal = get_config().data_portal
        config = {**config, **data_portal}
        
        for key in config:
            entry = config[key]
            if 'name' not in entry:
                continue
            broker = entry['name']
            if broker in brokers:
                brokers[broker].append(entry)
            else:
                brokers[broker] = [entry]
    except Exception:
        pass
    
    return brokers

def make_broker_key(name, **kwargs) -> str:
    import hashlib

    key = kwargs.pop('key', None)
    if key:
        return key
    
    used_keys = []
    kwargs['name'] = name
    
    kwarg_keys = sorted(set(list(kwargs.keys())))
    key = ''
    for k in kwarg_keys:
        if 'token' in k or 'id' in k or 'key' in k or k in ('name'):
            key += str(kwargs[k])
            used_keys.append(k)
    
    if not key:
        msg = f'Could not create key for broker {name}.'
        raise BrokerError(msg)
    else:
        key = hashlib.sha1(key.encode()).hexdigest() #nosec
            
    return key

_builtin_brokers_loader: Callable[[Any], None] | None = None

def set_builtin_brokers_loader(loader: Callable[[Any], None]):
    """Register a callable that will lazily load built-in data store types."""
    global _builtin_brokers_loader
    _builtin_brokers_loader = loader

def _ensure_builtin_brokers_loaded(broker_type:str|None=None):
    if _builtin_brokers_loader:
        _builtin_brokers_loader(broker_type)

    if (broker_type and broker_type not in _broker_registry) or broker_type is None:
        try:
            load_plugins('blueshift.plugins.broker')
        except Exception:
            pass

def register_broker(name:str, broker:Type[IBroker], calendar:TradingCalendar|str|None=None):
    """ register a broker type to the global mapping. """
    _broker_registry[name] = broker
    
    if name=='paper':
        broker = cast(Type[IPaperBroker], broker)
        _paper_broker_registry[name] = broker
    else:
        _broker_calendar_mapping[name] = calendar
    
def get_broker(name) -> Type[IBroker]|None:
    """ get a registered broker type by name. """
    if name not in _broker_registry:
        _ensure_builtin_brokers_loaded(name)
    return _broker_registry.get(name, None)

def get_broker_calendar(name) -> TradingCalendar|None:
    """ get a registered broker type by name. """
    if name not in _broker_registry:
        _ensure_builtin_brokers_loaded(name)
    return _broker_calendar_mapping.get(name, None)

def list_brokers() -> list[str]:
    """ get a list of registered broker names. """
    _ensure_builtin_brokers_loaded()
    return list(_broker_registry.keys())

def _broker_factory(name=None, *args, **kwargs) -> IBroker:
    from inspect import getfullargspec
    cls = get_broker(name)
    
    if not cls:
        raise BrokerError(f'broker {name} is not available.')
        
    specs = getfullargspec(cls.__init__)
    if specs.varkw:
        kw = kwargs.copy()
    else:
        args_specs = specs.args
        kw = {}
        for key in kwargs:
            if key in args_specs:
                kw[key] = kwargs[key]
    
    return cls(*args, **kw)

def register_custom_broker_factory(factory:Callable[...,IBroker], key:Any):
    _custom_factory_methods[key] = factory

def broker_factory(name=None, *args, **kwargs) -> IBroker:
    """ factory function to create broker. """
    for key in _custom_factory_methods:
        if key in kwargs and kwargs[key]:
            factory = _custom_factory_methods[key]
            kwargs.pop(key, None)
            return factory(name, *args, **kwargs)
    
    return _broker_factory(name=name, *args, **kwargs)

def paper_broker_factory(broker:ILiveBroker, backtester:IBacktestBroker, initial_capital:float|None, 
                 frequency:Frequency|str, **kwargs) -> IPaperBroker:
    if 'paper' not in _paper_broker_registry:
        raise NotImplementedError('paper broker is not implemented')
    
    cls = _paper_broker_registry['paper']
    return cls(broker, backtester, initial_capital, frequency, **kwargs)

_broker_collection_cls = None

def register_broker_collection(cls:Type[IBrokerCollection]):
    global _broker_collection_cls
    _broker_collection_cls = cls

def get_broker_collection(*args, **kwargs) -> IBrokerCollection:
    from inspect import getfullargspec

    _ensure_builtin_brokers_loaded(None)

    if not _broker_collection_cls:
        raise NotImplementedError('broker collection is not implemented.')
    
    specs = getfullargspec(_broker_collection_cls.__init__)

    if specs.varkw:
        kw = kwargs.copy()
    else:
        kw = {}
        args_specs = specs.args
        for key in kwargs:
            if key in args_specs:
                kw[key] = kwargs[key]
    
    return _broker_collection_cls(*args, **kw)

__all__ = [
    'IBroker', 
    'IBacktestBroker', 
    'IPaperBroker', 
    'ILiveBroker', 
    'IBrokerCollection',
    'register_broker', 
    'broker_factory',
    'paper_broker_factory',
    'get_broker',
    'get_broker_calendar',
    'list_brokers',
    'make_broker_key',
    'read_blueshift_broker_config',
    'set_builtin_brokers_loader',
    'register_custom_broker_factory',
    ]