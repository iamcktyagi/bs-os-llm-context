# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
from libc.math cimport sqrt, fabs, fmax, isnan
cimport numpy as np
import numpy as np
import math

from blueshift.interfaces.assets._assets cimport InstrumentType, Asset
from blueshift.interfaces.trading._simulation cimport ABCSlippageModel
from blueshift.lib.trades._order cimport Order
from blueshift.lib.trades._position cimport Position
from blueshift.lib.trades._order_types cimport (
    OrderSide, OrderType, OrderStatus, OrderValidity)

from blueshift.lib.common.constants import ALMOST_ZERO
from blueshift.lib.exceptions import BrokerError

cdef int LARGE_VOLUME = 1000000000
cdef int MAX_OPT_QTY = 100
cdef double MIN_OPT_PRICE = 0.05

cdef class SlippageModel(ABCSlippageModel):
    """ 
        Slippage model implements the backtest simulation. The core 
        method `simulate` takes in an order and simulates the fill 
        and the fill price.
    """
    def __init__(self, frequency=None, library=None):
        self._freq = frequency
        self._library = library
        
    def set_frequency(self, frequency):
        self._freq = frequency
        
    def set_library(self, library):
        self._library = library
        
    cpdef tuple simulate(self, Order order, object dt):
        """
            Takes in an order (may be already partially filled), and 
            returns a trade with executed price and amount, as well 
            as the fair mid. The difference between the execution price
            and the fair mid is the slippage.
            
            :param order: Order object to process.
            :type order: :ref:`order<Order>` object.
            :param pandas.Timestamp dt: Timestamp for the order.
            :return: A tuple of volume, price, fair mid and max volume.
            :rtype: (float, float, float, float)
        """
        raise NotImplementedError
    
cdef class OrderBookSlippage(SlippageModel):
    """ Trade simulation based on an order book. """
    pass

cdef class BarDataSlippage(SlippageModel):
    """ Trade simulation based on OHLCV data. """
    def __init__(self, max_volume=0.02, frequency=None, library=None):
        """
            At minimum, a bar (OHLCV) based trade simulator must be 
            initiated with a `library` object, that implements the 
            `get_spot_value` method to fetch an OHLCV column at a given
            timestamp. Also, optionally specify the `CostModel` for 
            calculating brokerages and other charges for this simulation.
            If not `cost_model` keyword is found, it is assumed to be 
            zero cost.
        """
        super(BarDataSlippage, self).__init__(frequency, library)
        self._max_volume = max_volume
        
    @classmethod
    def get_arguments(self):
        return ['frequency','library','max_volume']
        
    cdef _calculate(self, Order order, object dt):
        """
            The implementation of impact cost modelling.
            
            Returns:
                Tuple. Volume traded (float) and impact cost (float) w.r.t
                the close price at the bar. The impact cost is added 
                for a `buy` execution and subtracted for a `sell`.
        """
        raise NotImplementedError
        
    cpdef tuple simulate(self, Order order, object dt):
        """
            Generate trade against an order at a given timestamp. 
            Order may already be partially filled.
        """
        cdef np.float64_t px, impact, max_volume = 0
        cdef np.float64_t traded = 0
        
        if order.status != OrderStatus.OPEN:
            return 0, 0, 0, LARGE_VOLUME
        
        px = self._library.get_spot_value(
                order.asset, dt, 'close', self._freq)
        
        if math.isnan(px):
            return 0, 0, 0, LARGE_VOLUME
        
        last = px
        if not order.is_triggered(px):
            # checking for trigger also update the order
            # converting a SL order to a limit and a SL-M to market
            return 0, 0, last, LARGE_VOLUME
        # at this point, we should have either limit or market order
        
        traded, impact, max_volume = self._calculate(order, dt)
        if traded == 0:
            # could not get any fill
            return 0, 0, last, max_volume
        if order.fractional and abs(traded) < ALMOST_ZERO:
            return 0, 0, last, max_volume
        
        # no trade for FOK or AON if not in full
        if order.order_validity in [OrderValidity.FOK, OrderValidity.AON]:
            if traded < order.pending:
                return 0, 0, last, max_volume
        
        if order.side == OrderSide.BUY:
            px = px + impact
            if order.order_type == OrderType.LIMIT and px > order.price:
                return 0, 0, last, max_volume
        else:
            px = fmax(0, px - impact) # min sell price is 0
            if order.order_type == OrderType.LIMIT and px < order.price:
                return 0, 0, last, max_volume
        
        return traded, px, last, max_volume
    
cdef class NoSlippage(BarDataSlippage):
    """
        Trade simulation based on OHLCV data without any slippage. The
        max volume executed at each trading bar is capped at 
        `max_volume` fraction (defaults to 0.02, i.e. 2% of available
        volume at each bar) of the available volume at the bar. Setting 
        `max_volume` to 0 will disable any cap and the full pending 
        amount will be assumed to trade successfully in a single trade.
        The impact cost is always zero, i.e. the order is executed at 
        the available 'close' price on that bar.
        
        :param float max_volume: maximum volume that can be executed at a bar.
        
    """
            
    cdef _calculate(self, Order order, object dt):
        cdef np.float64_t available = 0
        cdef np.float64_t traded = 0
        
        if self._max_volume == 0:
            traded = order.pending
        else:
            if order.asset.instrument_type == InstrumentType.OPT:
                available = round(MAX_OPT_QTY*order.asset.mult/self._max_volume)
            elif order.asset.instrument_type == InstrumentType.FUTURES:
                available = round(MAX_OPT_QTY*order.asset.mult/self._max_volume)
            else:
                available = LARGE_VOLUME
            
            if not order.fractional:
                traded = min(round(self._max_volume*available), order.pending)
            else:
                traded = min(self._max_volume*available, order.pending)
        
        if traded == 0:
            return 0, 0, LARGE_VOLUME
        
        if order.fractional and abs(traded) < ALMOST_ZERO:
            return 0, 0, LARGE_VOLUME
        
        
        return traded, 0, LARGE_VOLUME
    
cdef class BidAskSlippage(BarDataSlippage):
    """
        Trade simulation based on OHLCV data with bid-ask spread. The
        max volume executed is capped at `max_volume` fraction of the 
        available volume at the bar. The impact cost equals to half the
        bid-ask spread. Setting`max_volume` to 0 will disable any cap 
        and the full pending amount will be assumed to trade successfully 
        in a single trade.
        
        :param float max_volume: maximum volume that can be executed at a bar.
        
        .. warning::
            This is supported only for cases where the dataset includes 
            the 'bid' and 'ask' data. At present, only the Forex data 
            set is suitable for this slippage model.
    """
        
    cdef _calculate(self, Order order, object dt):
        cdef np.float64_t bid, ask = 0
        cdef np.float64_t traded, available, max_tradeable = 0
        
        if order.asset.instrument_type == InstrumentType.OPT:
            raise NotImplementedError('Options slippage not implemented in this model')
        
        if self._max_volume == 0:
            traded = order.pending
            max_tradeable = LARGE_VOLUME
        else:
            available = self._library.get_spot_value(
                    order.asset, dt, 'volume', self._freq)
            if not order.fractional:
                traded = min(round(self._max_volume*available), order.pending)
                max_tradeable = round(self._max_volume*available)
            else:
                traded = min(self._max_volume*available, order.pending)
                max_tradeable = self._max_volume*available
        
        if traded == 0:
            return 0, 0, max_tradeable
        
        if order.fractional and abs(traded) < ALMOST_ZERO:
            return 0, 0, max_tradeable
        
        bid = self._library.get_spot_value(
                order.asset, dt, 'bid', self._freq)
        ask = self._library.get_spot_value(
                order.asset, dt, 'ask', self._freq)
        
        if math.isnan(bid) or math.isnan(ask):
            return 0, 0, max_tradeable
        
        return traded, (ask-bid)/2, max_tradeable

cdef class VolumeSlippage(BarDataSlippage):  
    r""" 
        Trade simulation based on OHLCV data with volume based slippage. 
        In this model, the max volume that can be executed is capped 
        at a fixed percent of the volume at the bar. The price impact is
        modelled based on the below.
        
        .. math::
            \Delta P = s/2 + \alpha.\sigma.(\frac{Q}{V})^\beta
            
        where:
            :math:`\Delta P` = impact,
            :math:`s` = spread,
            :math:`\alpha` = cost coefficient,
            :math:`\sigma` = historical volatility,
            :math:`Q` = traded volume,
            :math:`V` = available volume,
            :math:`\beta` = cost exponent
                
        :param float max_volume: maximum participation (defaults to 0.02).
        :param float cost_coeff: cost coefficient (defaults to 0.002).
        :param float cost_exponent: cost exponent (defaults to 0.5).
        :param float spread: constant spread (defaults to 0).
        :param bool spread_is_percentage: If false, spread is treated as absolute (defaults False).
        :param bool use_vol: If false, vol is set to 1.0 (defaults to False).
        :param float vol_floor: Floor value of vol (defaults to 0.05), ignored if use_vol is False.
        
        .. seealso::
            For more on the model, see |ref_link|.
            
        .. |ref_link| raw:: html
        
            <a href="https://mfe.baruch.cuny.edu/wp-content/uploads/2017/05/Chicago2016OptimalExecution.pdf" target="_blank">here</a>
            
    """
    cdef readonly np.float64_t _cost_coeff
    cdef readonly np.float64_t _cost_exponent
    cdef readonly np.float64_t _spread_cost
    cdef readonly bool _is_percent
    cdef readonly bool _is_volume_absolute
    cdef readonly np.float64_t _floor
    cdef readonly bool _use_vol
    cdef readonly object _vol
    cdef readonly object _last_dt
    
    def __init__(self, max_volume=0.02, cost_coeff=0.005, cost_exponent=0.5, 
                 spread=0.001, spread_is_percentage=True, 
                 volume_is_absolute=False, use_vol=False, 
                 vol_floor=0.05, frequency=None, library=None):
        super(VolumeSlippage, self).__init__(max_volume, frequency, library)
        
        self._cost_coeff = cost_coeff
        self._cost_exponent = cost_exponent
        self._spread_cost = spread/2
        self._is_percent = spread_is_percentage
        self._is_volume_absolute = volume_is_absolute
        self._use_vol = use_vol
        self._last_dt = None
        self._vol = {}
        self._floor = vol_floor
    
    @classmethod
    def get_arguments(self):
        parent_list = super(VolumeSlippage, self).get_arguments()
        return ['cost_coeff','cost_exponent','spread',
                'spread_is_percentage', 'is_volume_absolute',
                'use_vol', 'vol_floor'] + parent_list
    
    cdef _calculate(self, Order order, object dt):
        cdef np.float64_t participation, impact, px, spread = 0
        cdef np.float64_t traded, available, max_tradeable = 0
        cdef double vega = 0, daily_move=0, delta=0, imp_vol=0
        
        if self._is_volume_absolute:
            # mult is the min size, so max volume is a number times the lot
            available = order.asset.mult
        else:
            if order.asset.instrument_type == InstrumentType.OPT:
                available = round(MAX_OPT_QTY*order.asset.mult/self._max_volume)
            elif order.asset.instrument_type == InstrumentType.FUTURES:
                available = round(MAX_OPT_QTY*order.asset.mult/self._max_volume)
            else:
                available = self._library.get_spot_value(
                        order.asset, dt, 'volume', self._freq)
        
        if not order.fractional:
            traded = min(round(self._max_volume*available), order.pending)
            max_tradeable = round(self._max_volume*available)
        else:
            traded = min(self._max_volume*available, order.pending)
            max_tradeable = self._max_volume*available
        
        if traded == 0:
            return 0, 0, max_tradeable
        if order.fractional and abs(traded) < ALMOST_ZERO:
            return 0, 0, max_tradeable
        
        vol=1
        if self._use_vol:
            vol = self._get_vol(order.asset, dt)
        
        if order.asset.instrument_type == InstrumentType.OPT:
            imp_vol = self._library.get_spot_value(
                    order.asset, dt, 'implied_vol', self._freq)
            vega = self._library.get_spot_value(
                    order.asset, dt, 'vega', self._freq)
            px = self._library.get_spot_value(
                    order.asset, dt, 'close', self._freq)
            
            if isnan(imp_vol):
                imp_vol = 0.15
                
            vega = vega*imp_vol*0.005 # a 0.5% vol bump for impact cost
            impact = spread + fabs(vega)
            if isnan(impact):
                impact = max(px*0.01, MIN_OPT_PRICE)
            
            return traded, impact, max_tradeable
        else:
            px = self._library.get_spot_value(
                    order.asset, dt, 'close', self._freq)
        
        if self._is_percent:
            spread = self._spread_cost*px
        else:
            spread = self._spread_cost
            
        participation = <np.float64_t>traded/available
        impact = self._cost_coeff*vol*participation**self._cost_exponent
        impact = spread + impact*px
        
        return traded, impact, max_tradeable
    
    cdef _get_vol(self, asset, timestamp):
        dt = timestamp.normalize()
        if self._last_dt is None or self._last_dt != dt:
            px = self._library.read(
                    asset, ['close'], self._lookback, '1d', timestamp)
            self._vol[asset] = px.pct_change().dropna().std().iloc[0]
            self._last_dt = px.index[-1].normalize()
            if np.isnan(self._vol[asset]):
                self._vol[asset] = self._floor
            else:
                self._vol[asset] = max(self._floor, self._vol[asset])
        
        return self._vol[asset]
    
cdef class FixedSlippage(VolumeSlippage):
    """
        Trade simulation based on OHLCV data with a fixed slippage. The
        max volume executed is capped at `max_volume` fraction of the 
        available volume at the bar. Slippage is half the spread.
        
        :param float spread: bid-ask spread (constant).
        :param float max_volume: maximum volume that can be executed at a bar.
    """
    def __init__(self, spread=0, max_volume=0.02, frequency=None, 
                 library=None):
        super(FixedSlippage, self).__init__(
                max_volume, cost_coeff=0.0, cost_exponent=1.0, spread=spread,
                spread_is_percentage=False,
                frequency=frequency, library=library)
        
    @classmethod
    def get_arguments(self):
        return ['spread','max_volume','frequency','library']
    
cdef class FixedBasisPointsSlippage(VolumeSlippage):
    """
        Trade simulation based on OHLCV data with a fixed slippage 
        expressed in basis points (100th of a percentage point). The
        max volume executed is capped at `max_volume` fraction of the 
        available volume at the bar. Slippage is half the spread. The 
        actual spread applied is arrived at as :math:`spread*close/10000`, 
        where :math:`close` is the close price at that bar.
        
        :param float spread: bid-ask spread (constant in basis points).
        :param float max_volume: maximum volume that can be executed at a bar.
    """
    def __init__(self, spread=0, max_volume=0.02, frequency=None, 
                 library=None):
        super(FixedBasisPointsSlippage, self).__init__(
                max_volume, cost_coeff=0.0, cost_exponent=1.0, 
                spread=spread, spread_is_percentage=True,
                frequency=frequency, library=library)
        self._spread_cost = self._spread_cost/10000
        
    @classmethod
    def get_arguments(self):
        return ['spread','max_volume','frequency','library']
    
cdef class NSESlippage(VolumeSlippage):
    """ 
        approximation of NSE slippage with volume slippage - with 
        100% participation.
    """
    def __init__(self, max_volume=1.0, cost_coeff=0.005, cost_exponent=0.5, 
                 spread=0.001, spread_is_percentage=True, 
                 volume_is_absolute=False, use_vol=False, 
                 vol_floor=0.05, frequency=None, library=None):
        super(NSESlippage, self).__init__(
                max_volume=max_volume, cost_coeff=cost_coeff, 
                cost_exponent=cost_exponent, spread=spread, 
                spread_is_percentage=True, volume_is_absolute=volume_is_absolute, 
                use_vol=use_vol, vol_floor=vol_floor, frequency=frequency, 
                library=library)

cdef class NYSESlippage(VolumeSlippage):
    """ approximation of NYSE slippage with volume slippage. """
    def __init__(self, max_volume=1.0, cost_coeff=0.005, cost_exponent=0.5, 
                 spread=0.001, spread_is_percentage=True, 
                 volume_is_absolute=False, use_vol=False, 
                 vol_floor=0.05, frequency=None, library=None):
        super(NYSESlippage, self).__init__(
                max_volume=max_volume, cost_coeff=cost_coeff, 
                cost_exponent=cost_exponent, spread=spread, 
                spread_is_percentage=True, volume_is_absolute=volume_is_absolute, 
                use_vol=use_vol, vol_floor=vol_floor, frequency=frequency, 
                library=library)

cdef class FXSlippage(BidAskSlippage):
    """ 
        Approximation of FX slippage with bid-ask slippage on a typical 
        FX trading platform. We assume standard lot-size is 100000. Any
        size till this is filled immedeately, and any size over this is 
        carried over to the next bar. Max volume, unlike in the parent 
        class, is **NOT** a fraction, but absolute amount of this standard 
        lot size.
    """
    cdef readonly int _std_lot
    
    def __init__(self, max_volume=100000, frequency=None, library=None):
        super(FXSlippage, self).__init__(
                max_volume, frequency, library)
        self._std_lot = int(max_volume)
        
    cdef _calculate(self, Order order, object dt):
        cdef np.float64_t bid, ask = 0
        cdef np.float64_t traded, available = 0
        
        traded = min(self._std_lot, order.pending)
        
        bid = self._library.get_spot_value(
                order.asset, dt, 'bid', self._freq)
        ask = self._library.get_spot_value(
                order.asset, dt, 'ask', self._freq)
        
        if math.isnan(bid) or math.isnan(ask):
            return 0, 0, self._std_lot
        
        return traded, (ask-bid)/2, self._std_lot
    
cdef class CryptoSlippage(VolumeSlippage):
    """ 
        Approximation of crypto slippage with fixed basis point slippage 
        on a typical trading platform. We assume max lot-size is 
        100000 times the minimum allowed quantity. Any size till this 
        is filled immedeately, and any size over this is carried over 
        to the next bar. Max volume, unlike in the parent class, 
        is **NOT** a fraction, but absolute amount of this standard 
        lot size.
    """
    cdef readonly int _std_lot
    
    def __init__(self, spread=50, max_volume=10000, frequency=None, 
                 library=None):
        super(CryptoSlippage, self).__init__(
                max_volume, cost_coeff=0.0, cost_exponent=1.0, 
                spread=spread, spread_is_percentage=True,
                volume_is_absolute=True, frequency=frequency, 
                library=library)
        self._spread_cost = self._spread_cost/10000
    
