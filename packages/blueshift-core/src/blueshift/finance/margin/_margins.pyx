# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
from libc.math cimport fmax, fmin, fabs
cimport numpy as np
import numpy as np

from blueshift.interfaces.assets._assets cimport InstrumentType, Asset
from blueshift.interfaces.trading._simulation cimport ABCMarginModel
from blueshift.lib.trades._order cimport Order
from blueshift.lib.trades._position cimport Position
from blueshift.lib.trades._order_types cimport OrderSide, ProductType

from blueshift.lib.common.constants import ALMOST_ZERO
from blueshift.lib.common.functions import  listlike

cdef class MarginModel(ABCMarginModel):
    """
        Margin model calculates margins required (or released) for an 
        order execution. For a fully funded trade, (e.g. cash equities)
        the margin is 0. For margin trading or derivatives (futures and 
        options), each trade adds to or releases a certain margin. Margins 
        are charged from the available cash in the portfolio. On release, 
        it is added back to portfolio cash. Each order, before execution, 
        is checked for sufficient funds in the account. If sufficient funds
        are not available, the order is rejected.
        
        All margin models derive from this class and concretely 
        implement two methods, `calculate` and `exposure_margin`. The 
        method `calculate` is used to determine margin requirements for 
        a transaction. The method `exposure_margin` is used to calculate 
        daily MTM settlements at the end-of-day calculation.
        
        .. note::
            Selection of margin models has no impact on fully funded 
            assets (e.g. cash equities).
            
    """
    def __init__(self, library=None):
        self._library = library
        self.margins_collected = 0
        self.position_wise_margins = {}
    
    @classmethod
    def get_arguments(self):
        return ['library']
    
    @classmethod
    def supported_products(self):
        return [ProductType.DELIVERY, ProductType.MARGIN, ProductType.INTRADAY]
    
    def set_library(self, library):
        self._library = library
        
    def get_trading_margins(self, orders, positions, *args, **kwargs):
        timestamp = kwargs.get('timestamp', None)
        
        if not positions:
            positions = {}
        
        positions = positions.copy()
        
        for asset in positions:
            # create a copy to modify
            positions[asset] = positions[asset].copy()
            
        if orders:
            if isinstance(orders, dict):
                # it is an ID keyed dict, get the values
                orders = list(orders.values())
            if not listlike(orders):
                orders = [orders]
                
            for o in orders:
                pos = Position.from_order(o)
                if pos.asset in positions:
                    positions[pos.asset].add_to_position(pos)
                else:
                    positions[pos.asset] = pos
        
        margins = 0
        for asset, pos in positions.items():
            underlying_px = 0
            timestamp = timestamp or pos.timestamp
            exposure = pos.quantity*pos.last_price*pos.last_fx
            
            if asset.instrument_type == InstrumentType.OPT:
                underlying_px = self.library.get_spot_value(
                        o.asset, timestamp, 'atmf')
            
            margins += self.exposure_margin(
                    asset, exposure, pos, timestamp, last_fx=pos.last_fx, 
                    underlying_px=pos.underlying_price)
            
        return False, max(0, margins)
    
    cpdef tuple calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t position, object timestamp, np.float64_t last_fx=1.0, 
                    np.float64_t underlying_px=0, Position pos=None):
        """
            Calculate the cashflow and margin for a transaction. The 
            algo account must have sufficient cash to cover for the 
            transaction to happen.
            
            :param order: Order object to process.
            :type order: :ref:`order<Order>` object.
            :param float quantity: Amount to trade.
            :param float price: Traded price.
            :param float position: Current position in the asset with sign.
            :param pandas.Timestamp timestamp: Current timestamp.
            :param float last_fx: FX rate for conversion.
            :param float underlying_px: Underlying price for option.
            :param pos: Position object to process.
            :type pos: :ref:`position<Position>` object.
            :return: A tuple of cashflow, margin.
            :rtype: (float, float)
            
        """
        cdef int side = 0
        cdef np.float64_t cashflow, margin, traded = 0
        
        side = 1 if order.side == OrderSide.BUY else -1
        traded = quantity*side
        cashflow = quantity*price*side*last_fx
        
        if order.asset.instrument_type in (InstrumentType.SPOT, InstrumentType.FUNDS) and \
            order.product_type == ProductType.DELIVERY:
            return cashflow, 0
        elif order.asset.instrument_type == InstrumentType.OPT:           
            margin = self._opt_calculate(
                    order, quantity, price, position, pos, timestamp, 
                    last_fx, underlying_px)
        else:
            # return 0 cash flow, we adjust the mtm settlement at a 
            # global level on each position update
            cashflow = 0            
            margin = self._calculate(
                    order, quantity, price, position, pos, timestamp, 
                    last_fx, underlying_px)
        
        return cashflow, margin
    
    cpdef double exposure_margin(self, Asset asset, np.float64_t exposure,
                          Position pos, object timestamp, np.float64_t last_fx=1.0, 
                          np.float64_t underlying_px=0.0):
        # last_fx not needed as exposure is fx adjusted.
        """
            Compute the exposure margin, given an asset and position in 
            that asset. This is used for end-of-day mark-to-market 
            settlement computation and is settled against the account. 
            If the account is short of the required cash, the account is
            frozen for further trading.
            
            :param asset: Asset in which margin to calculate.
            :type asset: :ref:`asset<Asset>` object.
            :param float exposure: current exposure in the asset.
            :param pandas.Timestamp timestamp: current timestamp
            :return: exposure margin
            :rtype: float
        """
        raise NotImplementedError
        
    cdef double _calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t position, Position pos, object timestamp, 
                    np.float64_t last_fx=1.0, 
                    np.float64_t underlying_px=0.0):
        """
            Calculate incremental margin for a transaction. Positive 
            number is a charge, negative is a margin release.
            
            :param order: Order object to process.
            :type order: :ref:`order<Order>` object.
            :param float quantity: Amount to trade.
            :param float price: Traded price (or underlying price for detivatives).
            :param float position: Current position in the asset with sign.
            :param pandas.Timestamp timestamp: Current timestamp.
            :param float last_fx: FX rate for conversion.
            :return: margin change.
            :rtype: float
            
        """
        raise NotImplementedError
        
    cdef double _opt_calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t position, Position pos, object timestamp, 
                    np.float64_t last_fx=1.0, 
                    np.float64_t underlying_px=0.0):
        """
            Calculate incremental margin for an option transaction. 
            Positive number is a charge, negative is a margin release.
            
            :param order: Order object to process.
            :type order: :ref:`order<Order>` object.
            :param float quantity: Amount to trade.
            :param float price: Traded price (or underlying price for detivatives).
            :param float position: Current position in the asset with sign.
            :param pandas.Timestamp timestamp: Current timestamp.
            :param float last_fx: FX rate for conversion.
            :return: margin change.
            :rtype: float
            
        """
        raise NotImplementedError
        
cdef class NoMargin(MarginModel):
    """ 
        No margin. This model allows any amount of leverage for 
        unfunded products (e.g. forex or derivatives).
    """
    
    cdef double _calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t position, Position pos, object timestamp, 
                    np.float64_t last_fx=1.0,
                    np.float64_t underlying_px=0.0):
        return 0
    
    cdef double _opt_calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t position, Position pos, object timestamp, 
                    np.float64_t last_fx=1.0,
                    np.float64_t underlying_px=0.0):
        return 0
    
    @classmethod
    def supported_products(self):
        return [ProductType.DELIVERY]
    
    cpdef double exposure_margin(self, Asset asset, np.float64_t exposure,
                          Position pos, object timestamp,
                          np.float64_t last_fx=1.0, 
                          np.float64_t underlying_px=0.0):
        # last_fx not needed as exposure is fx adjusted.
        return 0
    
cdef class FlatMargin(MarginModel):
    """ 
        Flat margin on total exposure as percentage. The parameter 
        `intial_margin` is used to compute the fraction of the exposure 
        required to be posted as margin for a transaction. This means 
        `1/margin` is the leverage offered. The default value is 
        10% (i.e. 10x leverage). The parameter `maintenance_margin` is 
        applied for computation of margin required for carrying 
        an overnight position. It defaults to the same value as the `initial_margin` - 
        that is, no extra margin is charged for overnight positions.
        
        :param float intial_margin: Initial margin for a trade.
        :param float maintenance_margin: Maintenance margin.
    """
    cdef readonly np.float64_t _margin
    cdef readonly np.float64_t _maintenance_margin
    
    def __init__(self, intial_margin=0.10, maintenance_margin=None, 
                 library=None):
        super(FlatMargin, self).__init__(library)
        self._margin= intial_margin
        
        if maintenance_margin is None:
            self._maintenance_margin= intial_margin
        else:
            self._maintenance_margin= maintenance_margin
    
    @classmethod
    def get_arguments(self):
        parent_list = super(FlatMargin, self).get_arguments()
        return ['intial_margin','maintenance_margin'] + parent_list
    
    @classmethod
    def supported_products(self):
        return [ProductType.MARGIN]
    
    cdef double _calculate(self,Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t position, Position pos, object timestamp, 
                    np.float64_t last_fx=1.0,
                    np.float64_t underlying_px=0.0):
        """
            Calculate the flat margin for the transaction. The parameter 
            `current_exposure` is the current net exposure for the 
            asset. In case of options, the exposure is the equivalent 
            underlying exposure.
        """
        # last_fx NOT used as exposure already comes FX-corrected.
        cdef int sign = 0
        cdef np.float64_t old_exposure, new_exposure = 0
        cdef np.float64_t old_margin,new_margin,change = 0
        cdef np.float64_t mtm_settlement, remaining_mtm = 0
        
        sign = 1 if order.side == OrderSide.BUY else -1
        quantity = abs(quantity)*sign
        
        if pos is None:
            # if position is None, we assume a new position being open,
            # that means, old_exposure should be 0, but we use it 
            # anyways
            old_exposure = position*price*last_fx
            new_exposure = (position + quantity)*price*last_fx
            old_margin = abs(old_exposure)*self._margin
            new_margin = abs(new_exposure)*self._margin
            change = new_margin - old_margin
        else:
            if pos.quantity + quantity == 0:
                # release all margin on full unwind
                return -pos.margin
            if order.fractional and abs(pos.quantity + quantity) < ALMOST_ZERO:
                return -pos.margin
            
            new_exposure = (pos.quantity + quantity)*price*last_fx
            new_margin = abs(new_exposure)*self._margin
            mtm_settlement = pos.get_mtm_settlement(
                    quantity, price, last_fx)
            remaining_mtm = pos.unrealized_pnl + mtm_settlement
            new_margin = new_margin + max(0,-remaining_mtm)
            change = new_margin - pos.margin
        
        return change
    
    cdef double _opt_calculate(self,Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t position, Position pos, object timestamp, 
                    np.float64_t last_fx=1.0,
                    np.float64_t underlying_px=0.0):
        """
            Calculate the flat margin for the transaction. The parameter 
            `current_exposure` is the current net exposure for the 
            asset. In case of options, the exposure is the equivalent 
            underlying exposure for all strikes and maturities - this is 
            more conservative than a typical SPAN margin calculation.
        """
        # last_fx NOT used as exposure already comes FX-corrected.
        cdef int sign = 0
        cdef np.float64_t old_exposure, new_exposure = 0
        cdef np.float64_t old_margin,new_margin,change = 0
        cdef np.float64_t mtm_settlement, remaining_mtm = 0
        
        sign = 1 if order.side == OrderSide.BUY else -1
        quantity = abs(quantity)*sign
        
        if pos is None:
            # if position is None, we assume a new position being open,
            # that means, old_exposure should be 0, but we use it 
            # anyways. For option, opening a new buy position has 
            # zero margin and only cashflow equal to the premium to 
            # be paid. For sell, charge premium based on the underlying
            # exposure adjusted (delta equivalent)
            if sign == 1:
                return 0
            new_exposure = (position + quantity)*underlying_px*last_fx
            # charge only if the change is negative
            new_margin = -fmin(new_exposure*self._margin, 0)
            change = new_margin
        else:
            if pos.quantity + quantity == 0:
                # release all margin on full unwind
                return -pos.margin
            if order.fractional and abs(pos.quantity + quantity) < ALMOST_ZERO:
                return -pos.margin
            
            new_exposure = (position + quantity)*underlying_px*last_fx
            # charge only if the change is negative
            new_margin = -fmin(new_exposure*self._margin, 0)
            mtm_settlement = pos.get_mtm_settlement(
                    quantity, price, last_fx)
            remaining_mtm = pos.unrealized_pnl + mtm_settlement
            new_margin = new_margin + max(0,-remaining_mtm)
            change = new_margin - pos.margin
        
        return change
    
    cpdef double exposure_margin(self, Asset asset, np.float64_t exposure, 
                          Position pos, object timestamp,
                          np.float64_t last_fx=1.0, 
                          np.float64_t underlying_px=0.0):
        if pos is not None:
            if asset.instrument_type in (InstrumentType.SPOT, InstrumentType.FUNDS) and \
                pos.product_type == ProductType.DELIVERY:
                return 0
        
        # last_fx not needed as exposure is fx adjusted.
        cdef np.float64_t margin = 0
        if asset.instrument_type == InstrumentType.OPT:
            if exposure < 0:
                pass
            else:
                return -fmin(exposure*self._maintenance_margin,0)
        
        margin = abs(exposure)*self._maintenance_margin
        
        if pos is not None:
            margin = margin + max(0, -pos.unrealized_pnl)
        
        return margin

    
cdef class VarMargin(MarginModel):
    """ 
        Value-at-risk margin on total exposure - also known as 
        portfolio margin. On blueshift, this is applied to each 
        asset separately based on a multiplier of recent historical 
        volatility. The formula is `max(vol, vol_floor)*vol_factor`.
        The default value of `vol_factor` is 3 (approximating a 99% 
        VaR for normally distributed returns). The parameter 
        `vol_floor` defaults to 0.05 (i.e. a minimum 5% volatility).
        The default value of lookback for historical volatility 
        computation is 20. Volatilities are computed based on daily 
        returns (daily volatility).
        
            
        :param float vol_factor: Factor (z-score) for the probability threshold.
        :param float vol_floor: Minimum volatility level (percentage).
        :param float vol_lookback: Lookback to compute historical volatility.
    """
    
    def __init__(self, vol_factor=3.0, vol_floor=0.05, vol_lookback=20,
                 library=None):
        super(VarMargin, self).__init__(library)
        self._factor = vol_factor
        self._floor = vol_floor
        self._lookback = 20
        self._last_dt = None
        self._vol = {}
    
    @classmethod
    def get_arguments(self):
        parent_list = super(VarMargin, self).get_arguments()
        return ['vol_factor','vol_floor','vol_lookback'] + parent_list
    
    cdef double _calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t position, Position pos, object timestamp, 
                    np.float64_t last_fx=1.0,
                    np.float64_t underlying_px=0.0):
        """
            Calculate the var margin for the transaction. The parameter 
            `current_exposure` is the current net exposure for the 
            asset.
        """
        # last_fx NOT used as exposure already comes FX-corrected
        cdef int sign = 0
        cdef np.float64_t margin, old_margin, new_margin, change = 0
        cdef np.float64_t old_exposure, new_exposure, volatility = 0
        cdef np.float64_t mtm_settlement, remaining_mtm = 0
        cdef Asset asset
        
        asset = order.asset
        if asset.instrument_type in (InstrumentType.OPT, InstrumentType.FUTURES):
            asset = self._library.symbol(asset.underlying)
        
        # price must be the underlying price in case of options
        volatility = self._get_vol(asset, timestamp)
        margin = volatility*self._factor
        sign = 1 if order.side == OrderSide.BUY else -1
        quantity = abs(quantity)*sign
        
        if pos is None:
            old_exposure = position*price*last_fx
            old_margin = abs(old_exposure)*margin
            new_exposure = (position + quantity)*price*last_fx
            new_margin = abs(new_exposure)*margin
            change = new_margin - old_margin
        else:
            if pos.quantity + quantity == 0:
                # release all margin on full unwind
                return -pos.margin
            if order.fractional and abs(pos.quantity + quantity) < ALMOST_ZERO:
                return -pos.margin
            
            new_exposure = (pos.quantity + quantity)*price*last_fx
            new_margin = abs(new_exposure)*self._margin
            mtm_settlement = pos.get_mtm_settlement(
                    quantity, price, last_fx)
            remaining_mtm = pos.unrealized_pnl + mtm_settlement
            new_margin = new_margin + max(0,-remaining_mtm)
            change = new_margin - pos.margin
        
        return change
    
    cdef double _opt_calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t position, Position pos, object timestamp, 
                    np.float64_t last_fx=1.0,
                    np.float64_t underlying_px=0.0):
        raise NotImplementedError(f'Option margining not implemented for this model.')
    
    cpdef double exposure_margin(self, Asset asset, np.float64_t exposure,
                          Position pos, object timestamp,
                          np.float64_t last_fx=1.0, 
                          np.float64_t underlying_px=0.0):
        if pos is not None:
            if asset.instrument_type in (InstrumentType.SPOT, InstrumentType.FUNDS) and \
                pos.product_type == ProductType.DELIVERY:
                return 0
        
        # last_fx not needed as exposure is fx adjusted.
        cdef np.float64_t margin, volatility = 0
        
        if asset.instrument_type in (InstrumentType.OPT, InstrumentType.FUTURES):
            asset = self._library.symbol(asset.underlying)
        
        # price must be the underlying price in case of options
        volatility = self._get_vol(asset, timestamp)
        margin = volatility*self._factor
        margin = abs(exposure)*margin
        
        if pos is not None:
            margin = margin + max(0, -pos.unrealized_pnl)
        
        return margin
    
    cdef double _get_vol(self, Asset asset, object timestamp):        
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
            
    
cdef class NSEMargin(FlatMargin):
    """ NSE margining approximated as flat margin. """
    @classmethod
    def supported_products(self):
        return [ProductType.DELIVERY, ProductType.MARGIN, ProductType.INTRADAY]

cdef class RegTMargin(FlatMargin):
    """ 
        RegT margin. This is derived from `FlatMargin` class with 
        set values of `intial_margin` at 0.5 (50%) and 
        `maintenance_margin` the same as the `initial_margin` for carrying an 
        overnight position. The default product type is margin.
    """
    def __init__(self, intial_margin=0.50, maintenance_margin=None, 
                 library=None):
        super(RegTMargin, self).__init__(
                intial_margin=intial_margin, 
                maintenance_margin=maintenance_margin,
                library=library)
        
    @classmethod
    def supported_products(self):
        return [ProductType.MARGIN, ProductType.DELIVERY]

cdef class NYSEMargin(VarMargin):
    """ NYSE margining approximated as VaR based. """
    pass

cdef class FXMargin(FlatMargin):
    """ 
        Forex margining approximated as flat margin.
    """
    pass
        
