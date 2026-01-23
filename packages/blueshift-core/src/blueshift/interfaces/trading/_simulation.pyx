# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
import numpy as np
cimport numpy as np

from blueshift.interfaces.assets._assets cimport InstrumentType, Asset
from blueshift.lib.trades._order cimport Order
from blueshift.lib.trades._position cimport Position
from blueshift.lib.trades._order_types cimport OrderSide, ProductType

from blueshift.lib.common.functions import listlike


cdef class ABCSlippageModel:
    """ 
        Slippage models implement the order fill in backtest simulation. The core 
        method `simulate` takes in an order and simulates the fill and the fill price.
    """ 
    cpdef tuple simulate(self, Order order:Order, object dt):
        """
            Takes in an order (may be already partially filled), and 
            returns a trade with executed price and amount, as well 
            as the fair mid. The difference between the execution price
            and the fair mid is the slippage.
            
            :param order: Order object to process.
            :type order: :ref:`order<Order>` object.
            :param pandas.Timestamp dt : Timestamp for the order.
            :return: A tuple of volume, price, fair mid and max volume.
            :rtype: (float, float, float, float)
        """
        raise NotImplementedError

cdef class ABCChargesModel:
    """ 
        Charge models implement the non-broking charges in backtest simulation. The core 
        method `calculate` takes in an order fill details and returns the calculated charges.
    """ 
    @classmethod
    def get_arguments(self) -> list[str]:
        raise NotImplementedError
    
    cpdef double calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t commission=0, np.float64_t last_fx=1.0):
        """
            Calculate the non-broking charges for the transaction. The commission 
            (calculated beforehand) is an input as this may be required for tax 
            and other surcharges computation.
            
            Note:
                Charges are fees charged by the execution venue (e.g. 
                the exchange) that may include any exchange fees and 
                other government or regulatory charges.
                
            :param order: Order object to process.
            :type order: :ref:`order<Order>` object.
            :param float quantity: Traded amount.
            :param float price: Traded price.
            :param float commission: Commission charged on this trade.
            :param float last_fx: FX rate for conversion (defaults to 1.0).
            :return: charges for this transaction.
            :rtype: float
        """
        raise NotImplementedError

cdef class ABCCostModel:
    """
        CostModel defines the broking commission/ brokerage cost 
        modelling (plus any exchange fees and/or tax/ regulatory 
        charges, which defaults to zero and cannot be modified 
        by the user). A concrete implementation must define override 
        the `calculate` method or define the `_calculate` method.
                
    """
    @classmethod
    def get_arguments(self) -> list[str]:
        raise NotImplementedError
        
    cpdef tuple calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t last_fx=1.0):
        """
            Calculate the commission and charges for the transaction.
            Commission is the fees deducted by the broker for this 
            particular transaction. Charges are any exchange fees 
            and/ or government or regulatory charges on top.
            
            :param order: Order object to process.
            :type order: :ref:`order<Order>` object.
            :param float quantity: Traded amount.
            :param float price: Traded price.
            :param float last_fx: FX rate for conversion (defaults to 1.0).
            :return: A tuple of commission, charges.
            :rtype: (float, float)
        
        """
        cdef np.float64_t commissions, charges = 0
        
        commissions = self._calculate(order, quantity, price, last_fx)
        charges = self._chargemodel.calculate(
                order, quantity, price, commissions, last_fx)
        
        return commissions, charges

    cdef double _calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t last_fx=1.0):
        """ 
            Calculate only the commission part for the transaction. 

            :param order: Order object to process.
            :type order: :ref:`order<Order>` object.
            :param float quantity: Traded amount.
            :param float price: Traded price.
            :param float last_fx: FX rate for conversion (defaults to 1.0).
            :return: A tuple of commission, charges.
            :rtype: float
        """
        raise NotImplementedError

    cpdef tuple rollcost(self, object positions):
        """ 
            Return the required margin and cost (tuple) for rolling the positions. 

            :param positions: Current open positions.
            :type dict: A dictionary dict[Asset:Position]
            :return: A tuple of margins, roll costs.
            :rtype: (float, float)
        """
        raise NotImplementedError

cdef class ABCMarginModel:
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
    @classmethod
    def get_arguments(self) -> list[str]:
        raise NotImplementedError
    
    @classmethod
    def supported_products(self) -> list[ProductType]:
        return [ProductType.DELIVERY, ProductType.MARGIN, ProductType.INTRADAY]
        
    def get_trading_margins(self, orders:dict[Asset:Order]|list[Order], positions:dict[Asset, Position], 
                        *args, **kwargs) -> tuple[bool, float]:
        """ 
            Returns the required margin for the orders, given the existing positions.
            Returns a (bool, float). The first element is True if the margin reported 
            is incremental, the second element is the required margin. If incremental 
            is True, the margin computed is the extra required on top of existing margin.
            If it is False, the margin is the current total margin.

            :param orders: orders to compute the margins for.
            :type dict: A dictionary dict[Asset:Order]
            :param positions: existing open positions
            :type dict: A dictionary dict[Asset:Position]
            :return: A tuple of incremental flag, margin.
            :rtype: (bool, float)
        """
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
            :param pos: Position object capturing current position.
            :type pos: :ref:`position<Position>` object.
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
            :param pos: Position object to process.
            :type pos: :ref:`position<Position>` object.
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
            :param pos: Position object to process.
            :type pos: :ref:`position<Position>` object.
            :param pandas.Timestamp timestamp: Current timestamp.
            :param float last_fx: FX rate for conversion.
            :return: margin change.
            :rtype: float
            
        """
        raise NotImplementedError