# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
cimport numpy as np
import numpy as np

from blueshift.interfaces.assets._assets cimport InstrumentType, Asset
from blueshift.interfaces.trading._simulation cimport ABCCostModel
from blueshift.lib.trades._order cimport Order
from blueshift.lib.trades._position cimport Position
from blueshift.lib.trades._order_types cimport OrderSide, ProductType
from ._charges cimport ChargesModel, NoCharge

from blueshift.lib.common.types import MaxSizedOrderedDict
from blueshift.lib.exceptions import BrokerError

cdef np.float64_t MAX_CHARGE = 1E6
cdef int ORDERS_CACHE_SIZE = 100000
cdef int ORDERS_CHUNK_SIZE = 500

# NSE constants
cdef np.float64_t SLB_LENDING_RATE = 0.0003 # daily rate for 0.9% per month
cdef np.float64_t SLB_MARGIN = 1.25

# NYSE constants
cdef np.float64_t ETF_RATE = 0.0          # no cost
cdef np.float64_t EQUITY_RATE = 0.005     # per share
cdef np.float64_t EQUITY_CAP = 0.01       # per dollar
cdef np.float64_t EQUITY_FLOOR = 1        # per order
cdef np.float64_t FUTURES_RATE = 0.85     # per contract
cdef np.float64_t FUTURES_FLOOR = 1       # per order
cdef np.float64_t OPT_RATE = 0.65         # per contract
cdef np.float64_t OPT_FLOOR = 1           # per order
cdef np.float64_t BORROW_COST = 0.0002    # monthly 0.6% borrow costs

cdef class CostModel(ABCCostModel):
    r"""
            CostModel defines the broking commission/ brokerage cost 
            modelling (plus any exchange fees and/or tax/ regulatory 
            charges, which defaults to zero and cannot be modified 
            by the user). A concrete implementation must define the 
            `calculate` method.
            
            :param float commissions: Brokerage commission - interpretation depends on implementation.
            :param float cost_cap: Max possible brokerage.
            :param float cost_floor: Minimum brokerage levied.
            :param bool cost_on_sell_only: If brokerage only on sell leg.
            :param \**kwargs:
                For compatibility, see below.
                
            :Keyword Arguments:
                * *cost* (``float``) --
                  if supplied, overwrites the `commission` parameter.
                * *min_trade_cost* (``float``) --
                  if supplied, overwrites the `cost_floor` parameter.
                
    """
    
    def __init__(self, commissions=0, cost_cap=MAX_CHARGE, cost_floor=0,
                 cost_on_sell_only=False, charge_model=None,
                 *args, **kwargs):
        if 'cost' in kwargs and commissions == 0:
            commissions = kwargs['cost']
        if 'min_trade_cost' in kwargs and cost_floor==0:
            cost_floor=kwargs['min_trade_cost']
            
        self._commissions = commissions
        self._cap = cost_cap
        self._floor = cost_floor
        self._sell_only = cost_on_sell_only
        
        if charge_model is None:
            charge_model = NoCharge()
        self._chargemodel = charge_model
        
    def set_charge_model(self, charge_model):
        self._chargemodel = charge_model
    
    @classmethod
    def get_arguments(self):
        return ['charge_model', 'commissions','cost_cap','cost_floor',
                'cost_on_sell_only']
        
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
        """ Calculate the commission for the transaction. """
        raise NotImplementedError
    
    cpdef tuple rollcost(self, object positions):
        """ 
            Costs for rolling positions overnight. This is usually 
            applicable for rolling margin trading positions (e.g 
            Forex CFDs).
            
            :param dict position: Open positions.
            :return: A tuple of costs, margin.
            :rtype: (float, float)
            
            .. note::
                the input is a dictionary of current open positions - 
                keyed by the :ref:`assets <Asset>` and values are 
                :ref:`position <Position>` objects. The `cost` in the returned 
                tuple is the cost to charge for the roll, and `margin` 
                is the overnight margin to settle.
        """
        return 0, 0
        
cdef class NoCommission(CostModel):
    """ Zero commission and trading charges. """
    cdef double _calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t last_fx=1.0):
        return 0
    
cdef class PerDollar(CostModel):
    """ 
        Brokerage costs based on total value traded with cap and floor. 
        This is derived from `CostModel` and takes in the 
        same parameters. The parameter `commissions` (or `cost`) is 
        multiplied with the traded value (quantity times the price) 
        to determine the cost.
    """
        
    cdef double _calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t last_fx=1.0):
        cdef np.float64_t commissions = 0
        if self._sell_only and order.side == OrderSide.BUY:
            return 0
        
        commissions = self._commissions*quantity*price*last_fx
        return max(self._floor, min(self._cap, commissions))
    
cdef class PerShare(CostModel):
    """ 
        Brokerage costs based on total quantity traded with cap and floor.
        This is derived from `CostModel` and takes in the 
        same parameters. The parameter `commissions` (or `cost`) is 
        multiplied with the traded quantity to determine the cost.
        
    """        
    cdef double _calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t last_fx=1.0):
        cdef np.float64_t commissions = 0
        
        if self._sell_only and order.side == OrderSide.BUY:
            return 0
        
        commissions = self._commissions*quantity
        return max(self._floor, min(self._cap, commissions))
    
cdef class PerOrder(CostModel):
    """ 
        Flat brokerage costs per order. This is derived from `CostModel` 
        and takes in the same parameters. The parameter `commissions` 
        (or `cost`) is the flat rate per order. If an order results in 
        multiple trades (corresponding to multiple fills), the charge 
        is applied only once (per order).
    """
    cdef readonly object _order_cache
    
    def __init__(self, commissions=0, cost_cap=MAX_CHARGE, cost_floor=0,
                 cost_on_sell_only=False, charge_model=None):
        super(PerOrder, self).__init__(
                commissions, cost_cap, cost_floor, 
                cost_on_sell_only, charge_model)
        
        self._order_cache = MaxSizedOrderedDict(
                max_size=ORDERS_CACHE_SIZE, chunksize=ORDERS_CHUNK_SIZE)
        
    cdef double _calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t last_fx=1.0):
        if self._sell_only and order.side == OrderSide.BUY:
            return 0
        
        if order.oid in self._order_cache:
            return 0
        
        self._order_cache[order.oid] = True
        return max(self._floor, min(self._cap, self._commissions))
    
cdef class NSECommission(CostModel):
    """ 
        Typical NSE discount brokerage costs. Brokerage is 0 for 
        equity delivery trades. Brokerage is 3bps, capped at INR 20
        for everything else (including intraday equities).
    """
    cdef readonly object _order_cache
    
    def __init__(self, commissions=0, cost_cap=MAX_CHARGE, cost_floor=0,
                 cost_on_sell_only=False, charge_model=None):
        super(NSECommission, self).__init__(
                commissions, cost_cap, cost_floor, 
                cost_on_sell_only, charge_model)
        
        self._order_cache = MaxSizedOrderedDict(
                max_size=ORDERS_CACHE_SIZE, chunksize=ORDERS_CHUNK_SIZE)
        
    cdef double _calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t last_fx=1.0):
        cdef np.float64_t brokerage, charge, remaining = 0
        
        if order.asset.is_funded() and \
            order.asset.product_type == ProductType.DELIVERY:
            return 0
        
        if order.oid in self._order_cache and \
            self._order_cache[order.oid] >= self._cap:
            return 0
        
        brokerage = self._commissions*quantity*price*last_fx
        brokerage = max(self._floor, brokerage)
        
        if order.oid in self._order_cache:
            remaining = self._cap - self._order_cache[order.oid]
            charge = min(remaining, brokerage)
            self._order_cache[order.oid] += charge
        else:
            charge = min(self._cap, brokerage)
            self._order_cache[order.oid] = charge
        
        return charge
    
    cpdef tuple rollcost(self, object positions):
        # there are no rollover costs for long equity positions, as well 
        # long/ short F&O positions. In case of short equity positions,
        # we assume a flat 0.09% monthly lending fees for SLB and a 
        # margin requirements of 125%
        cdef np.float64_t charges=0, margins = 0
        cdef Asset asset
        cdef Position pos
        
        for asset in positions:
            pos = positions[asset]
            if pos.is_funded():
                continue
            
            charges += (abs(pos.get_exposure())*SLB_LENDING_RATE)
            
        return charges, margins
    
cdef class NYSECommission(CostModel):
    """ 
        Typical US discount brokerage costs. Brokerage is USD 0.005 for 
        equity trades (per share), which is capped at 1% of trade 
        value and floored at USD 1 per order. For futures, commission 
        per contract is USD 0.85. For options, USD 0.65 per contract, 
        floored at USD 1 per order. ETFs has no fees. All are assumed 
        to include any exchange and transaction fees.
        
        See https://www.interactivebrokers.co.in/en/index.php?f=39753&p=stocks
    """
    cdef readonly object _order_cache
    
    def __init__(self, commissions=0, cost_cap=MAX_CHARGE, cost_floor=0,
                 cost_on_sell_only=False, charge_model=None):
        super(NYSECommission, self).__init__(
                commissions, cost_cap, cost_floor, 
                cost_on_sell_only, charge_model)
        
        self._order_cache = MaxSizedOrderedDict(
                max_size=ORDERS_CACHE_SIZE, chunksize=ORDERS_CHUNK_SIZE)
        
    cdef double _calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t last_fx=1.0):
        cdef np.float64_t floor, cap, charge, brokerage, remaining = 0
        cdef ncontracts = 0
        
        if order.asset.instrument_type == InstrumentType.FUNDS:
            charge=ETF_RATE*quantity
        elif order.asset.instrument_type == InstrumentType.SPOT:
            floor = EQUITY_FLOOR
            cap = EQUITY_CAP*order.filled*order.average_price*last_fx
            charge = EQUITY_RATE*quantity
        elif order.asset.instrument_type == InstrumentType.FUTURES:
            floor = FUTURES_FLOOR
            cap = MAX_CHARGE
            ncontracts = int(quantity/order.asset.mult)
            charge = FUTURES_RATE*ncontracts
        elif order.asset.instrument_type == InstrumentType.OPT:
            floor = OPT_FLOOR
            cap = MAX_CHARGE
            ncontracts = int(quantity/order.asset.mult)
            charge = OPT_RATE*ncontracts
        
        if order.oid in self._order_cache and \
            self._order_cache[order.oid] >= cap:
            return 0
        
        brokerage = max(floor, charge)
        
        if order.oid in self._order_cache:
            remaining = cap - self._order_cache[order.oid]
            charge = min(remaining, brokerage)
            self._order_cache[order.oid] += charge
        else:
            charge = min(cap, brokerage)
            self._order_cache[order.oid] = charge
        
        return charge
    
    cpdef tuple rollcost(self, object positions):
        # there are no rollover costs for long equity positions, as well 
        # long/ short F&O positions. In case of short equity positions,
        # we assume a flat 0.05% monthly borrowing costs and no interest
        # on margin
        cdef np.float64_t charges=0, margins = 0
        cdef Asset asset
        cdef Position pos
        
        for asset in positions:
            pos = positions[asset]
            if pos.is_funded():
                continue
            
            charges += (abs(pos.get_exposure())*BORROW_COST)
            
        return charges, margins
    
cdef class FXCommission(CostModel):
    """ 
        Typical brokerage costs for FX trading platform. All costs are 
        assumed to be included in the bid-ask spread. This cost is 
        automatically captured in the slippage simulation.
    """        
    cdef double _calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t last_fx=1.0):
        return 0
    
    cpdef tuple rollcost(self, object positions):
        cdef np.float64_t total=0, roll = 0
        cdef Asset asset
        cdef Position pos
        
        for asset in positions:
            pos = positions[asset]
            if pos.value > 0:
                roll = getattr(asset, 'buy_roll', 0)
            else:
                roll = getattr(asset, 'sell_roll', 0)
                
            total += abs(roll*positions[asset].quantity)
                
        return total, 0
    
cdef class PipCost(FXCommission):
    """ 
        Brokerage costs based on total quantity traded with cap and floor.
        This is derived from `CostModel` and takes in the 
        same parameters. The parameter `commissions` (or `cost`) is 
        multiplied with the traded quantity to determine the cost. In 
        addition, this also implements the `rollcost` method to calculate 
        the overnight funding cost of carrying over the position.
        
        .. note::
            This cost model is suitable for Forex assets only.
    """        
    cdef double _calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t last_fx=1.0):
        cdef np.float64_t commissions = 0
        
        if self._sell_only and order.side == OrderSide.BUY:
            return 0
        
        commissions = self._commissions*quantity
        return max(self._floor, min(self._cap, commissions))
    
cdef class PipsCost(PipCost):
    """ 
        PipCost version for backward compatibility.
        
        .. note::
            This cost model is suitable for Forex assets only.
    """        
    pass