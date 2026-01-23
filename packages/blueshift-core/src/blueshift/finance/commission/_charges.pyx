# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
cimport numpy as np
import numpy as np

from blueshift.interfaces.assets._assets cimport InstrumentType
from blueshift.interfaces.trading._simulation cimport ABCCostModel
from blueshift.lib.trades._order cimport Order
from blueshift.lib.trades._order_types cimport OrderSide, ProductType

from blueshift.lib.common.types import MaxSizedOrderedDict, OnetoOne
from blueshift.lib.exceptions import BrokerError

cdef np.float64_t MAX_CHARGE = 1E6
cdef int ORDERS_CACHE_SIZE = 100000
cdef int ORDERS_CHUNK_SIZE = 500

# NSE constants
cdef np.float64_t STT_EQUITY=0.001                # on both buy and sell
cdef np.float64_t STT_EQUITY_INTRADAY = 0.00025   # on sell side only
cdef np.float64_t STT_FUT = 0.0001                # on sell side only
cdef np.float64_t STT_OPT = 0.0005                # on sell side only on premium
cdef np.float64_t TXNS_CHARGES_EQUITY = 3.45e-05  # percent of trade value
cdef np.float64_t TXNS_CHARGES_FUT = 2e-05        # percent of trade value
cdef np.float64_t TXNS_CHARGES_OPT = 0.00053      # percent of premiumn
cdef np.float64_t GST = 0.18                      # on brokerage + txns charges
cdef np.float64_t SEBI = 5e-07                    # INR 5 per `crore` (10 million)
cdef np.float64_t STAMP_DUTY = 0.00015            # 'INR 1500 per `crore`

# NYSE constants
TAPE_A_B_C = 0.003

cdef class ChargesModel:
    
    def __init__(self, 
                 np.float64_t charges=0, 
                 np.float64_t charge_cap=MAX_CHARGE, 
                 np.float64_t charge_floor=0,
                 bool charge_on_sell_only=False):
        """
            Interface for models for non-broking charges. The use of the
            arguments depends on the particular implementation.
            
            Args:
                `charge_cap (float)`: Charge for this model.
                
                `cap (float)`: Max possible charge.
                
                `charge_floor (float)`: Minimum charge levied.
                
                `charge_on_sell_only (bool)`: If charged only on sell leg.
        """
        self._charges = charges
        self._cap = charge_cap
        self._floor = charge_floor
        self._sell_only = charge_on_sell_only
    
    @classmethod
    def get_arguments(self):
        return ['charges','charge_cap','charge_floor',
                'charge_on_sell_only']
    
    cpdef double calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t commission=0, np.float64_t last_fx=1.0):
        """
            Calculate the commission for the transaction.
            
            Note:
                Charges are fees charged by the execution venue (e.g. 
                the exchange) that may include any exchange fees and 
                other government or regulatory charges.
                
            Args:
                `order (obj)`: Order object to process.
                
                `quantity (float)`: Traded amount.
                
                `price (float)`: Traded price.
                
                `commission (float)`: Commission charged on this trade.
                
                `last_fx (float)`: FX rate for conversion.
                
            Returns:
                Float, charges for this transaction.
        """
        raise NotImplementedError
        
cdef class NoCharge(ChargesModel):
    """ Zero trading charges. """
    cpdef double calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t commission=0, np.float64_t last_fx=1.0):
        return 0
    
cdef class DollarCharge(ChargesModel):
    """ 
        Trading charges based on total value traded with cap and floor. 
        Set `sell_only` to True if charges are applicable for only 
        sell side.
    """        
    cpdef double calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t commission=0, np.float64_t last_fx=1.0):
        cdef np.float64_t charges = 0
        
        if self._sell_only and order.side == OrderSide.BUY:
            return 0
        
        charges = self._charges*quantity*price*last_fx
        return max(self._floor, min(self._cap, charges))
    
cdef class PerShareCharge(ChargesModel):
    """ 
        Charges based on total quantity traded with cap and floor. 
        Set `sell_only` to True if charges are applicable for only 
        sell side.
    """        
    cpdef double calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t commission=0, np.float64_t last_fx=1.0):
        cdef np.float64_t charges = 0
        
        if self._sell_only and order.side == OrderSide.BUY:
            return 0
        
        charges = self._charges*quantity
        return max(self._floor, min(self._cap, charges))
    
cdef class PerOrderCharge(ChargesModel):
    """ 
        Flat trading charges per order. Set `sell_only` to True if 
        charges are applicable for only sell side.
    """
    cdef readonly object _order_cache
    
    def __init__(self,  
                 np.float64_t charges=0, 
                 np.float64_t charge_cap=MAX_CHARGE, 
                 np.float64_t charge_floor=0,
                 bool charge_on_sell_only=False):
        super(PerOrderCharge, self).__init__(
                charges, charge_cap, charge_floor, charge_on_sell_only)
        
        self._order_cache = MaxSizedOrderedDict(
                max_size=ORDERS_CACHE_SIZE, chunksize=ORDERS_CHUNK_SIZE)
        
    cpdef double calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t commission=0, np.float64_t last_fx=1.0):
        if self._sell_only and order.side == OrderSide.BUY:
            return 0
        
        if order.oid in self._order_cache:
            return 0
        
        self._order_cache[order.oid] = True
        return max(self._floor, min(self._cap, self._charges))
    
cdef class NSECharges(ChargesModel):
    """
        NSE trading charges. See 
        https://zerodha.com/charges/#tab-equities. These charges 
        are on dollar value basis.
    """
    cpdef double calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t commission=0, np.float64_t last_fx=1.0):
        cdef np.float64_t value = quantity*price
        cdef np.float64_t stt, txns, sebi, stamp_duty, gst = 0
        
        if order.asset.is_funded():
            if order.product_type == ProductType.INTRADAY:
                if order.side == OrderSide.SELL:
                    stt = STT_EQUITY_INTRADAY*value
            else:
                stt = STT_EQUITY*value
            txns = TXNS_CHARGES_EQUITY*value
        elif order.asset.instrument_type == InstrumentType.FUTURES:
            if order.side == OrderSide.SELL:
                stt = STT_FUT*value
            txns = TXNS_CHARGES_FUT*value
        elif order.asset.instrument_type == InstrumentType.OPT:
            if order.side == OrderSide.SELL:
                stt = STT_OPT*value
            txns = TXNS_CHARGES_OPT*value
        else:
            raise BrokerError('this instrument is not supported.')
        
        sebi = value*SEBI
        stamp_duty = value*STAMP_DUTY
        gst = (commission + txns)*GST
        return (stt + txns + gst + sebi + stamp_duty)*last_fx
    
cdef class NYSECharges(ChargesModel):
    """ 
        NYSE trading charges 
        (https://www.interactivebrokers.com/en/index.php?f=934). These
        charges are on a per share basis.
    """
    cpdef double calculate(self, Order order, np.float64_t quantity, np.float64_t price, 
                    np.float64_t commission=0, np.float64_t last_fx=1.0):
        if not order.fractional:
            quantity = int(round(quantity/order.asset.mult)*order.asset.mult)
        else:
            quantity = round(quantity/order.asset.mult)*order.asset.mult
            
        if order.asset.is_funded():
            return TAPE_A_B_C*quantity
        else:
            raise BrokerError('instrument type not supported.')
            
cdef class FXCharges(NoCharge):
    """ 
        Assumed all charges are merged into commission for FX platforms.
    """
    pass
