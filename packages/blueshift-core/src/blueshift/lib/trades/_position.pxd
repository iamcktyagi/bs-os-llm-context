# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
cimport numpy as np
import numpy as np

from ._order_types import (
        ProductType,
        OrderFlag,
        OrderType,
        OrderValidity,
        OrderSide,
        OrderStatus,
        OrderUpdateType,
        GREEK)

from ._trade cimport Trade
from blueshift.interfaces.assets._assets cimport Asset
from ._order cimport Order

cdef class Position:
    cdef readonly object pid
    cdef readonly object instrument_id
    cdef readonly Asset asset
    cdef readonly np.float64_t quantity
    cdef readonly np.float64_t buy_quantity
    cdef readonly np.float64_t buy_price
    cdef readonly np.float64_t sell_quantity
    cdef readonly np.float64_t sell_price
    cdef readonly np.float64_t pnl
    cdef readonly np.float64_t realized_pnl
    cdef readonly np.float64_t unrealized_pnl
    cdef readonly np.float64_t last_price
    cdef readonly np.float64_t last_fx
    cdef readonly np.float64_t underlying_price
    cdef readonly object timestamp
    cdef readonly object entry_time         # filed not used
    cdef readonly np.float64_t value
    cdef readonly np.float64_t cost_basis
    cdef readonly np.float64_t margin
    cdef readonly int product_type
    cdef readonly int position_side
    cdef readonly bool fractional

    cpdef to_dict(self)
    cpdef to_json(self)
    cpdef __reduce__(self)

    cdef compute_pnls(self, bool realized=*)
    cdef update_value(self)
    cpdef add_to_position(self, Position pos)
    cpdef update_price(self, np.float64_t price, np.float64_t last_fx=*,
                       np.float64_t underlying_price=*)
    cpdef update(self, Trade trade)
    cpdef bool if_closed(self)
    cpdef np.float64_t apply_split(
            self, np.float64_t ratio, np.float64_t last_fx=*)
    cpdef np.float64_t apply_merger(
            self, Asset acquirer, np.float64_t ratio, np.float64_t cash_pct,
            np.float64_t last_fx=*)
    cpdef get_mtm_settlement(self, np.float64_t quantity, np.float64_t last_price,
                              np.float64_t last_fx=*)
    cpdef get_unwind_order(self, object timestamp, np.float64_t quantity=*, 
                           int product_type=*, int order_flag=*, 
                           int order_type=*, int order_validity=*,
                           object remark=*)
    
    cpdef double get_exposure(self)
    cpdef double get_value(self)
    cpdef double upfront_cash(self, np.float64_t pct_margin=*)
    cpdef bool is_funded(self)
    cpdef double get_greeks(self, int greek)
