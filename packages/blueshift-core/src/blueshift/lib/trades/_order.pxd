# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool

cimport numpy as np
import numpy as np

cimport blueshift.lib.trades._order_types
from ._order_types import (
        ProductType,
        OrderFlag,
        OrderType,
        OrderValidity,
        OrderSide,
        OrderStatus,
        OrderUpdateType)
from ._trade cimport Trade 
from ._position cimport Position
from blueshift.interfaces.assets._assets cimport Asset

cdef class Order:
    '''
        Order objects definition. This provisions for order group
        through parent order ID, as well as other standard fields.
        The `oid` is the field through which the platform tracks an
        order (which can be different from broekr or exchange IDs).
    '''
    cdef readonly object oid
    cdef readonly int hashed_oid
    cdef readonly object reference_id
    cdef readonly object broker_order_id
    cdef readonly object exchange_order_id
    cdef readonly object parent_order_id
    cdef readonly Asset asset
    cdef readonly object user
    cdef readonly object placed_by      # the algo ID!
    cdef readonly int product_type
    cdef readonly int order_flag
    cdef readonly int order_type
    cdef readonly int order_validity
    cdef readonly np.float64_t quantity
    cdef readonly np.float64_t filled
    cdef readonly np.float64_t pending
    cdef readonly np.float64_t disclosed
    cdef readonly np.float64_t price
    cdef readonly np.float64_t average_price
    cdef readonly np.float64_t trigger_price
    cdef readonly np.float64_t stoploss_price
    cdef readonly int side
    cdef readonly int status
    cdef readonly object status_message
    cdef readonly object exchange_timestamp
    cdef readonly object timestamp
    cdef readonly object tag            # any order comment
    cdef readonly bool fractional
    cdef readonly np.float64_t price_at_entry
    cdef readonly np.float64_t create_latency
    cdef readonly np.float64_t exec_latency
    cdef readonly object remark

    cpdef to_dict(self)
    cpdef to_json(self)
    cpdef __reduce__(self)
    cpdef cash_outlay(self, object to_ccy, object data_portal)
    cpdef convert_to_limit(self, float price)
    cpdef set_order_id(self, object order_id)
    cpdef set_reference_id(self, object reference_id)
    cpdef set_user(self, object user)
    cpdef set_placed_by(self, object placed_by)
    cpdef set_tag(self, object tag)
    cpdef set_remark(self, object remark)
    cpdef set_timestamp(self, object timestamp, bool exchange=*)
    cpdef set_exchange_timestamp(self, object timestamp)
    cpdef set_latency(self, np.float64_t create_latency=*, 
                      np.float64_t exec_latency=*)
    cpdef update(self,int update_type, object kwargs)
    cpdef execute(self, float price, np.float64_t volume, object timestamp)
    cpdef partial_execution(self, Trade trade)
    cpdef partial_cancel(self, object reason=*)
    cpdef reject(self, object reason)
    cpdef user_update(self, object kwargs)
    cpdef update_from_pos(self, Position pos, np.float64_t price)
    cpdef bool is_inactive(self)
    cpdef bool is_open(self)
    cpdef bool is_done(self)
    cpdef bool is_cancelled(self)
    cpdef bool is_rejected(self)
    cpdef bool is_final(self)
    cpdef bool has_failed(self)
    cpdef bool is_buy(self)
    cpdef bool is_triggered(self, np.float64_t price)
    
