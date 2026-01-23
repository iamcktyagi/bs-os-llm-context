# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
cimport numpy as np
import numpy as np
from cpython cimport bool
from blueshift.interfaces.assets._assets cimport Asset, InstrumentType

cdef class Trade:
    # class declaration for cimport
    cdef readonly int tid
    cdef readonly int hashed_tid
    cdef readonly object oid
    cdef readonly object broker_order_id
    cdef readonly object exchange_order_id
    cdef readonly object instrument_id
    cdef readonly object side
    cdef readonly int product_type
    cdef readonly np.float64_t average_price
    cdef readonly np.float64_t cash_flow        # excluding charges/ comm.
    cdef readonly np.float64_t margin
    cdef readonly np.float64_t commission       # to the broker
    cdef readonly np.float64_t charges          # to others
    cdef readonly object exchange_timestamp
    cdef readonly object timestamp
    cdef readonly Asset asset
    cdef readonly np.float64_t quantity
    cdef readonly np.float64_t last_fx
    cdef readonly np.float64_t underlying_price
    cdef readonly bool fractional
    cpdef to_dict(self)
    cpdef to_json(self)
    cpdef __reduce__(self)
    cpdef get_cash_outlay(self)
    cpdef get_exposure(self)
    cpdef get_value(self)
    cpdef get_greeks(self, int greek)
