# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
import numpy as np
cimport numpy as np

from blueshift.lib.trades._order_types cimport ProductType

cpdef enum AssetClass:
    EQUITY = 0,
    FOREX = 1,
    COMMODITY = 2,
    RATES = 3,
    CASH = 4,
    CRYPTO = 5,
    VOL = 6

cpdef enum InstrumentType:
    SPOT = 0,
    FUTURES = 1,
    OPT = 2,
    MARGIN = 3,
    FUNDS = 4,
    CFD = 5,
    STRATEGY = 6

cpdef enum MktDataType:
    PRICING = 0,     # price data tied to a sym
    SERIES = 1,      # single point series data tied to a sym
    GENERAL = 2,     # general purpose multi-column data for a sym

cpdef enum OptionType:
    CALL = 0        # european call
    PUT = 1         # european put
    
cpdef enum StrikeType:
    ABS = 0        # absolute strike
    REL = 1        # relative strike
    DEL = 2        # delta offset 
    INCR= 3        # offset count in OTM or ITM
    PREMIUM = 4    # price premium

cdef class MarketData:
    cdef readonly int sid # internal identifier
    cdef readonly hashed_id
    cdef readonly int mktdata_type
    cdef readonly object symbol
    cdef readonly object name
    # live trading related fields that does not need to be saved in schema
    cdef readonly object security_id # external identifier assigned by trading venue
    cdef readonly object broker_symbol # external symbol assigned by trading venue
    cdef readonly float upper_circuit # upper circuit price
    cdef readonly float lower_circuit # lower circuit price
    cdef readonly object exchange_ticker # lower circuit price
    # end live trading related fields
    cdef readonly bool can_trade # if the asset currently tradeable
    cdef readonly object start_date
    cdef readonly object end_date
    cdef readonly object ccy
    cdef readonly object exchange_name
    cdef readonly object calendar_name
    
    cpdef bool is_asset(self)
    cpdef bool is_funded(self)
    cpdef bool is_index(self)
    cpdef bool is_margin(self)
    cpdef bool is_intraday(self)
    cpdef bool is_equity(self)
    cpdef bool is_forex(self)
    cpdef bool is_etfs(self)
    cpdef bool is_crypto(self)
    cpdef bool is_commodity(self)
    cpdef bool is_opt(self)
    cpdef bool is_futures(self)
    cpdef bool is_rolling(self)
    cpdef bool is_call(self)
    cpdef bool is_put(self)

cdef class Asset(MarketData):
    cdef readonly int asset_class
    cdef readonly int instrument_type
    cdef readonly np.float64_t mult
    cdef readonly float tick_size # ticksize is stored as reciprocal
    cdef readonly int precision # precision of tick size and prices
    cdef readonly object auto_close_date # if asset needs to auto-close
    cdef readonly bool fractional       # if fractional trading supported
