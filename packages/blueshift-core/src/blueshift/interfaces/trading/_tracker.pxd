# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
from cpython cimport bool
import numpy as np
cimport numpy as np
from blueshift.lib.trades._order cimport Order
from blueshift.lib.trades._accounts cimport BlotterAccount

cdef class Tracker:

    cdef readonly object FILE_FORMAT        # file name format to save/ load
    cdef readonly object _name              # run name
    cdef readonly object _mode              # LIVE or BACKTEST
    cdef readonly object _ccy               # account currency
    cdef readonly bool _local_ccy           # if local currency
    cdef readonly object _timestamp         # current timestamp
    cdef readonly object _last_reconciled
    cdef readonly object _last_saved
    cdef readonly bool _needs_reconciliation
    cdef readonly bool _last_reconciliation_status

    cdef _skip_ts(cls, object ts, object timestamp=*)
    cdef _last_saved_date(
            self, object directory, object timestamp, object pattern=*,
            object prefix=*, object suffix=*)
    cpdef set_timestamp(self, object timestamp)
    cpdef if_update_required(self, object broker)

