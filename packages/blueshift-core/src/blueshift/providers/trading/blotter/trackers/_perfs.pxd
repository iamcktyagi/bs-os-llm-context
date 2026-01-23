# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
cimport numpy as np
import numpy as np
from numpy cimport float64_t, ndarray

from blueshift.lib.trades._accounts cimport BlotterAccount
from ._tracker cimport Tracker

cdef class PerformanceTracker(Tracker):
    """ Implementation of performance tracker using file system.
        This handles performance and risks metrics calculation, saving
        the current values as a dict and historical values as numpy
        arrays which are converted to DataFrame on demand.

        Note:
            If mode is LIVE, the data are saved daily, else once at the
            end of the backtest. For backtest runs, roll method only
            increments the position pointer.

        Args:
            ``name (str)``: Name of the current run.

            ``mode (int)``: Run MODE of the current run.

            ``timestamp (Timestamp)``: Current timestamp.

            ``max_lookback (int)``: Maximum lookback for rolling measures.

            ``chunk_size (int)``: Chunk size of array allocation.

        Returns:
            None.
        """

    cdef readonly Py_ssize_t acct_cols
    cdef readonly Py_ssize_t pos_cols
    cdef readonly Py_ssize_t pos_count_cols
    cdef readonly Py_ssize_t risk_cols
    cdef readonly Py_ssize_t perf_cols

    cdef readonly np.uint64_t _pos_idx
    cdef readonly np.uint64_t _max_lookback

    cdef readonly np.uint64_t CHUNK_SIZE
    cdef readonly object PERF_REPORTS_FILE
    cdef readonly object _perf_dir

    cdef readonly object _acct_report
    cdef readonly object _pos_report
    cdef readonly object _pos_count_report
    cdef readonly object _perf_report
    cdef readonly object _risk_report
    cdef readonly object _array_idx
    cdef readonly dict current_performance
    cdef readonly dict perf_report
    cdef readonly object tz
    
    cdef readonly object _benchmark
    cdef readonly object _benchmark_rets

    cdef _set_current_performance(self, BlotterAccount account)
    cdef _check_and_allocate_array(self)
    cpdef update_metrics(self, BlotterAccount account, object orders,
                         object positions, np.int64_t timestamp)
    cdef _update_account_metrics(self, BlotterAccount account)
    cdef _update_positions_metrics(self, BlotterAccount account)
    cdef _update_positions_counts(self, BlotterAccount account)
    cdef _update_perf_metrics(self)
    cdef _update_risk_metrics(self)
    cdef _calc_vol_drawdown(self,
                            np.float64_t [:, :] account_view,
                            np.float64_t [:, :] perf_view)
    
    cpdef _get_aligned_benchmark(
            self, object returns, object funding, 
            float init_cap, object benchmark_rets)
    cdef np.ndarray[double] _cumulative_benchmark(
            self, float64_t[:] capitals, float64_t[:] returns)