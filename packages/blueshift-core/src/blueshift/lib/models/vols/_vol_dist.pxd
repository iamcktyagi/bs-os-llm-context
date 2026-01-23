# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
from numpy cimport float64_t, ndarray

from blueshift.interfaces.assets._assets cimport OptionType
from ._vol_models cimport Volatility

cpdef ndarray implied_dist(Volatility vol, int n, float64_t[:] bounds, 
                           bool normalize, float64_t width)

cpdef ndarray realized_dist(float64_t[:] rets, float64_t t, int n, 
                            float64_t[:] bounds, bool normalize)

cpdef float64_t dist_pricer(float64_t atmf, float64_t strike, 
                            ndarray[float64_t,ndim=2] dist, 
                            OptionType option_type, float64_t discount,
                            float64_t bound)