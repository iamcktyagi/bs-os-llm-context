# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
from numpy cimport float64_t, ndarray

cpdef enum VolModel:
    UNDEFINED = 0,
    LINNEAR = 1,
    QUADRATIC = 2,
    SABR = 3

cdef class Volatility:
    cdef readonly float64_t atmf
    cdef readonly float64_t atmv
    cdef readonly float64_t t
    cdef readonly float64_t beta
    cdef readonly float64_t vol_cutoff
    cdef readonly float64_t rmse
    cdef readonly bool calibrated
    cdef readonly VolModel model
    
    cpdef calibrate(self, float64_t atmf, float64_t t, float64_t[:] strikes, float64_t[:] vols)
    cdef _calibrate(self, float64_t atmf, float64_t t, float64_t[:] strikes, float64_t[:] vols)
    cpdef float64_t get_vol(self, float64_t strike)
    cdef float64_t _get_vol(self, float64_t strike)
    cpdef ndarray[float64_t] get_volv(self, float64_t[:] strikes)