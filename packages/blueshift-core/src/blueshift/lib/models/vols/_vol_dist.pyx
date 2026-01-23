# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
from numpy cimport float64_t, ndarray
cimport numpy as np
import numpy as np

from libc.math cimport sqrt, fmax, fmin, fabs

from blueshift.interfaces.assets._assets cimport OptionType
from ._black_scholes cimport bs_price
from ._vol_models cimport Volatility

from blueshift.lib.common.constants import ANNUALIZATION_FACTOR

@cython.boundscheck(False)
@cython.wraparound(False)
cpdef ndarray implied_dist(Volatility vol, int n, float64_t[:] bounds, 
                           bool normalize, float64_t width):
    cdef float64_t k=0, p1=0, p2=0, p3=0, offset=0
    cdef float64_t lowstrike = 100*(1+bounds[0])
    cdef float64_t highstrike = 100*(1+bounds[1])
    cdef ndarray[float64_t] strikes = np.linspace(
            lowstrike, highstrike, num=n, dtype=np.float64)
    cdef ndarray[float64_t] prob = np.zeros(n, dtype=np.float64)
    cdef float64_t d = fabs(strikes[1] - strikes[0])
    cdef float64_t h = width*d
    
    for i in range(n):
        k = strikes[i]
        p1 = bs_price(100, k+h, vol.get_vol((k+h)*vol.atmf/100), vol.t, OptionType.CALL)
        p2 = bs_price(100, k, vol.get_vol(k*vol.atmf/100), vol.t, OptionType.CALL)
        p3 = bs_price(100, k-h, vol.get_vol((k-h)*vol.atmf/100), vol.t, OptionType.CALL)
        prob[i] = (p1 - 2*p2 + p3)/h**2
        
    if normalize:
        prob = prob/np.sum(prob*d)
        
    if normalize:
        offset = 100 - np.sum(strikes*prob)*d
        strikes = strikes + offset
        
    return np.vstack([strikes, prob]).T

cpdef ndarray realized_dist(float64_t[:] rets, float64_t t, int n, float64_t[:] bounds, 
                    bool normalize):
    from scipy.stats import gaussian_kde
    
    cdef float64_t offset=0
    cdef float64_t lowstrike = 100*(1+bounds[0])
    cdef float64_t highstrike = 100*(1+bounds[1])
    cdef ndarray[float64_t] strikes = np.linspace(
            lowstrike, highstrike, num=n, dtype=np.float64)
    cdef float64_t d = fabs(strikes[1] - strikes[0])
    cdef float64_t dt = t/(1.0/ANNUALIZATION_FACTOR)
    cdef ndarray[float64_t] bins = np.linspace(
            bounds[0]*sqrt(dt), bounds[1]*sqrt(dt), num=n, dtype=np.float64)
    
    rets = rets*np.sqrt(dt)
    kde = gaussian_kde(rets)
    cdef ndarray[float64_t] prob = kde.pdf(bins)
    
    if normalize:
        prob = prob/np.sum(prob*d)
        
    if normalize:
        offset = 100 - np.sum(strikes*prob)*d
        strikes = strikes + offset
        
    return np.vstack([strikes, prob]).T
    
cpdef float64_t _payoff(float64_t x, float64_t k, ndarray[float64_t,ndim=2] dist):
    cdef float64_t p = np.interp(x, dist[:,0], dist[:,1])
    return fmax(0, x-k)*p

cpdef float64_t dist_pricer(float64_t atmf, float64_t strike, 
                            ndarray[float64_t,ndim=2] dist, 
                            OptionType option_type, float64_t discount,
                            float64_t bound):
    from scipy.integrate import quad as integrate
    cdef float64_t k = 100*strike/atmf
    
    if bound == 0:
        bound = np.inf
    else:
        bound = 100*(1+abs(bound))
    
    cdef float64_t price = integrate(
            _payoff, k, bound, args=(k, dist))[0]

    if type==OptionType.PUT:
        price = price - (100-k)
      
    return discount*price*atmf/100