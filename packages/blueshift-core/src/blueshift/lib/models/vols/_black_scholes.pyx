# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
cimport numpy as np
import numpy as np
import bisect
from numpy cimport float64_t, int64_t, ndarray
from libc.math cimport erf, sqrt, log, exp, M_SQRT2, M_PI, isnan, fmax

from blueshift.interfaces.assets._assets cimport OptionType
from blueshift.lib.trades._order_types cimport GREEK

cdef double INTRINSIC_VOL = 0.000001
cdef double MIN_TOLERANCE = 0.000001
cdef double MIN_TIME = 0.000004 # between 2 to 1 min in years

cdef double N(double x) nogil:
    # standard normal cdf
    return (1 + erf(x/M_SQRT2))/2

cdef double phi(double z) nogil:
    # standard normal pdf
    return exp(-(z*z)/2)/sqrt(2*M_PI)

cpdef double interpolate_atmf(double futures, double spot, 
                                   double t, double T, 
                                   double tick=0.05, int precision=2):
    cdef double atmf = 0
    if T < 1:
        T = 1E-7
        
    if t < 1:
        t = 1E-7
        
    r = (log(futures) - log(spot))/T
    atmf = spot*exp(r*t)
    if isnan(atmf):
        return atmf
    
    return round(round(atmf/tick)*tick, precision)

cpdef np.ndarray[double] interpolate_atmfv(float64_t[:] futures, float64_t[:] spot, 
                                   float64_t[:] t, float64_t[:] T, 
                                   double tick=0.05, int precision=2):
    cdef Py_ssize_t i, n = len(T)
    cdef double r
    cdef np.ndarray[double] atmf = np.empty(n)
    
    for i in range(n):
        if T[i] < 1:
            T[i] = 1E-7
            
        if t[i] < 1:
            t[i] = 1E-7
        
        r = (log(futures[i]) - log(spot[i]))/T[i]
        atmf[i] = spot[i]*exp(r*t[i])
        if not isnan(atmf[i]):
            atmf[i] = round(round(atmf[i]/tick)*tick, precision)
    
    return atmf

cpdef double bs_price(double F, double K, double v, double T, OptionType option_type):
    cdef double tv, d1, d2
    
    if v == INTRINSIC_VOL or T <= MIN_TIME:
        if option_type == OptionType.PUT:
            return fmax(K-F,0)
        else:
            return fmax(F-K,0)
    
    if v<=0 or F<=0 or K<=0:
        return np.nan
    if isnan(v) or isnan(F):
        return np.nan
    
    tv = v*sqrt(T)
    d1 = (log(F/K) + (v**2/2)*T)/tv
    d2 = d1 - tv
    
    if option_type == OptionType.PUT:
        return round(K*N(-d2) - F*N(-d1),6)
    else:
        return round(F*N(d1) - K*N(d2),6)
    
@cython.boundscheck(False)
@cython.wraparound(False)
cpdef np.ndarray[double] bs_pricev(float64_t[:] F, float64_t[:] K, float64_t[:] v, float64_t[:] T, OptionType option_type):
    cdef Py_ssize_t i, n = len(F)
    cdef double tv, d1, d2
    cdef np.ndarray[double] out = np.empty(n)
    
    for i in range(n):
        if v[i] == INTRINSIC_VOL or T[i] <= MIN_TIME:
            if option_type == OptionType.PUT:
                out[i] = fmax(K[i]-F[i],0)
            else:
                out[i] = fmax(F[i]-K[i],0)
        elif isnan(v[i]) or isnan(F[i]):
            out[i] = np.nan
        elif v[i]<=0 or F[i]<=0 or K[i]<=0:
            out[i] = np.nan
        else:
            tv = v[i]*sqrt(T[i])
            d1 = (log(F[i]/K[i]) + (v[i]**2/2)*T[i])/tv
            d2 = d1 - tv
            if option_type == OptionType.PUT :
                out[i] = round(K[i]*N(-d2) - F[i]*N(-d1),6)
            else:
                out[i] = round(F[i]*N(d1) - K[i]*N(d2),6)
            
    return out
    
cdef double bs_delta(double F, double K, double v, double T, OptionType option_type):
    cdef double tv, d1
    
    if v == INTRINSIC_VOL or T <= MIN_TIME:
        if option_type == OptionType.PUT:
            return -1.0
        else:
            return 1.0
    
    if v<=0 or F<=0 or K<=0:
        return np.nan
    if isnan(v) or isnan(F):
        return np.nan
    
    tv = v*sqrt(T)
    d1 = (log(F/K) + (v**2/2)*T)/tv
    
    if option_type == OptionType.PUT:
        return N(d1) - 1
    else:
        return N(d1)
    
@cython.boundscheck(False)
@cython.wraparound(False)
cdef np.ndarray[double] bs_deltav(float64_t[:] F, float64_t[:] K, float64_t[:] v, float64_t[:] T, OptionType option_type):
    cdef Py_ssize_t i, n = len(F)
    cdef double tv, d1
    cdef np.ndarray[double] out = np.empty(n)
    
    for i in range(n):
        if v[i] == INTRINSIC_VOL or T[i] <= MIN_TIME:
            if option_type == OptionType.PUT:
                out[i] = -1.0
            else:
                out[i] = 1.0
        elif isnan(v[i]) or isnan(F[i]):
            out[i] = np.nan
        elif v[i]<=0 or F[i]<=0 or K[i]<=0:
            out[i] = np.nan
        else:
            tv = v[i]*sqrt(T[i])
            d1 = (log(F[i]/K[i]) + (v[i]**2/2)*T[i])/tv
        
            if option_type == OptionType.PUT:
                out[i] = N(d1) - 1
            else:
                out[i] = N(d1)
            
    return out

cdef double bs_strike_delta(double F, double K, double v, double T, OptionType option_type):
    cdef double tv, d1, d2
    
    if v == INTRINSIC_VOL or T <= MIN_TIME:
        if option_type == OptionType.PUT:
            return 1.0
        else:
            return -1.0
    
    if v<=0 or F<=0 or K<=0:
        return np.nan
    if isnan(v) or isnan(F):
        return np.nan
    
    tv = v*sqrt(T)
    d1 = (log(F/K) + (v**2/2)*T)/tv
    d2 = d1 - tv
    
    if option_type == OptionType.PUT:
        return N(-d2)
    else:
        return -N(d2)
    
@cython.boundscheck(False)
@cython.wraparound(False)
cdef np.ndarray[double] bs_strike_deltav(float64_t[:] F, float64_t[:] K, float64_t[:] v, float64_t[:] T, OptionType option_type):
    cdef Py_ssize_t i, n = len(F)
    cdef double tv, d1
    cdef np.ndarray[double] out = np.empty(n)
    
    for i in range(n):
        if v[i] == INTRINSIC_VOL or T[i] <= MIN_TIME:
            if option_type == OptionType.PUT:
                out[i] = 1.0
            else:
                out[i] = -1.0
        elif isnan(v[i]) or isnan(F[i]):
            out[i] = np.nan
        elif v[i]<=0 or F[i]<=0 or K[i]<=0:
            out[i] = np.nan
        else:
            tv = v[i]*sqrt(T[i])
            d1 = (log(F[i]/K[i]) + (v[i]**2/2)*T[i])/tv
            d2 = d1 - tv
        
            if option_type == OptionType.PUT:
                out[i] = N(-d2)
            else:
                out[i] = -N(d2)
            
    return out
    
cdef double bs_gamma(double F, double K, double v, double T, OptionType option_type):
    cdef double tv, d1
    
    if v == INTRINSIC_VOL or T <= MIN_TIME:
        return 0
    
    if v<=0 or F<=0 or K<=0:
        return np.nan
    if isnan(v) or isnan(F):
        return np.nan
    
    tv = v*sqrt(T)
    d1 = (log(F/K) + (v**2/2)*T)/tv
    
    return phi(d1)/(F*tv)

@cython.boundscheck(False)
@cython.wraparound(False)
cdef np.ndarray[double] bs_gammav(float64_t[:] F, float64_t[:] K, float64_t[:] v, float64_t[:] T, OptionType option_type):
    cdef Py_ssize_t i, n = len(F)
    cdef double tv, d1
    cdef np.ndarray[double] out = np.empty(n)
    
    for i in range(n):
        if v[i] == INTRINSIC_VOL or T[i] <= MIN_TIME:
            out[i] = 0.0
        elif isnan(v[i]) or isnan(F[i]):
            out[i] = np.nan
        elif v[i]<=0 or F[i]<=0 or K[i]<=0:
            out[i] = np.nan
        else:
            tv = v[i]*sqrt(T[i])
            d1 = (log(F[i]/K[i]) + (v[i]**2/2)*T[i])/tv
            out[i] = phi(d1)/(F[i]*tv)
    
    return out

cdef double bs_vega(double F, double K, double v, double T, OptionType option_type):
    cdef double tv, d1
    
    if v == INTRINSIC_VOL or T <= MIN_TIME:
        return 0
    
    if v<=0 or F<=0 or K<=0:
        return np.nan
    if isnan(v) or isnan(F):
        return np.nan
    
    tv = v*sqrt(T)
    d1 = (log(F/K) + (v**2/2)*T)/tv
    
    return F*phi(d1)*sqrt(T)

@cython.boundscheck(False)
@cython.wraparound(False)
cdef np.ndarray[double] bs_vegav(float64_t[:] F, float64_t[:] K, float64_t[:] v, float64_t[:] T, OptionType option_type):
    cdef Py_ssize_t i, n = len(F)
    cdef double tv, d1
    cdef np.ndarray[double] out = np.empty(n)
    
    for i in range(n):
        if v[i] == INTRINSIC_VOL or T[i] <= MIN_TIME:
            out[i] = 0.0
        elif isnan(v[i]) or isnan(F[i]):
            out[i] = np.nan
        elif v[i]<=0 or F[i]<=0 or K[i]<=0:
            out[i] = np.nan
        else:
            tv = v[i]*sqrt(T[i])
            d1 = (log(F[i]/K[i]) + (v[i]**2/2)*T[i])/tv
            out[i] = F[i]*phi(d1)*sqrt(T[i])
    
    return out

cdef double bs_theta(double F, double K, double v, double T, OptionType option_type):
    cdef double tv, d1
    
    if v == INTRINSIC_VOL or T <= MIN_TIME:
        return 0
    
    if v<=0 or F<=0 or K<=0:
        return np.nan
    if isnan(v) or isnan(F):
        return np.nan
    
    tv = v*sqrt(T)
    d1 = (log(F/K) + (v**2/2)*T)/tv
    
    return - F*phi(d1)*v/(2*sqrt(T))

@cython.boundscheck(False)
@cython.wraparound(False)
cdef np.ndarray[double] bs_thetav(float64_t[:] F, float64_t[:] K, float64_t[:] v, float64_t[:] T, OptionType option_type):
    cdef Py_ssize_t i, n = len(F)
    cdef double tv, d1
    cdef np.ndarray[double] out = np.empty(n)
    
    for i in range(n):
        if v[i] == INTRINSIC_VOL or T[i] <= MIN_TIME:
            out[i] = 0.0
        elif isnan(v[i]) or isnan(F[i]):
            out[i] = np.nan
        elif v[i]<=0 or F[i]<=0 or K[i]<=0:
            out[i] = np.nan
        else:
            tv = v[i]*sqrt(T[i])
            d1 = (log(F[i]/K[i]) + (v[i]**2/2)*T[i])/tv
            out[i] = -F[i]*phi(d1)*v[i]/(2*sqrt(T[i]))
    
    return out

cpdef double bs_greek(double F, double K, double v, double T, 
                OptionType option_type, GREEK greek):
    if greek == GREEK.THETA:
        return bs_theta(F, K, v, T, option_type)
    elif greek == GREEK.GAMMA:
        return bs_gamma(F, K, v, T, option_type)
    elif greek == GREEK.VEGA:
        return bs_vega(F, K, v, T, option_type)
    else:
        return bs_delta(F, K, v, T, option_type)
    
cpdef np.ndarray[double] bs_greekv(float64_t[:] F, float64_t[:] K, float64_t[:] v, float64_t[:] T, 
                OptionType option_type, GREEK greek):
    if greek == GREEK.THETA:
        return bs_thetav(F, K, v, T, option_type)
    elif greek == GREEK.GAMMA:
        return bs_gammav(F, K, v, T, option_type)
    elif greek == GREEK.VEGA:
        return bs_vegav(F, K, v, T, option_type)
    else:
        return bs_deltav(F, K, v, T, option_type)
    
cpdef double bs_implied_vol(double F, double K, double price, double T, 
                            OptionType option_type, double precision=1.0e-4,
                            bool early_return=False):
    cdef double calc, vega, err = 0
    cdef int max_iter = 200
    #cdef double precision = 1.0e-4
    cdef double v = 0.5
    
    if T <= MIN_TIME:
        return INTRINSIC_VOL
    
    if price<0 or F<=0 or K<=0:
        return np.nan
    
    if isnan(price):
        return np.nan
    
    if option_type == OptionType.PUT:
        if price <= fmax(K-F,0):
            return INTRINSIC_VOL
    else:
        if price <= fmax(F-K,0):
            return INTRINSIC_VOL
    
    for i in range(max_iter):
        if round(v,6) <= 0:
            if early_return:
                return np.nan
            break
        
        calc = bs_price(F, K, v, T, option_type)
        vega = bs_vega(F, K, v, T, option_type)
        
        if isnan(calc) or calc <=0:
            if early_return:
                return np.nan
            break
            
        err = price - calc
        if abs(err) < precision:
            return round(v,6)
        if vega == 0 or isnan(vega):
            if early_return:
                return np.nan
            break
        v = v + err/vega
        
    # did not converge, try with a large initial guess
    v = 3.0
    err = 0
    
    for i in range(max_iter):
        if round(v,6) <= 0:
            return np.nan
        
        calc = bs_price(F, K, v, T, option_type)
        vega = bs_vega(F, K, v, T, option_type)
        
        if isnan(calc) or calc <=0:
            return np.nan
        
        err = price - calc
        if abs(err) < precision:
            return round(v,6)
        if vega == 0 or isnan(vega):
            return np.nan
        v = v + err/vega
    # did not converge, we give up
    return np.nan

cpdef np.ndarray[double] bs_implied_volv(float64_t[:] F, float64_t[:] K, float64_t[:] price, 
                             float64_t[:] T, OptionType option_type, 
                             double precision=1.0e-4, bool early_return=False):
    cdef Py_ssize_t i, n = len(F)
    cdef np.ndarray[double] out = np.empty(n)
    
    for i in range(n):
        out[i] = bs_implied_vol(F[i], K[i], price[i], T[i], option_type,
           precision, early_return)
        
    return out

cdef double _piecewise_linear_vol_smile(
        float64_t[:] strikes, float64_t[:] vols, double K,
        bool can_interpolate):
    # https://www.lme.com/-/media/Files/Trading/Contract-types/\
    # Options/Review-of-Methods-for-the-Interpolation-of-Contributed-\
    # Wing-Volatilities.pdf
    cdef Py_ssize_t idx, nK = len(strikes)
    cdef double adj = 0
    
    #idx = np.searchsorted(strikes, K, side='left')
    idx = bisect.bisect_left(strikes, K)
    if idx == 0 and K < strikes[0]:
        if not can_interpolate:
            return np.nan
        # extrapolate to left, floored at the last vol
        #adj = (vols[1]-vols[0])/(log(strikes[1])-log(strikes[0]))
        #adj = adj*(log(K)-log(strikes[0]))
        #adj = vols[0] + adj
        #return fmax(adj, vols[0])
        return vols[0]
    elif idx > nK-1:
        if not can_interpolate:
            return np.nan
        # extrapolate to right
        #adj = (vols[nK-1]-vols[nK-2])/(log(strikes[nK-1])-log(strikes[nK-2]))
        #adj = adj*(log(K)-log(strikes[nK-1]))
        #adj = vols[nK-1] + adj
        #return fmax(adj, vols[nK-1])
        return vols[nK-1]
    elif K == strikes[idx]:
        # exact match
        return vols[idx]
    else:
        if not can_interpolate:
            return np.nan
        # interpolate
        adj = (vols[idx]-vols[idx-1])/(log(strikes[idx])-log(strikes[idx-1]))
        adj = adj*(log(K)-log(strikes[idx-1]))
        return vols[idx-1] + adj
    
cpdef double piecewise_linear_vol_smile(
        double F, double K, float64_t[:] strikes, float64_t[:] vols):
    cdef Py_ssize_t i, n=0
    cdef Py_ssize_t nK = len(strikes)
    cdef np.ndarray[double] k = np.empty(nK)
    cdef np.ndarray[double] v = np.empty(nK)
    cdef bool can_interpolate=False, otm_c=False, otm_p=False
    
    # remove NaN values
    for i in range(nK):
        if not isnan(vols[i]):
            v[n] = vols[i]
            k[n] = strikes[i]
            if k[n] > F:
                otm_c = True
            else:
                otm_p = True
            n = n+1
                
    #can_interpolate = nK > 2 and otm_c and otm_p
    k = k[:n]
    v = v[:n]
    
    if n < 1:
        return np.nan
    elif n < 2:
        return v[0]
    else:
        can_interpolate = n > 2  
        return _piecewise_linear_vol_smile(k[:n], v[:n], K, can_interpolate)

    
cpdef double bs_price_to_strike(double F, double price, double V, double T, 
                            OptionType option_type, double precision=1.0e-2,
                            bool early_return=False):
    cdef double calc, delta, err = 0
    cdef int max_iter = 200
    #cdef double precision = 1.0e-4
    cdef double K = F
    
    if T <= MIN_TIME:
        return np.nan
    
    if price<0 or F<=0 or V<=0:
        return np.nan
    
    if isnan(price) or isnan(V):
        return np.nan
    
    for i in range(max_iter):
        if round(K,6) <= 0:
            if early_return:
                return np.nan
            break
        
        calc = bs_price(F, K, V, T, option_type)
        delta = bs_strike_delta(F, K, V, T, option_type)
        
        if isnan(calc) or calc <=0:
            if early_return:
                return np.nan
            break
            
        err = price - calc
        if abs(err) < precision:
            return round(K,6)
        if delta == 0 or isnan(delta):
            if early_return:
                return np.nan
            break
        K = K + err/delta
        
    # did not converge, try with a large initial guess
    err = 0
    calc = bs_price(F, F, V, T, option_type)
    if isnan(calc) or calc <=0:
        return np.nan
    
    if price > calc:
        # ITM guess
        if option_type == OptionType.PUT:
            K = 2*F
        else:
            K = 0.2*F
    else:
        # OTM guess
        if option_type == OptionType.PUT:
            K = 0.2*F
        else:
            K = 2*F
    
    for i in range(max_iter):
        if round(K,6) <= 0:
            return np.nan
        
        calc = bs_price(F, K, V, T, option_type)
        delta = bs_strike_delta(F, K, V, T, option_type)
        
        if isnan(calc) or calc <=0:
            return np.nan
        
        err = price - calc
        if abs(err) < precision:
            return round(K,6)
        if delta == 0 or isnan(delta):
            return np.nan
        K = K + err/delta
    
    # did not converge, we give up
    return np.nan

