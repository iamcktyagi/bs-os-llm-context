# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
from numpy cimport float64_t, ndarray
cimport numpy as cnp
import numpy as np
from libc.math cimport sqrt, fmax, fmin, fabs, pow

from ._black_scholes cimport piecewise_linear_vol_smile

cnp.import_array()

cdef class Volatility:
    """
        Volatility model for an option chain with single expiry and underlying.
        The `strike_cutoff` is the value beyond which input strikes are not 
        considered for calibrartion. The `vol_cutoff` is the max value of 
        vol to protect againt any numerical explosion at far OTM.
    """
    def __init__(self, float64_t beta=1.0, float64_t vol_cutoff=0.0):
        self.atmf = 0.0
        self.atmv = 0.0
        self.beta = beta
        self.vol_cutoff = vol_cutoff
        self.t = 0.0
        self.rmse = 0.0
        self.calibrated = False
        self.model = VolModel.UNDEFINED
        
    cpdef calibrate(
            self, float64_t atmf, float64_t t, float64_t[:] strikes, float64_t[:] vols):
        if len(strikes) != len(vols):
            raise ValueError(
                    f'strikes and input vols must have same # of observations.')
        if len(strikes) < 3:
            raise ValueError(f'At least 3 observations are required.')
            
        return self._calibrate(atmf, t, strikes, vols)
        
    cpdef float64_t get_vol(self, float64_t strike):
        cdef float64_t vol = self._get_vol(strike)
        
        if self.vol_cutoff > 0:
            return fmin(fmax(0, vol), self.vol_cutoff)
        return fmax(0, vol)
        
    @cython.boundscheck(False)
    @cython.wraparound(False)
    cpdef ndarray[float64_t] get_volv(self, float64_t[:] strikes):
        cdef Py_ssize_t i = 0, n = len(strikes)
        cdef ndarray[float64_t] vols = np.zeros(n, dtype=np.float64) 
        
        for i in range(n):
            vols[i] = self.get_vol(strikes[i])
            
        return vols
    
    def update(self, params):
        raise NotImplementedError
    
    cdef float64_t _get_vol(self, float64_t strike):
        raise NotImplementedError
        
    cdef _calibrate(
            self, float64_t atmf, float64_t t, float64_t[:] strikes, float64_t[:] vols):
        raise NotImplementedError


cdef class QuadraticVol(Volatility):
    """
        Quadratic volatilty with level, skew and convexity parameter.
    """
    cdef readonly float64_t convexity
    cdef readonly float64_t skew
    cdef readonly float64_t level
    
    def __init__(self, float64_t beta=1.0, float64_t vol_cutoff=0.0):
        super(QuadraticVol, self).__init__(beta, vol_cutoff)
        self.level = 0
        self.convexity = 0
        self.skew = 0
        self.model = VolModel.QUADRATIC
        
    def update(self, params):
        if 'atmf' in params and params['atmf'] > 0:
            self.atmf = params['atmf']
            
        if 'atmvol' in params and params['atmvol'] > 0:
            self.level = params['atmvol']
            self.atmv = params['atmvol']
            
        if 't' in params and params['t'] > 0:
            self.t = params['t']
            
        if 'skew' in params and params['skew'] != 0:
            self.skew = params['skew']
            
        if 'convexity' in params and params['convexity'] > 0:
            self.convexity = params['convexity']
    
    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef _calibrate(
            self, float64_t atmf, float64_t t, float64_t[:] strikes, float64_t[:] vols):
        cdef Py_ssize_t i = 0, n = len(strikes)
        cdef float64_t base = 0, convexity=0, skew=0, level = 0, err = 0
        cdef float64_t rmse = 0
        cdef ndarray[float64_t] X = np.zeros(n, dtype=np.float64)
        cdef ndarray A = np.zeros([n,3], dtype=np.float64)
        
        base = pow(atmf,self.beta)
        
        for i in range(n):
            X[i] = (strikes[i] - atmf)/base
        
        A = np.vstack([X**2, X, np.ones(len(X))]).T
        convexity, skew, level = np.linalg.lstsq(A, vols, rcond=None)[0]
        
        if convexity < 0:
            raise ValueError(f'Quadratic vol calibration failed - negative convexity.')
            
        self.atmf = atmf
        self.t = t
        self.atmv = level
        self.level = level
        self.skew = skew
        self.convexity = convexity
        self.calibrated = True
        
        for i in range(n):
            err = self.atmv + self.skew*X[i] + self.convexity*(X[i]**2)
            rmse = rmse + (err - vols[i])**2
            
        self.rmse = sqrt(rmse)/n
        
    cdef float64_t _get_vol(self, float64_t strike):
        cdef float64_t k = pow((strike-self.atmf)/self.atmf,self.beta) #(strike-self.atmf)/self.atmf**self.beta
        return self.level + self.skew*k + self.convexity*(k**2)
    
cdef float64_t _SABRvol(float64_t f,float64_t k,float64_t t, float64_t a,
                       float64_t b, float64_t r, float64_t v):
    cdef float64_t term1=0, term2=0, term3=0, x=0, y=0, z=0, fk=0
    
    if(fabs(f/k-1) < 1e-04):
        term1 = a/pow(f,(1-b))
        term2 = ((((1-b)**2)/24)*((a**2)/pow(f,(2-2*b))) + r*b*a*v/(4*pow(f,(1-b))) + (2-3*(r**2))*(v**2/24))
        y = term1*(1 + term2*t)
    else:
        fk = f*k
        z = v/a*pow((fk),((1-b)/2))*np.log(f/k)
        x = np.log((np.sqrt(1-2*r*z + z**2) + z-r)/(1-r))
        term1 = a / pow(fk,((1-b)/2)) / (1 + (1-b)**2/24*np.log(f/k)**2 +(1-b)**4/1920*np.log(f/k)**4)
        if np.isnan(x) or x==0:
            term2 = 1
        else:
            term2 = z / x

        term3 = 1 + ((1-b)**2/24*a**2/pow(fk,(1-b)) + r*b*v*a/(4*pow(fk,((1-b)/2))) + (2-3*r**2)/24*v**2)*t
        y = term1*term2*term3

    return y

cpdef float64_t _find_alpha(float64_t f, float64_t t, float64_t atmvol, 
                            float64_t b, float64_t r, float64_t v):
    cdef ndarray[float64_t] p
    
    p = np.array([
        ((((1-b)**2)/24)/pow(f,(2-2*b)))/pow(f,(1-b))*t,
        r*b*v/(4*pow(f,(1-b)))/pow(f,(1-b))*t,
        ((2-3*(r**2))*(v**2/24))/pow(f,(1-b))*t + 1/pow(f,(1-b)),
        -atmvol
    ])
    
    roots = np.roots(p)
    roots = roots[(~np.iscomplex(roots)) & (roots > 0)]
    
    if len(roots) > 0:
        roots = roots.astype(np.float64)
        return np.min(roots)
    else:
        return atmvol*pow(f,(1-b))

@cython.boundscheck(False)
@cython.wraparound(False)
cpdef float64_t err_func(float64_t[:] params, float64_t[:] strikes, 
                         float64_t[:] vols,  float64_t atmvol, float64_t f, 
                         float64_t t, float64_t b):
    cdef Py_ssize_t i = 0, n = len(strikes)
    cdef float64_t err = 0
    cdef float64_t a = 0
    cdef float64_t r=params[0]
    cdef float64_t v=params[1]
    
    if(fabs(r)>1):
        return np.Inf
    
    if(v<0):
        return np.Inf
    
    a = _find_alpha(f,t,atmvol,b,r,v)

    for i in range(n):
        err = err + (_SABRvol(f,strikes[i],t,a,b,r,v) - vols[i])**2

    return err
    
cdef class SABRVol(Volatility):
    """
        SABR volatilty with alpha, rho and nu calibration (beta fixed).
    """
    cdef readonly float64_t alpha
    cdef readonly float64_t rho
    cdef readonly float64_t nu
    cdef readonly object bounds
    
    def __init__(self, float64_t beta=1.0, float64_t vol_cutoff=0.0, 
                 bounds=[(-1, 1), (0, 100)]):
        super(SABRVol, self).__init__(beta, vol_cutoff)
        self.alpha = 0
        self.rho = 0
        self.nu = 0
        self.bounds = bounds
        self.model = VolModel.SABR
        
    def update(self, params):    
        if 't' in params and params['t'] > 0:
            self.t = params['t']
            
        if 'rho' in params and abs(params['rho']) < 1:
            self.rho = params['rho']
            
        if 'nu' in params and params['nu'] > 0:
            self.nu = params['nu']
            
        if 'atmf' in params and params['atmf'] > 0:
            self.atmf = params['atmf']
            
        if 'alpha' in params and params['alpha'] > 0:
            self.alpha = params['alpha']
            self.atmv = self.get_vol(self.atmf)
        elif 'atmvol' in params and params['atmvol'] > 0:
            self.alpha = _find_alpha(
                    self.atmf,self.t, params['atmvol'],self.beta, self.rho,
                    self.nu)
            self.atmv = self.get_vol(self.atmf)
        
    
    cpdef _calibrate(
            self, float64_t atmf, float64_t t, float64_t[:] strikes, 
            float64_t[:] vols):
        from scipy.optimize import dual_annealing
        
        cdef float64_t err = 0
        cdef Py_ssize_t n = len(strikes)
        cdef float64_t atmvol = piecewise_linear_vol_smile(
                atmf, atmf, strikes, vols)
        
        fit = dual_annealing(
                err_func, 
                bounds=self.bounds, 
                args=(strikes, vols, atmvol, atmf, t, self.beta))
        self.rho = fit.x[0]
        self.nu = fit.x[1]
        self.atmf = atmf
        self.t = t
        self.alpha = _find_alpha(
                    self.atmf,self.t, atmvol, self.beta, self.rho, self.nu)
        self.atmv = self.get_vol(self.atmf)
        self.calibrated = True
        
        err = err_func(np.array([self.rho,self.nu], dtype=np.float64), 
                       strikes, vols, atmvol, self.atmf, self.t, self.beta)
        self.rmse = sqrt(err)/n
        
        
    cdef float64_t _get_vol(self, float64_t strike):
        return _SABRvol(self.atmf, strike, self.t, self.alpha, self.beta, 
                        self.rho, self.nu)