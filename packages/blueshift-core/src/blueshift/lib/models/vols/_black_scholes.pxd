# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool

cimport numpy as np
import numpy as np
from numpy cimport float64_t, ndarray


from blueshift.interfaces.assets._assets cimport OptionType
from blueshift.lib.trades._order_types cimport GREEK


# scalar atmf interpolation
cpdef double interpolate_atmf(double futures, double spot, 
                                   double t, double T, 
                                   double tick=*, int precision=*)

# vectorized atmf interpolation
cpdef np.ndarray[double] interpolate_atmfv(float64_t[:] futures, float64_t[:] spot, 
                                   float64_t[:] t, float64_t[:] T, 
                                   double tick=*, int precision=*)

# scalar black scholes pricing function
cpdef double bs_price(double F, double K, double v, double T, 
                      OptionType option_type)
# vectorized black scholes pricing function
cpdef np.ndarray[double] bs_pricev(float64_t[:] F, float64_t[:] K, 
                float64_t[:] v, float64_t[:] T, OptionType option_type)

# scalar option greeks function
cpdef double bs_greek(double F, double K, double v, double T, 
                OptionType option_type, GREEK greek)

# vectorized option greeks function
cpdef np.ndarray[double] bs_greekv(float64_t[:] F, float64_t[:] K, 
                float64_t[:] v, float64_t[:] T, OptionType option_type, 
                GREEK greek)

# scalar black scholes implied vol function
cpdef double bs_implied_vol(double F, double K, double price, double T, 
                            OptionType option_type,
                            double precision=*,
                            bool early_return=*)

# vectorized black scholes implied vol function
cpdef np.ndarray[double] bs_implied_volv(float64_t[:] F, float64_t[:] K, float64_t[:] price, 
                             float64_t[:] T, OptionType option_type, 
                             double precision=*, bool early_return=*)

# piece-wise linear vol smile interpolation
cpdef double piecewise_linear_vol_smile(
        double F, double K, float64_t[:] strikes, float64_t[:] vols)

# scalar black scholes price to strike
cpdef double bs_price_to_strike(double F, double price, double V, double T, 
                            OptionType option_type,
                            double precision=*,
                            bool early_return=*)