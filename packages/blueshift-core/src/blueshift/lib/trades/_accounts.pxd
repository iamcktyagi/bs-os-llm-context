# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
cimport numpy as np
import numpy as np
from cpython cimport bool

from ._trade cimport Trade

cdef class Account:
    cdef readonly np.float64_t margin
    cdef readonly np.float64_t net                      # computed
    cdef readonly object name
    cdef readonly object currency
    cdef readonly np.float64_t gross_leverage           # computed
    cdef readonly np.float64_t net_leverage             # computed
    cdef readonly np.float64_t gross_exposure
    cdef readonly np.float64_t net_exposure
    cdef readonly np.float64_t cash
    cdef readonly np.float64_t holdings
    cdef readonly np.float64_t cost_basis
    cdef readonly np.float64_t mtm
    cdef readonly np.float64_t liquid_value             # computed
    cdef readonly np.float64_t commissions
    cdef readonly np.float64_t charges

    cpdef update_account(self, np.float64_t cash, np.float64_t margin,
                         dict positions)
    cpdef update_commissions(self, np.float64_t commissions,
                             np.float64_t charges)
    cdef update_from_positions(self, dict positions)
    cdef update_lv_net(self)
    cdef update_leverages(self)

cdef class BlotterAccount(Account):
    cdef readonly np.float64_t starting_cash
    cdef readonly np.float64_t funding
    cdef readonly np.float64_t realized
    cdef readonly np.float64_t day_pnl
    cdef readonly np.float64_t pnls
    cdef readonly np.float64_t long_exposure
    cdef readonly np.float64_t short_exposure
    cdef readonly np.uint64_t longs_count
    cdef readonly np.uint64_t shorts_count
    cdef readonly np.float64_t cash_adj

    cpdef incremental_update(self, np.float64_t cash,       #incremental
                             np.float64_t margin,           #incremental
                             np.float64_t commissions,      #incremental
                             np.float64_t charges,          #incremental
                             np.float64_t realized_pnl,     #total
                             np.float64_t unrealized_pnls,  #total
                             np.float64_t gross_exposure,   #total
                             np.float64_t net_exposure,     #total
                             np.float64_t holdings,         #total
                             np.float64_t cost_basis,       #total
                             np.float64_t long_exposure,    #total
                             np.float64_t short_exposure,   #total
                             np.uint64_t longs_count,       #total
                             np.uint64_t shorts_count,      #total
                             np.float64_t adj,              #total
                             np.float64_t day_pnl           #total
                             )
    
    cpdef blotter_update(self, np.float64_t cash,       #incremental
                             np.float64_t margin,           #incremental
                             np.float64_t commissions,      #incremental
                             np.float64_t charges,          #incremental
                             np.float64_t realized_pnl,     #total
                             np.float64_t unrealized_pnls,  #total
                             np.float64_t gross_exposure,   #total
                             np.float64_t net_exposure,     #total
                             np.float64_t holdings,         #total
                             np.float64_t cost_basis,       #total
                             np.float64_t long_exposure,    #total
                             np.float64_t short_exposure,   #total
                             np.uint64_t longs_count,       #total
                             np.uint64_t shorts_count,      #total
                             np.float64_t adj,              #total
                             np.float64_t day_pnl           #total
                             )

    cpdef cashflow(self, np.float64_t cash, np.float64_t margin)
    cpdef settle_cash(self, np.float64_t cash, np.float64_t margin,
                      np.float64_t realized=*)
    cpdef settle_dividends(self, np.float64_t div)
    cpdef block_margin(self, np.float64_t amount)
    cpdef release_margin(self, np.float64_t amount)
    cpdef settle_trade(self, Trade t)
    cpdef fund_transfer(self, np.float64_t amount)
    cpdef add_to_account(self, BlotterAccount acct)
    cpdef set_day_pnl(self, np.float64_t day_pnl)

cdef class BacktestAccount(BlotterAccount):
    pass

cdef class TradingAccount(Account):
    cpdef reconcile(self, object trades, object positions)

cdef class EquityAccount(TradingAccount):
    pass

cdef class ForexAccount(TradingAccount):
    cpdef convert_currency(self, object currency)
