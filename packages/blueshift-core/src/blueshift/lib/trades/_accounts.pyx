# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
cimport numpy as np
import numpy as np
from cpython cimport bool
from ._trade cimport Trade
from ._position cimport Position

from blueshift.lib.exceptions import InsufficientFund, ValidationError
from blueshift.lib.common.constants import CCY

cdef int _PRECISION = 10

cdef class Account:
    '''
        Trade object definition. A trade belongs to an order that
        generated the trade(s)
    '''

    def __init__(self,
                 object name,
                 np.float64_t cash,                # available cash
                 np.float64_t margin=0,            # blocked margin
                 np.float64_t gross_exposure=0,    # existing exposure
                 np.float64_t net_exposure=0,      # existing exposure
                 np.float64_t holdings=0,          # existing holdings value
                 np.float64_t cost_basis=0,        # existing cost basis
                 np.float64_t mtm=0,               # unrealized position value
                 np.float64_t commissions=0,       # cumulative commissions
                 np.float64_t charges=0,           # cumulative charges
                 object currency=CCY.LOCAL):

        self.name = name
        self.cash = round(cash, _PRECISION)
        self.gross_exposure = round(gross_exposure, _PRECISION)
        self.net_exposure = round(net_exposure, _PRECISION)
        self.holdings = round(holdings, _PRECISION)
        self.cost_basis = round(holdings, _PRECISION)
        self.margin = round(margin, _PRECISION)
        self.mtm = round(mtm, _PRECISION)
        self.currency = currency
        self.commissions = round(commissions, _PRECISION)
        self.charges = round(charges, _PRECISION)
        
        if self.margin < 0:
            self.cash = round(self.cash + self.margin, _PRECISION)
            self.margin = 0

        self.update_lv_net()
        self.update_leverages()

    def to_dict(self):
        return {'margin':self.margin,
                'net':self.net,
                'name':self.name,
                'currency':self.currency.name,
                'gross_leverage':self.gross_leverage,
                'net_leverage':self.net_leverage,
                'gross_exposure':self.gross_exposure,
                'net_exposure':self.net_exposure,
                'holdings': self.holdings,
                'cost_basis': self.cost_basis,
                'cash':self.cash,
                'mtm':self.mtm,
                'liquid_value':self.liquid_value,
                'commissions':self.commissions,
                'charges':self.charges}

    def to_json(self):
        return {'margin':self.margin,
                'net':self.net,
                'name':self.name,
                'currency':self.currency.name,
                'gross_leverage':self.gross_leverage,
                'net_leverage':self.net_leverage,
                'gross_exposure':self.gross_exposure,
                'net_exposure':self.net_exposure,
                'holdings': self.holdings,
                'cost_basis': self.cost_basis,
                'cash':self.cash,
                'mtm':self.mtm,
                'liquid_value':self.liquid_value,
                'commissions':self.commissions,
                'charges':self.charges}

    def __str__(self):
        return 'Blueshift Account [name:%s, net:%.2f, cash:%.2f, mtm:%.2f]' % \
                    (self.name,self.net,self.cash, self.mtm)

    def __repr__(self):
        return self.__str__()

    def __reduce__(self):
        return(self.__class__,( self.margin,
                                self.net,
                                self.name,
                                self.currency,
                                self.gross_leverage,
                                self.net_leverage,
                                self.gross_exposure,
                                self.net_exposure,
                                self.holdings,
                                self.cost_basis,
                                self.cash,
                                self.mtm,
                                self.liquid_value,
                                self.commissions,
                                self.charges))

    @classmethod
    def from_dict(cls, data):
        valid_keys = ["name","cash","margin","gross_exposure",
                      "net_exposure","holdings","cost_basis","mtm",
                      "commissions","charges","currency"]
        kwargs = {}
        for key in data:
            if key in valid_keys:
                kwargs[key] = data[key]

        if "currency" in kwargs and \
            not isinstance(kwargs["currency"], CCY):
            ccy = kwargs.pop("currency")
            try:
                kwargs["currency"] = CCY[ccy.upper()]
            except Exception:
                raise ValidationError(
                        'currency {} not supported'.format(ccy))
        else:
            kwargs["currency"] = CCY.LOCAL

        return cls(**kwargs)
    
    def copy(self):
        return Account.from_dict(self.to_dict())

    cdef update_lv_net(self):
        self.liquid_value = self.cash + self.margin + self.mtm
        self.liquid_value = self.liquid_value + self.cost_basis
        self.liquid_value = round(self.liquid_value, _PRECISION)
        self.net = self.liquid_value

    cdef update_leverages(self):
        if self.liquid_value > 0:
            self.gross_leverage = round(
                    self.gross_exposure/self.liquid_value, _PRECISION)
            self.net_leverage = round(
                    self.net_exposure/self.liquid_value, _PRECISION)
        else:
            self.gross_leverage = 0
            self.net_leverage = 0

    cpdef update_account(self, np.float64_t cash, np.float64_t margin,
                         dict positions):
        """
            Update an account from total (stock) inputs. The reported
            cash and margins overwrite current values. The positions are
            looped through to update exposure, holdings, cost basis and
            mark-to-market. Finally liquid value and leverages are updated.

            Args:
                ``cash (float)``: Total account cash, as reported.

                ``margin (float)``: Total margin held, as reported.

                ``positions (dict)``: Current reported positions.
        """
        self.cash = round(cash, _PRECISION)
        self.margin = round(margin, _PRECISION)
        
        if self.margin < 0:
            self.cash = round(self.cash + self.margin, _PRECISION)
            self.margin = 0

        self.update_from_positions(positions)
        self.update_lv_net()
        self.update_leverages()

    cpdef update_commissions(self, np.float64_t commissions,
                             np.float64_t charges):
        """
            Update an account from total(stock) inputs. the charges
            and commissions over-write the existing levels. This
            does not trigger any other updates as the cash levels
            and positions are not changed in this method call.

            Args:
                ``commissions (float)``: Total account cash, as reported.

                ``charges (float)``: Total margin held, as reported.
        """
        self.commissions = round(commissions, _PRECISION)
        self.charges = round(charges, _PRECISION)

    cdef update_from_positions(self, dict positions):
        cdef np.float64_t net_exposure, gross_exposure, exp, mtm
        cdef np.float64_t value, cost_basis
        cdef Position position

        net_exposure = gross_exposure = exp = mtm = 0
        value = cost_basis = 0

        for pos in positions:
            position = positions[pos]
            if position.if_closed():
                continue
            exp = position.get_exposure()
            value = value + position.value
            cost_basis = cost_basis + position.cost_basis
            net_exposure = net_exposure + exp
            gross_exposure = gross_exposure + np.abs(exp)
            mtm = position.unrealized_pnl + mtm

        self.holdings = round(value, _PRECISION)
        self.cost_basis = round(cost_basis, _PRECISION)

        self.gross_exposure = round(gross_exposure, _PRECISION)
        self.net_exposure = round(net_exposure, _PRECISION)
        self.mtm = round(mtm, _PRECISION)

cdef class BlotterAccount(Account):
    """
        Blotter account for tracking performance metrics. This includes
        extra fields realized (captures realized pnls, which is otherwise
        reflected in cash) and starting cash (which the start value) of
        the cash.
    """
    def __init__(self,
                 object name,
                 np.float64_t cash,                # available cash
                 np.float64_t starting_cash,       # starting cash value
                 np.float64_t funding=0,           # cumulative capital infusion 
                 np.float64_t margin=0,            # blocked margin
                 np.float64_t realized = 0,        # realized pnls
                 np.float64_t gross_exposure=0,    # existing exposure
                 np.float64_t net_exposure=0,      # existing exposure
                 np.float64_t holdings=0,
                 np.float64_t cost_basis=0,
                 np.float64_t mtm=0,               # unrealized position value
                 np.float64_t commissions=0,       # cumulative commissions
                 np.float64_t charges=0,           # cumulative charges
                 np.float64_t long_exposure=0,
                 np.float64_t short_exposure=0,
                 np.uint64_t longs_count=0,
                 np.uint64_t shorts_count=0,
                 object currency=CCY.LOCAL,
                 np.float64_t cash_adj=0,
                 np.float64_t day_pnl=0):

        super(BlotterAccount,self).__init__(
                 name=name,
                 cash=cash,
                 margin=margin,
                 gross_exposure=gross_exposure,
                 net_exposure=net_exposure,
                 holdings = holdings,
                 cost_basis = cost_basis,
                 mtm=mtm,
                 commissions=commissions,
                 charges=charges,
                 currency=currency)

        # NOTE: we assume realized is also reflected in cash.
        # That is cash = original cash + realized pnls - charges - margin
        self.starting_cash = round(starting_cash, _PRECISION)
        self.funding = round(funding, _PRECISION)
        self.realized = round(realized, _PRECISION)
        self.long_exposure = round(long_exposure, _PRECISION)
        self.short_exposure = round(short_exposure, _PRECISION)
        self.longs_count = round(longs_count, _PRECISION)
        self.shorts_count = round(shorts_count, _PRECISION)
        self.cash_adj = cash_adj
        self.day_pnl = day_pnl

        self.pnls = round(
                self.realized + self.mtm - self.charges - self.commissions,
                _PRECISION)

    def to_dict(self):
        return {'margin':self.margin,
                'net':self.net,
                'name':self.name,
                'currency':self.currency.name,
                'gross_leverage':self.gross_leverage,
                'net_leverage':self.net_leverage,
                'gross_exposure':self.gross_exposure,
                'net_exposure':self.net_exposure,
                'holdings': self.holdings,
                'cost_basis': self.cost_basis,
                'long_exposure':self.long_exposure,
                'short_exposure':self.short_exposure,
                'longs_count':self.longs_count,
                'shorts_count':self.shorts_count,
                'cash':self.cash,
                'starting_cash':self.starting_cash,
                'funding':self.funding,
                'mtm':self.mtm,
                'realized':self.realized,
                'pnls':self.pnls,
                'liquid_value':self.liquid_value,
                'commissions':self.commissions,
                'charges':self.charges,
                'cash_adj':self.cash_adj,
                'day_pnl':self.day_pnl}

    def to_json(self):
        return {'margin':self.margin,
                'net':self.net,
                'name':self.name,
                'currency':self.currency.name,
                'gross_leverage':self.gross_leverage,
                'net_leverage':self.net_leverage,
                'gross_exposure':self.gross_exposure,
                'net_exposure':self.net_exposure,
                'holdings': self.holdings,
                'cost_basis': self.cost_basis,
                'long_exposure':self.long_exposure,
                'short_exposure':self.short_exposure,
                'longs_count':self.longs_count,
                'shorts_count':self.shorts_count,
                'cash':self.cash,
                'starting_cash':self.starting_cash,
                'funding':self.funding,
                'mtm':self.mtm,
                'realized':self.realized,
                'pnls':self.pnls,
                'liquid_value':self.liquid_value,
                'commissions':self.commissions,
                'charges':self.charges,
                'cash_adj':self.cash_adj,
                'day_pnl':self.day_pnl}

    def __reduce__(self):
        return(self.__class__,( self.margin,
                                self.net,
                                self.name,
                                self.currency,
                                self.gross_leverage,
                                self.net_leverage,
                                self.gross_exposure,
                                self.net_exposure,
                                self.holdings,
                                self.cost_basis,
                                self.long_exposure,
                                self.short_exposure,
                                self.longs_count,
                                self.shorts_count,
                                self.cash,
                                self.starting_cash,
                                self.funding,
                                self.mtm,
                                self.realized,
                                self.pnls,
                                self.liquid_value,
                                self.commissions,
                                self.charges,
                                self.cash_adj,
                                self.day_pnl))

    @classmethod
    def from_dict(cls, data):
        valid_keys = ["name","cash","starting_cash","margin","realized",
                      "gross_exposure","net_exposure","holdings", "funding",
                      "cost_basis", "long_exposure", "short_exposure",
                      "longs_count","shorts_count", "mtm","commissions",
                      "charges","currency","cash_adj","day_pnl"]
        kwargs = {}
        for key in data:
            if key in valid_keys:
                kwargs[key] = data[key]
                
        if "currency" in kwargs and \
            not isinstance(kwargs["currency"], CCY):
            ccy = kwargs.pop("currency")
            try:
                kwargs["currency"] = CCY[ccy.upper()]
            except Exception:
                raise ValidationError(
                        'currency {} not supported'.format(ccy))
        else:
            kwargs["currency"] = CCY.LOCAL

        return cls(**kwargs)
    
    def copy(self):
        return BlotterAccount.from_dict(self.to_dict())

    cpdef cashflow(self, np.float64_t cash, np.float64_t margin):
        """
            Incremental update involving only cash and margin changes.
        """
        self.cash = round(self.cash + cash, _PRECISION)
        self.margin = round(self.margin + margin, _PRECISION)
        self.liquid_value = self.cash + self.margin
        
        if self.margin < 0:
            self.cash = round(self.cash + self.margin, _PRECISION)
            self.margin = 0

        self.update_lv_net()
        self.update_leverages()
        
    cpdef settle_cash(self, np.float64_t cash, np.float64_t margin,
                      np.float64_t realized=0):
        """
            Update an account from incremental inputs. The reported
            cash and margins adds to existing levels.

            Args:
                ``cash (float)``: Cash amount to settle

                ``margin (float)``: Margin to settle
                
                ``realized (float)``: Realized profit to settle
        """
        self.cash = self.cash + round(cash, _PRECISION)
        self.margin = self.margin + round(margin, _PRECISION)
        self.realized = self.realized + round(realized, _PRECISION)
        
        if self.margin < 0:
            self.cash = round(self.cash + self.margin, _PRECISION)
            self.margin = 0
            
        self.update_lv_net()
        self.update_leverages()
        
    cpdef settle_dividends(self, np.float64_t div):
        """
            Update an account for a new dividend payment.

            Args:
                ``div (float)``: Cash amount to settle
        """
        self.cash = self.cash + round(div, _PRECISION)
        self.realized = self.realized + round(div, _PRECISION)
        self.update_lv_net()
        self.update_leverages()
        self.pnls = round(self.realized + self.mtm - self.charges - self.commissions, _PRECISION)
        
    cpdef fund_transfer(self, np.float64_t amount):
        """
            Add capital to the account. Positive values are infusion, negative 
            values mean withdrawal.
        """
        if amount + self.cash < 0:
            raise InsufficientFund('cannot complete fund transfer.')

        self.cash = round(self.cash + amount, _PRECISION)
        self.funding = round(self.funding + amount, _PRECISION)
        
        self.update_lv_net()
        self.update_leverages()
        
    cpdef release_margin(self, np.float64_t amount):
        """ release margin and add back to cash. """
        if amount < 0:
            return

        if self.margin < amount:
            amount = self.margin

        self.cash = round(self.cash + amount, _PRECISION)
        self.margin = round(self.margin - amount, _PRECISION)

    cpdef block_margin(self, np.float64_t amount):
        """ debit cash balance and add to margin. """
        if amount < 0:
            return

        if self.cash < amount:
            raise InsufficientFund()

        self.cash = round(self.cash - amount, _PRECISION)
        self.margin = round(self.margin + amount, _PRECISION)
        
    cpdef settle_trade(self, Trade t):
        """
            Settle a given trade. This changes the cash, charges, comm.
            and margin incrementally. This also changes exposures, and
            potentially holdings and cost basis.
        """
        cdef np.float64_t exp = 0
        # a trade can require cash flow and/ or margin block.
        # NOTE: cash flow is net of charges and commissions.
        if t.cash_flow + t.margin > self.cash:
            raise InsufficientFund()

        self.cash = round(self.cash - t.cash_flow, _PRECISION)
        self.cost_basis = round(self.cost_basis + t.cash_flow,
                                   _PRECISION)

        if t.margin > 0:
            self.block_margin(t.margin)
        else:
            self.release_margin(-t.margin)

        self.commissions = round(self.commissions + t.commission,
                                    _PRECISION)
        self.charges = round(self.charges + t.charges, _PRECISION)

        exp = t.get_exposure()
        self.net_exposure = round(self.net_exposure + exp, _PRECISION)
        self.gross_exposure = round(self.gross_exposure + np.abs(exp),
                                       _PRECISION)
        self.holdings = round(self.holdings + t.get_value(),
                                 _PRECISION)

        self.update_lv_net()
        self.update_leverages()

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
                             ):
        """
            Incremental update of the acount based on incremental cash,
            margin, charges from trades as well as stock (total, not
            incremental) value of realized, unrealized pnls and exposures.
        """
        # NOTE: the supplied is cash is before charges and commissions
        # but the cash stored in account object is net of them.
        self.cash = round(self.cash + cash - margin - commissions - charges,
                          _PRECISION)
        # update the cash to reflect realized pnl settlements
        # this settlement is a stock variable, so we deduct the last
        # settled value and add back the current value
        self.cash = self.cash - self.realized + realized_pnl \
                                            - (adj - self.cash_adj)
        self.cash = round(self.cash, _PRECISION)
        # update the rest.
        self.margin = round(self.margin + margin, _PRECISION)
        self.commissions = round(self.commissions + commissions,
                                    _PRECISION)
        self.charges = round(self.charges + charges, _PRECISION)
        
        if self.margin < 0:
            self.cash = round(self.cash + self.margin, _PRECISION)
            self.margin = 0

        self.holdings = round(holdings, _PRECISION)
        self.cost_basis = round(cost_basis, _PRECISION)
        self.gross_exposure = round(gross_exposure, _PRECISION)
        self.net_exposure = round(net_exposure, _PRECISION)
        self.long_exposure = round(long_exposure, _PRECISION)
        self.short_exposure = round(short_exposure, _PRECISION)
        self.longs_count = longs_count
        self.shorts_count = shorts_count
        self.mtm = round(unrealized_pnls, _PRECISION)
        self.realized = round(realized_pnl, _PRECISION)

        self.update_lv_net()
        self.update_leverages()
        self.pnls = round(self.realized + self.mtm - self.charges - self.commissions, _PRECISION)

        self.cash_adj = adj

        if not np.isnan(day_pnl):
            self.day_pnl= round(day_pnl, _PRECISION)
        
    cpdef blotter_update(self, np.float64_t cash,           #incremental
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
                             ):
        """
            Incremental update of the acount based on incremental cash,
            margin, charges from trades as well as stock (total, not
            incremental) value of realized, unrealized pnls and exposures.
        """
        # NOTE: the supplied is cash is before charges and commissions
        # but the cash stored in account object is net of them.
        self.cash = round(self.cash + cash - margin - commissions - charges,
                          _PRECISION)
        
        # update the rest.
        self.margin = round(self.margin + margin, _PRECISION)
        self.commissions = round(self.commissions + commissions,
                                    _PRECISION)
        self.charges = round(self.charges + charges, _PRECISION)
        
        if self.margin < 0:
            self.cash = round(self.cash + self.margin, _PRECISION)
            self.margin = 0

        self.holdings = round(holdings, _PRECISION)
        self.cost_basis = round(cost_basis, _PRECISION)
        self.gross_exposure = round(gross_exposure, _PRECISION)
        self.net_exposure = round(net_exposure, _PRECISION)
        self.long_exposure = round(long_exposure, _PRECISION)
        self.short_exposure = round(short_exposure, _PRECISION)
        self.longs_count = longs_count
        self.shorts_count = shorts_count
        self.mtm = round(unrealized_pnls, _PRECISION)
        self.realized = round(realized_pnl, _PRECISION)

        self.update_lv_net()
        self.update_leverages()
        self.pnls = round(self.realized + self.mtm - self.charges - self.commissions, _PRECISION)

        self.cash_adj = adj
        
        if day_pnl == 0:
            self.day_pnl = self.pnls
        elif not np.isnan(day_pnl):
            self.day_pnl= round(day_pnl, _PRECISION)
        
    cpdef add_to_account(self, BlotterAccount acct):
        if self.currency != acct.currency:
            raise ValidationError(f'Different currencies, cannot merge.')
        
        self.cash = round(self.cash+acct.cash, _PRECISION)
        self.gross_exposure = round(self.gross_exposure+acct.gross_exposure, _PRECISION)
        self.net_exposure = round(self.net_exposure+acct.net_exposure, _PRECISION)
        self.holdings = round(self.holdings+acct.holdings, _PRECISION)
        self.cost_basis = round(self.cost_basis+acct.cost_basis, _PRECISION)
        self.margin = round(self.margin+acct.margin, _PRECISION)
        self.mtm = round(self.mtm+acct.mtm, _PRECISION)
        self.commissions = round(self.commissions+acct.commissions, _PRECISION)
        self.charges = round(self.charges+acct.charges, _PRECISION)
        
        if self.margin < 0:
            self.cash = round(self.cash + self.margin, _PRECISION)
            self.margin = 0
        
        self.starting_cash = round(self.starting_cash+acct.starting_cash, _PRECISION)
        self.funding = round(self.funding+acct.funding, _PRECISION)
        self.realized = round(self.realized+acct.realized, _PRECISION)
        self.long_exposure = round(self.long_exposure+acct.long_exposure, _PRECISION)
        self.short_exposure = round(self.short_exposure+acct.short_exposure, _PRECISION)
        self.longs_count = round(self.longs_count+acct.longs_count, _PRECISION)
        self.shorts_count = round(self.shorts_count+acct.shorts_count, _PRECISION)
        self.cash_adj = round(self.cash_adj+acct.cash_adj, _PRECISION)
        self.day_pnl = round(self.day_pnl+acct.day_pnl, _PRECISION)
        
        self.update_lv_net()
        self.update_leverages()
        
        self.pnls = round(
                self.realized + self.mtm - self.charges - self.commissions,
                _PRECISION)

    cpdef set_day_pnl(self, np.float64_t day_pnl):
        self.day_pnl = day_pnl
        
        
cdef class BacktestAccount(BlotterAccount):
    """
        back-testing account.
    """
    pass

cdef class TradingAccount(Account):
    '''
        real trading account. Most of the stuff is already done by the
        broker.
    '''
    cpdef reconcile(self, object trades, object positions):
        pass

cdef class EquityAccount(TradingAccount):
    '''
        Trading account for equity trading.
    '''
    pass

cdef class ForexAccount(TradingAccount):
    '''
        Trading account for equity trading.
    '''
    cpdef convert_currency(self, object currency):
        pass
