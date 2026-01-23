from __future__ import annotations

from blueshift.lib.common.constants import Currency, CCY

class Account:
    margin:float
    net:float
    name:str
    currency:str
    gross_leverage:float
    net_leverage:float
    gross_exposure:float
    net_exposure:float
    cash:float
    holdings:float
    cost_basis:float
    mtm:float
    liquid_value:float
    commissions:float
    charges:float

    def __init__(self,
                 name:str,
                 cash:float,                    # available cash
                 margin:float=0,                # blocked margin
                 gross_exposure:float=0,        # existing exposure
                 net_exposure:float=0,          # existing exposure
                 holdings:float=0,              # existing holdings value
                 cost_basis:float=0,            # existing cost basis
                 mtm:float=0,                   # unrealized position value
                 commissions:float=0,           # cumulative commissions
                 charges:float=0,               # cumulative charges
                 currency:Currency=CCY.LOCAL    # type: ignore -> account currencty
                 ):...
    def to_dict(self) -> dict:...
    def to_json(self) -> dict:...
    def __str__(self) -> str:...
    def __repr__(self) -> str:...
    @classmethod
    def from_dict(cls, data) -> Account:...
    def copy(self) -> Account:...
    def update_account(self, cash:float, margin:float, positions:dict):...
    def update_commissions(self, commissions:float, charges:float):...

class BlotterAccount(Account):
    starting_cash:float
    funding:float
    realized:float
    day_pnl:float
    pnls:float
    long_exposure:float
    short_exposure:float
    longs_count:int
    shorts_count:int
    cash_adj:float

    def __init__(self,
                 name:str,
                 cash:float,                # available cash
                 starting_cash:float,       # starting cash value
                 funding:float=0,           # cumulative capital infusion 
                 margin:float=0,            # blocked margin
                 realized:float = 0,        # realized pnls
                 gross_exposure:float=0,    # existing exposure
                 net_exposure:float=0,      # existing exposure
                 holdings:float=0,          # existing holdings value
                 cost_basis:float=0,        # existing cost basis
                 mtm:float=0,               # unrealized position value
                 commissions:float=0,       # cumulative commissions
                 charges:float=0,           # cumulative charges
                 long_exposure:float=0,     # existing long exposure
                 short_exposure:float=0,    # existing short exposure
                 longs_count:int=0,         # existing count of long positions
                 shorts_count:int=0,        # existing count of short positions
                 currency:Currency=CCY.LOCAL, # type: ignore -> account currencty
                 cash_adj:float=0,          
                 day_pnl:float=0            # today's profit-and-loss
                 ):...

    @classmethod
    def from_dict(cls, data:dict) -> BlotterAccount:...
    def cashflow(self, cash:float, margin:float):...
    def settle_cash(self, cash:float, margin:float, realized:float=0.0):...
    def settle_dividends(self, div:float):...
    def fund_transfer(self, amount:float):...
    def release_margin(self, amount:float):...
    def block_margin(self, amount:float):...
    def settle_trade(self, t):...
    def incremental_update(self, cash:float, margin:float, commissions:float, 
                           charges:float, realized_pnl:float, unrealized_pnls:float, 
                           gross_exposure:float, net_exposure:float, holdings:float, 
                           cost_basis:float, long_exposure:float, short_exposure:float, 
                           longs_count:int, shorts_count:int, adj:float, 
                           day_pnl:float):...
    def copy(self) -> BlotterAccount:...
    def add_to_account(self, acct:BlotterAccount):...
    def set_day_pnl(self, day_pnl:float):...
        
