# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
from blueshift.lib.trades._order_types cimport ProductType

import numpy as np
cimport numpy as np

from blueshift.lib.exceptions import ValidationError
from blueshift.lib.common.constants import CCY
from blueshift.lib.common.functions import to_int_or_float


cdef int TICK_PRECISION = 6

cdef class MarketData:
    '''
        MarketData class encapsulates an object with which some 
        data (pricing or otherwise) may be associated. All 
        tradable assets are derived from this class. This can 
        also represent other generic data series like 
        macro-economic data or corporate fundamental data.
        
        A MarketData object has the following attributes. All 
        attributes are read-only from a strategy code.
        
        +-----------------+----------------------+-------------------------------+
        |Attribute        | Type                 |Description                    |
        +=================+======================+===============================+
        |sid              | ``int``              |Unique Identifier              |
        +-----------------+----------------------+-------------------------------+
        |symbol           | ``str``              |Symbol string                  |
        +-----------------+----------------------+-------------------------------+
        |name             | ``str``              |Long name                      |
        +-----------------+----------------------+-------------------------------+
        |start_date       | ``pandas.Timestamp`` |Start date of data             |
        +-----------------+----------------------+-------------------------------+
        |end_date         | ``pandas.Timestamp`` |End date of data               |
        +-----------------+----------------------+-------------------------------+
        |ccy              | :ref:`Currency`      |Asset currency                 |
        +-----------------+----------------------+-------------------------------+
        |can_trade        | ``bool``             |If tradeable                   |
        +-----------------+----------------------+-------------------------------+
        |exchange_name    | ``str``              |Name of the exchange           |
        +-----------------+----------------------+-------------------------------+
        |calendar_name    | ``str``              |Name of the calendar           |
        +-----------------+----------------------+-------------------------------+
        
    '''
    def __init__(self,
                 int sid,
                 object symbol,
                 object name=None,
                 object security_id=None,
                 object broker_symbol=None,
                 float upper_circuit=np.nan,
                 float lower_circuit=np.nan,
                 object start_date=None,
                 object end_date=None,
                 object exchange_name=None,
                 object calendar_name=None,
                 object ccy=CCY.LOCAL):
        import pandas as pd
        from blueshift.interfaces.assets.utils import get_sym_exchange

        if ':' in symbol:
            exchange_name, symbol = get_sym_exchange(symbol)
        self.sid = sid
        self.symbol = symbol
        #self.hashed_id = hash(self.symbol+str(self.sid))
        self.hashed_id = hash(self.symbol+str(self.exchange_name))
        
        if name is None:
            name = symbol
        self.name = name
        
        self.security_id = security_id or self.sid
        self.broker_symbol = broker_symbol or self.symbol
        self.upper_circuit = upper_circuit
        self.lower_circuit = lower_circuit
        
        if exchange_name:
            self.exchange_ticker = exchange_name + ':' + symbol
        else:
            self.exchange_ticker = symbol
        
        if start_date is not None:
            try:
                start_date = pd.Timestamp(start_date)
            except Exception:
                raise ValidationError(f'illegal start date {start_date}.')
            else:
                start_date = start_date.normalize()
                if start_date.tz:start_date=start_date.tz_localize(None)
        self.start_date = start_date
        
        if end_date is None:
            end_date = pd.Timestamp.now().normalize()
        else:
            try:
                end_date = pd.Timestamp(end_date)
            except Exception:
                raise ValidationError(f'illegal end date {end_date}.')
            else:
                end_date = end_date.normalize()
                if end_date.tz:end_date=end_date.tz_localize(None)
        self.end_date = end_date
        self.exchange_name = exchange_name
        if calendar_name is None:
            calendar_name = exchange_name
        self.calendar_name = calendar_name
        self.mktdata_type = -1
        self.can_trade = False
        
        if isinstance(ccy, CCY):
            self.ccy = ccy
        else:
            try:
                self.ccy=CCY[ccy.upper()]
            except Exception:
                raise ValidationError(f'unsupported currency {ccy}')

    def __int__(self):
        return self.sid

    def __hash__(self):
        return self.hashed_id

    def __index__(self):
        return self.sid

    def __eq__(x,y):
        try:
            return hash(x) == hash(y)
        except (TypeError, AttributeError, OverflowError):
            raise TypeError

    def __lt__(x,y):
        try:
            return hash(x) < hash(y)
        except (TypeError, AttributeError, OverflowError):
            raise TypeError

    def __gt__(x,y):
        try:
            return hash(x) > hash(y)
        except (TypeError, AttributeError, OverflowError):
            raise TypeError

    def __ne__(x,y):
        try:
            return hash(x) != hash(y)
        except (TypeError, AttributeError, OverflowError):
            raise TypeError

    def __str__(self):
        return '%s(%s [%d])' % (type(self).__name__, self.exchange_ticker, self.sid)

    def __repr__(self):
        return self.__str__()

    def to_dict(self):
        return {
                'sid':self.sid,
                'symbol':self.symbol,
                'name':self.name,
                'security_id':self.security_id,
                'broker_symbol':self.broker_symbol,
                'upper_circuit':self.upper_circuit,
                'lower_circuit':self.lower_circuit,
                'start_date':self.start_date,
                'end_date':self.end_date,
                'exchange_name':self.exchange_name,
                'calendar_name':self.calendar_name,
                'mktdata_type':self.mktdata_type,
                'can_trade':self.can_trade,
                'ccy':self.ccy}

    def __reduce__(self):
        return(self.__class__,(self.sid,
                               self.symbol,
                               self.name,
                               self.security_id,
                               self.broker_symbol,
                               self.upper_circuit,
                               self.lower_circuit,
                               self.start_date,
                               self.end_date,
                               self.exchange_name,
                               self.calendar_name,
                               self.can_trade))

    @classmethod
    def from_dict(cls, data):
        return cls(**data)
    
    def update_from_dict(self, dict data):
        import pandas as pd

        if 'name' in data:
            self.name = data.pop('name')
        if 'security_id' in data:
            self.security_id = data.pop('security_id')
        if 'broker_symbol' in data:
            self.broker_symbol = data.pop('broker_symbol')
        if 'upper_circuit' in data:
            self.upper_circuit = data.pop('upper_circuit')
        if 'lower_circuit' in data:
            self.lower_circuit = data.pop('lower_circuit')
        if 'start_date' in data and data['start_date'] is not None:
            start_date = data['start_date']
            try:
                start_date = pd.Timestamp(start_date)
            except Exception:
                raise ValidationError(f'illegal start date {start_date}.')
            else:
                start_date = start_date.normalize()
                if start_date.tz:start_date=start_date.tz_localize(None)
            self.start_date = start_date
        if 'end_date' in data and data['end_date'] is not None:
            end_date = data['end_date']
            try:
                end_date = pd.Timestamp(end_date)
            except Exception:
                raise ValidationError(f'illegal end date {end_date}.')
            else:
                end_date = end_date.normalize()
                if end_date.tz:end_date=end_date.tz_localize(None)
            self.end_date = end_date
        
        calendar_name = None
        if 'exchange_name' in data:
            self.exchange_name = data.pop('exchange_name')
            calendar_name = self.exchange_name
        if 'calendar_name' in data and data['calendar_name'] is not None:
            calendar_name  = data.pop('calendar_name')
        if calendar_name:
            self.calendar_name = calendar_name
            
    def fx_rate(self, object to_ccy, object data_portal, object dt=None):
        """
            Fx for conversion is always driven by asset currency, hence
            we put this here.

            Args:
                ``to_ccy (str or CCY)``: output currency for conversion

                ``data_portal (object)``: Data portal to query fx rate.
                
                ``dt (Timestamp)``: Timestamp for conversion rate.
        """
        if isinstance(self.ccy, str):
            from_ccy = self.ccy.upper()
        else:
            # Let's throw an error if ccy is nether string or CCY
            from_ccy = self.ccy.name

        if isinstance(to_ccy, str):
            to_ccy = to_ccy.upper()
        else:
            # Let's throw an error if ccy is nether string or CCY
            to_ccy = to_ccy.name

        if to_ccy == from_ccy:
            return 1.0

        if to_ccy == "LOCAL" or from_ccy == "LOCAL":
            return 1.0

        return data_portal.getfx(from_ccy + "/" + to_ccy, dt)

    cpdef bool is_asset(self):
        return False
    
    cpdef bool is_funded(self):
        return False

    cpdef bool is_index(self):
        return True
    
    cpdef bool is_opt(self):
        return False
    
    cpdef bool is_futures(self):
        return False
    
    cpdef bool is_rolling(self):
        return False

    cpdef bool is_margin(self):
        return False

    cpdef bool is_intraday(self):
        return False
    
    cpdef bool is_equity(self):
        return False
    
    cpdef bool is_forex(self):
        return False

    cpdef bool is_commodity(self):
        return False
    
    cpdef bool is_etfs(self):
        return False
    
    cpdef bool is_crypto(self):
        return False

    cpdef bool is_call(self):
        return False

    cpdef bool is_put(self):
        return False

cdef class Asset(MarketData):
    '''
        Asset models a tradeable asset and derives from ``MarketData``.
        
        An Asset object has the following attributes in addition 
        to the attributes of the parent class. All attributes are 
        read-only from a strategy code.
        
        +-----------------+----------------------+-------------------------------+
        |Attribute        | Type                 |Description                    |
        +=================+======================+===============================+
        |asset_class      | :ref:`AssetClass`    |Asset class                    |
        +-----------------+----------------------+-------------------------------+
        |instrument_type  | :ref:`InstrumentType`|Instrument type                |
        +-----------------+----------------------+-------------------------------+
        |mult             | ``float``            |Asset multiplier (lot size)    |
        +-----------------+----------------------+-------------------------------+
        |tick_size        | ``int``              |Tick size (reciprocal)         |
        +-----------------+----------------------+-------------------------------+
        |auto_close_date  | ``pandas.Timestamp`` |Auto-close day                 |
        +-----------------+----------------------+-------------------------------+
        |can_trade        | ``bool``             |If tradeable                   |
        +-----------------+----------------------+-------------------------------+
        |fractional       | ``bool``             |Fractional trading supported   |
        +-----------------+----------------------+-------------------------------+
        
        .. warning::
            The ``mult`` attribute defaults to 1. Any order placed for an asset 
            should include this in the order size. Order placement routine
            will check if the order size is a multiple of ``mult``. 
            For e.g. an asset with a lot size of 75 should use order 
            size of 150 (``order(asset, 150)``) to place an order 
            for 2 lots.
            
        .. note::    
            The attribute ``tick_size`` is stored as reciprocal of the 
            actual tick size. For a tick size of 0.05, the value stored 
            will be 20. It defaults to 100 (corresponding to a tick 
            size of 0.01).
            
            The ``auto_close_date`` is the date on which the asset is 
            automatically squared-off (if still held by the algo). This 
            defaults to the ``end_date``.
        
    '''    
    def __init__(self,
                 int sid,
                 object symbol,
                 object name=None,
                 object security_id=None,
                 object broker_symbol=None,
                 float upper_circuit=np.nan,
                 float lower_circuit=np.nan,
                 object start_date=None,
                 object end_date=None,
                 np.float64_t mult=1,
                 float tick_size=100, # mult by tick size
                 bool can_trade=True,
                 object auto_close_date=None,
                 object exchange_name=None,
                 object calendar_name=None,
                 object ccy=CCY.LOCAL,
                 bool fractional=False):
        import pandas as pd
        super(Asset,self).__init__(
                 sid=sid,
                 symbol=symbol,
                 name=name,
                 security_id=security_id,
                 broker_symbol=broker_symbol,
                 upper_circuit=upper_circuit,
                 lower_circuit=lower_circuit,
                 start_date=start_date,
                 end_date=end_date,
                 exchange_name=exchange_name,
                 calendar_name = calendar_name,
                 ccy=ccy)
        
        self.mult = mult            # lot-size in integer
        self.tick_size = round(tick_size, TICK_PRECISION)  # inverse of the tick-size
        self.can_trade = can_trade
        self.fractional = fractional
        self.precision = max(0,np.ceil(np.log10(self.tick_size))) + 1
        
        if auto_close_date is None:
            self.auto_close_date = self.end_date
        else:
            try:
                auto_close_date = pd.Timestamp(auto_close_date)
            except Exception:
                raise ValidationError(f'illegal date {auto_close_date}.')
            else:
                auto_close_date = auto_close_date.normalize()
                if auto_close_date.tz:
                    auto_close_date=auto_close_date.tz_localize(None)
            if auto_close_date > self.end_date:
                raise ValidationError(
                        f'autoclose date {auto_close_date} greater than end date {self.end_date}.')
            self.auto_close_date = auto_close_date
        
        self.mktdata_type = MktDataType.PRICING
        self.asset_class = AssetClass.EQUITY
        self.instrument_type = -1
        self.hashed_id = hash(
                self.symbol+str(self.exchange_name)+str(self))
        
    def get_tick_size(self):
        try:
            return round(1/self.tick_size, self.precision)
        except Exception:
            return 0

    def to_dict(self):
        d = super(Asset,self).to_dict()
        d['mult']=self.mult
        d['tick_size']=round(self.tick_size, TICK_PRECISION)
        d['auto_close_date']=self.auto_close_date
        d['asset_class']=self.asset_class
        d['instrument_type']=self.instrument_type
        d['fractional']=self.fractional
        return d

    def __reduce__(self):
        _, data = super(Asset, self).__reduce__()
        return(self.__class__,(*data,
                               self.mult,
                               round(self.tick_size, TICK_PRECISION),
                               self.auto_close_date,
                               self.fractional))

    cpdef bool is_asset(self):
        return True
    
    cpdef bool is_funded(self):
        if self.get_product_type() == ProductType.DELIVERY and self.instrument_type in (
                InstrumentType.SPOT, InstrumentType.FUNDS):
                return True
            
        return False

    cpdef bool is_index(self):
        return False
    
    cpdef bool is_opt(self):
        return self.instrument_type == InstrumentType.OPT
    
    cpdef bool is_futures(self):
        return self.instrument_type == InstrumentType.FUTURES
    
    cpdef bool is_rolling(self):
        if hasattr(self, 'roll_day') and self.roll_day >= 0:
            return True
        return False

    cpdef bool is_margin(self):
        return self.get_product_type() == ProductType.MARGIN

    cpdef bool is_intraday(self):
        return self.get_product_type() == ProductType.INTRADAY
    
    cpdef bool is_equity(self):
        return self.asset_class == AssetClass.EQUITY
    
    cpdef bool is_forex(self):
        return self.asset_class == AssetClass.FOREX
    
    cpdef bool is_etfs(self):
        return self.asset_class == AssetClass.EQUITY and self.instrument_type==InstrumentType.FUNDS
    
    cpdef bool is_crypto(self):
        return self.asset_class == AssetClass.CRYPTO

    cpdef bool is_commodity(self):
        return self.asset_class == AssetClass.COMMODITY

    cpdef bool is_call(self):
        return self.is_opt() and self.option_type == OptionType.CALL

    cpdef bool is_put(self):
        return self.is_opt() and self.option_type == OptionType.PUT
    
    def update_from_dict(self, dict data):
        import pandas as pd
        super(Asset, self).update_from_dict(data)
        if 'name' in data:
            self.name = data.pop('name')
            
        if 'start_date' in data:
            start_date = data['start_date']
            try:
                start_date = pd.Timestamp(start_date)
            except Exception:
                raise ValidationError(f'illegal start date {start_date}.')
            else:
                start_date = start_date.normalize()
                if start_date.tz:start_date=start_date.tz_localize(None)
            self.start_date = start_date
        
        if 'end_date' in data:
            end_date = data['end_date']
            try:
                end_date = pd.Timestamp(end_date)
            except Exception:
                raise ValidationError(f'illegal end date {end_date}.')
            else:
                end_date = end_date.normalize()
                if end_date.tz:end_date=end_date.tz_localize(None)
            self.end_date = end_date
            if 'auto_close_date' in data:
                auto_close_date = data.pop('auto_close_date')
                try:
                    auto_close_date = pd.Timestamp(auto_close_date)
                except Exception:
                    raise ValidationError(f'illegal date {auto_close_date}.')
                else:
                    auto_close_date = auto_close_date.normalize()
                    if auto_close_date.tz:
                        auto_close_date=auto_close_date.tz_localize(None)
                self.auto_close_date = auto_close_date
            else:
                self.auto_close_date = self.end_date
        
        if 'mult' in data:
            self.mult = data.pop('mult')
        if 'tick_size' in data:
            self.tick_size = round(data.pop('tick_size'),TICK_PRECISION)
        if 'can_trade' in data:
            self.can_trade = data.pop('can_trade')
        if 'fractional' in data:
            self.fractional = data.pop('fractional')
            
    def get_product_type(self):
        return ProductType.DELIVERY
    

cdef class Equity(Asset):
    '''
        Equity models exchange traded equities asset. This is 
        derived from the ``Asset`` class. This has the following 
        attributes in addition to the attributes of the parent 
        class. All attributes are read-only from a strategy code.
        
        +-----------------+----------------------+-------------------------------+
        |Attribute        | Type                 |Description                    |
        +=================+======================+===============================+
        |shortable        | ``bool``             |If shortable                   |
        +-----------------+----------------------+-------------------------------+
        |borrow_cost      | ``float``            |Borrow cost if shortable       |
        +-----------------+----------------------+-------------------------------+
        
        .. note::
            The ``asset_class`` is set to ``EQUITY`` and the 
            ``instrument_type`` is ``SPOT`` for equities and FUNDS for 
            ETFs.
    '''
    cdef readonly bool shortable
    cdef readonly float borrow_cost
    
    def __init__(self,
                 int sid,
                 object symbol,
                 object name=None,
                 object security_id=None,
                 object broker_symbol=None,
                 float upper_circuit=np.nan,
                 float lower_circuit=np.nan,
                 object start_date=None,
                 object end_date=None,
                 np.float64_t mult=1,
                 float tick_size=100,
                 bool can_trade=True,
                 bool shortable=True,
                 float borrow_cost=0,
                 object auto_close_date=None,
                 object exchange_name=None,
                 object calendar_name=None,
                 object ccy=CCY.LOCAL,
                 int instrument_type=InstrumentType.SPOT,
                 bool fractional=False):
        super(Equity, self).__init__(
                 sid=sid,
                 symbol=symbol,
                 name=name,
                 security_id=security_id,
                 broker_symbol=broker_symbol,
                 upper_circuit=upper_circuit,
                 lower_circuit=lower_circuit,
                 start_date=start_date,
                 end_date=end_date,
                 mult=mult,
                 tick_size=tick_size,
                 can_trade=can_trade,
                 auto_close_date=auto_close_date,
                 exchange_name=exchange_name,
                 calendar_name=calendar_name,
                 ccy=ccy,
                 fractional=fractional)
        self.borrow_cost=borrow_cost
        self.shortable=shortable
        self.asset_class = AssetClass.EQUITY
        self.instrument_type = instrument_type
        
    def __reduce__(self):
        _, data = super(Equity, self).__reduce__()
        return(self.__class__,(*data,
                               self.borrow_cost,
                               self.shortable))
    
    def to_dict(self):
        d = super(Equity,self).to_dict()
        d['borrow_cost']=self.borrow_cost
        d['shortable']=self.shortable
        return d
    
    def update_from_dict(self, dict data):
        super(Equity, self).update_from_dict(data)
        
        if 'borrow_cost' in data:
            self.borrow_cost = data.pop('borrow_cost')
        if 'shortable' in data:
            self.shortable = data.pop('shortable')
        
        if 'instrument_type' in data:
            self.instrument_type = data.pop('instrument_type')
            
    def __str__(self):
        if self.instrument_type==InstrumentType.FUNDS:
            return '%s(%s:%s [%d])' % ('ETF', self.exchange_name, self.symbol, self.sid)
        else:
            return '%s(%s:%s [%d])' % ('Equity', self.exchange_name, self.symbol, self.sid)
        
cdef class EquityMargin(Equity):
    '''
        Margin traded equity product.
    '''
    def __init__(self,
                 int sid,
                 object symbol,
                 object name=None,
                 object security_id=None,
                 object broker_symbol=None,
                 float upper_circuit=np.nan,
                 float lower_circuit=np.nan,
                 object start_date=None,
                 object end_date=None,
                 np.float64_t mult=1,
                 float tick_size=100,
                 bool can_trade=True,
                 bool shortable=True,
                 float borrow_cost=0,
                 object auto_close_date=None,
                 object exchange_name=None,
                 object calendar_name=None,
                 object ccy=CCY.LOCAL,
                 int instrument_type=InstrumentType.MARGIN,
                 bool fractional=False):
        super(EquityMargin, self).__init__(
                 sid=sid,
                 symbol=symbol,
                 name=name,
                 security_id=security_id,
                 broker_symbol=broker_symbol,
                 upper_circuit=upper_circuit,
                 lower_circuit=lower_circuit,
                 start_date=start_date,
                 end_date=end_date,
                 mult=mult,
                 tick_size=tick_size,
                 can_trade=can_trade,
                 shortable=shortable,
                 borrow_cost=borrow_cost,
                 auto_close_date=auto_close_date,
                 exchange_name=exchange_name,
                 calendar_name=calendar_name,
                 instrument_type=InstrumentType.MARGIN,
                 ccy=ccy,
                 fractional=fractional)
        
    def __str__(self):
        return '%s(%s:%s [%d])' % ('EquityMargin', self.exchange_name, self.symbol, self.sid)
    
    def get_product_type(self):
        return ProductType.MARGIN
    
cdef class EquityIntraday(Equity):
    '''
        Intraday margin traded equity product.
    '''
    def __init__(self,
                 int sid,
                 object symbol,
                 object name=None,
                 object security_id=None,
                 object broker_symbol=None,
                 float upper_circuit=np.nan,
                 float lower_circuit=np.nan,
                 object start_date=None,
                 object end_date=None,
                 np.float64_t mult=1,
                 float tick_size=100,
                 bool can_trade=True,
                 bool shortable=True,
                 float borrow_cost=0,
                 object auto_close_date=None,
                 object exchange_name=None,
                 object calendar_name=None,
                 object ccy=CCY.LOCAL,
                 int instrument_type=InstrumentType.MARGIN,
                 bool fractional=False):
        super(EquityIntraday, self).__init__(
                 sid=sid,
                 symbol=symbol,
                 name=name,
                 security_id=security_id,
                 broker_symbol=broker_symbol,
                 upper_circuit=upper_circuit,
                 lower_circuit=lower_circuit,
                 start_date=start_date,
                 end_date=end_date,
                 mult=mult,
                 tick_size=tick_size,
                 can_trade=can_trade,
                 shortable=shortable,
                 borrow_cost=borrow_cost,
                 auto_close_date=auto_close_date,
                 exchange_name=exchange_name,
                 calendar_name=calendar_name,
                 instrument_type=InstrumentType.MARGIN,
                 ccy=ccy,
                 fractional=fractional)
        
    def __str__(self):
        return '%s(%s:%s [%d])' % ('EquityIntraday', self.exchange_name, self.symbol, self.sid)
    
    def get_product_type(self):
        return ProductType.INTRADAY
        

cdef class CFDAsset(Asset):
    '''
        Assets packaged as CFD for equity-likes. Multiplier is set to 1.0.
    '''
    cdef readonly float initial_margin
    cdef readonly float maintenance_margin
    
    def __init__(self,
                 int sid,
                 object symbol,
                 object name=None,
                 object security_id=None,
                 object broker_symbol=None,
                 float upper_circuit=np.nan,
                 float lower_circuit=np.nan,
                 object start_date=None,
                 object end_date=None,
                 np.float64_t mult=1,
                 float tick_size=100,
                 bool can_trade=True,
                 float initial_margin=0,
                 float maintenance_margin=0,
                 object auto_close_date=None,
                 object exchange_name=None,
                 object calendar_name=None,
                 object ccy=CCY.LOCAL,
                 bool fractional=False):
        super(CFDAsset, self).__init__(
                 sid=sid,
                 symbol=symbol,
                 name=name,
                 security_id=security_id,
                 broker_symbol=broker_symbol,
                 upper_circuit=upper_circuit,
                 lower_circuit=lower_circuit,
                 start_date=start_date,
                 end_date=end_date,
                 mult=mult,
                 tick_size=tick_size,
                 can_trade=can_trade,
                 auto_close_date=auto_close_date,
                 exchange_name=exchange_name,
                 calendar_name=calendar_name,
                 ccy=ccy,
                 fractional=fractional)
        self.asset_class = AssetClass.EQUITY
        self.instrument_type = InstrumentType.CFD
        self.initial_margin=0
        self.maintenance_margin=0
        
    def __reduce__(self):
        _, data = super(CFDAsset, self).__reduce__()
        return(self.__class__,(*data,
                               self.initial_margin,
                               self.maintenance_margin))
        
    def to_dict(self):
        d = super(CFDAsset,self).to_dict()
        d['initial_margin']=self.initial_margin
        d['maintenance_margin']=self.maintenance_margin
        return d
    
    def update_from_dict(self, dict data):
        super(CFDAsset, self).update_from_dict(data)
        if 'initial_margin' in data:
            self.initial_margin = data.pop('initial_margin')
        if 'maintenance_margin' in data:
            self.maintenance_margin = data.pop('maintenance_margin')
            
    def get_product_type(self):
        return ProductType.MARGIN

cdef class Futures(Asset):
    '''
        Futures class models exchange traded futures. This is 
        derived from the ``Asset`` class. This has the following 
        attributes in addition to the attributes of the parent 
        class. All attributes are read-only from a strategy code.
        
        +------------------+----------------------+-------------------------------+
        |Attribute         | Type                 |Description                    |
        +==================+======================+===============================+
        |underlying        | ``str``              |Symbol of underlying asset     |
        +------------------+----------------------+-------------------------------+
        |root              | ``str``              |Root part of the symbol        |
        +------------------+----------------------+-------------------------------+
        |roll_day          | ``int``              |Roll period if rolling asset   |
        +------------------+----------------------+-------------------------------+
        |expiry_date       | ``pandas.Timestamp`` |Date of expiry                 |
        +------------------+----------------------+-------------------------------+
        |initial_margin    | ``float``            |Initial margin                 |
        +------------------+----------------------+-------------------------------+
        |maintenance_margin| ``float``            |Maintenance margin             |
        +------------------+----------------------+-------------------------------+
        
        .. note::
            The ``asset_class`` is set to ``EQUITY`` and the 
            ``instrument_type`` is ``FUTURES``. The ``roll_day`` parameter
            determines if the asset is a rolling asset (i.e. should be
            rolled automatically in backtest if value is greater than 
            -1). The ``roll_day`` is the offset from the ``expiry_date`` 
            to determine the roll date.
    '''
    cdef readonly object underlying
    cdef readonly object root
    cdef readonly int roll_day
    cdef readonly object expiry_date
    cdef readonly float initial_margin
    cdef readonly float maintenance_margin

    def __init__(self,
                 int sid,
                 object symbol,
                 object underlying=None,
                 object root=None,
                 object name=None,
                 object security_id=None,
                 object broker_symbol=None,
                 float upper_circuit=np.nan,
                 float lower_circuit=np.nan,
                 object start_date=None,
                 object end_date=None,
                 object expiry_date=None,
                 int roll_day=-1,           # -1 means no rolling
                 np.float64_t mult=1,
                 float tick_size=100,
                 bool can_trade=True,
                 float initial_margin=0,
                 float maintenance_margin=0,
                 object auto_close_date=None,
                 object exchange_name=None,
                 object calendar_name=None,
                 object ccy=CCY.LOCAL,
                 bool fractional=False):
        import pandas as pd
        from blueshift.interfaces.assets.utils import get_fut_occ_sym

        if not symbol and expiry_date and underlying:
            try:
                expiry_date = pd.Timestamp(expiry_date)
                symbol = get_fut_occ_sym(underlying, expiry_date)
            except Exception as e:
                msg = f'failed to infer symbol from underlying:{underlying} and expiry:{expiry_date}:{str(e)}'
                raise ValidationError(msg)

        super(Futures, self).__init__(
                 sid=sid,
                 symbol=symbol,
                 name=name,
                 security_id=security_id,
                 broker_symbol=broker_symbol,
                 upper_circuit=upper_circuit,
                 lower_circuit=lower_circuit,
                 start_date=start_date,
                 end_date=end_date,
                 mult=mult,
                 tick_size=tick_size,
                 can_trade=can_trade,
                 auto_close_date=auto_close_date,
                 exchange_name=exchange_name,
                 calendar_name=calendar_name,
                 ccy=ccy,
                 fractional=fractional)
        from blueshift.interfaces.assets.utils import get_occ_fut_details
        und = exp = None
        if 'FUT' in self.symbol:
            und, exp = get_occ_fut_details(self.symbol)
        
        self.underlying = underlying or und
        
        if root is None:
            root = underlying
        
        self.root = root
        
        expiry_date = expiry_date or exp
        if expiry_date is None:
            raise ValidationError('expiry date must be specified.')
        try:
            # no localization or normalize
            expiry_date = pd.Timestamp(expiry_date)
        except Exception:
            raise ValidationError(f'illegal expiry date {expiry_date}.')
        else:
            expiry_date=expiry_date.tz_localize(None)
        self.expiry_date = expiry_date
        
        self.roll_day = roll_day
        
        if self.end_date > self.expiry_date:
            self.end_date = self.expiry_date
        if self.auto_close_date > self.expiry_date:
            self.auto_close_date = self.expiry_date
        
        self.asset_class = AssetClass.EQUITY
        self.instrument_type = InstrumentType.FUTURES
        self.initial_margin=initial_margin
        self.maintenance_margin=maintenance_margin

    def __reduce__(self):
        _, data = super(Futures, self).__reduce__()
        return(self.__class__,(*data,
                               self.root,
                               self.underlying,
                               self.expiry_date,
                               self.roll_day,
                               self.initial_margin,
                               self.maintenance_margin))

    def to_dict(self):
        d = super(Futures,self).to_dict()
        d['root']=self.root
        d['underlying']=self.underlying
        d['expiry_date']=self.expiry_date
        d['roll_day']=self.roll_day
        d['initial_margin']=self.initial_margin
        d['maintenance_margin']=self.maintenance_margin
        return d
    
    def update_from_dict(self, dict data):
        import pandas as pd

        super(Futures, self).update_from_dict(data)
        if 'expiry_date' in data:
            expiry_date = data.pop('expiry_date')
            try:
                expiry_date = pd.Timestamp(expiry_date)
            except Exception:
                raise ValidationError(f'illegal expiry date {expiry_date}.')
            else:
                expiry_date=expiry_date.tz_localize(None)
                self.expiry_date = expiry_date
            
            if 'roll_day' in data:
                try:
                    self.roll_day = int(data.pop('roll_day'))
                except Exception:
                    raise ValidationError(
                            f'illegal roll_day parameter, must be integer.')
            
        if self.end_date > self.expiry_date:
            self.end_date = self.expiry_date
        if self.auto_close_date > self.expiry_date:
            self.auto_close_date = self.expiry_date
                
        if 'initial_margin' in data:
            self.initial_margin = data.pop('initial_margin')
        if 'maintenance_margin' in data:
            self.maintenance_margin = data.pop('maintenance_margin')

cdef class EquityFutures(Futures):
    """ Equity futures models exchange traded equities futures.  """
    pass

cdef class Forex(Asset):
    """
        Forex models for margin traded forex. This is 
        derived from the ``Asset`` class. This has the following 
        attributes in addition to the attributes of the parent 
        class. All attributes are read-only from a strategy code.
        
        +------------------+----------------------+-------------------------------+
        |Attribute         | Type                 |Description                    |
        +==================+======================+===============================+
        |ccy_pair          | ``str``              |currency pair (XXX/YYY)        |
        +------------------+----------------------+-------------------------------+
        |base_ccy          | ``str``              |Base currency (XXX)            |
        +------------------+----------------------+-------------------------------+
        |quote_ccy         | ``str``              |Quote currency (YYY)           |
        +------------------+----------------------+-------------------------------+
        |buy_roll          | ``float``            |Roll cost for a long position  |
        +------------------+----------------------+-------------------------------+
        |sell_roll         | ``float``            |Roll cost for a short position |
        +------------------+----------------------+-------------------------------+
        |initial_margin    | ``float``            |Initial margin                 |
        +------------------+----------------------+-------------------------------+
        |maintenance_margin| ``float``            |Maintenance margin             |
        +------------------+----------------------+-------------------------------+
        
        .. note::
            The ``asset_class`` is set to ``FOREX`` and the 
            ``instrument_type`` is ``MARGIN``. 
            
            The roll costs are for 1 unit - the overnight roll is 
            calculated as roll cost multiplied by the quantity 
            in open position.
    """
    cdef readonly object ccy_pair
    cdef readonly object base_ccy
    cdef readonly object quote_ccy
    cdef readonly float buy_roll
    cdef readonly float sell_roll
    cdef readonly float initial_margin
    cdef readonly float maintenance_margin

    def __init__(self,
                 int sid,
                 object symbol,
                 object ccy_pair=None,
                 object base_ccy=None,
                 object quote_ccy=None,
                 object name=None,
                 object security_id=None,
                 object broker_symbol=None,
                 float upper_circuit=np.nan,
                 float lower_circuit=np.nan,
                 object start_date=None,
                 object end_date=None,
                 np.float64_t mult=1,
                 float tick_size=10000,
                 bool can_trade=True,
                 float buy_roll=0,
                 float sell_roll=0,
                 float initial_margin = 0,
                 float maintenance_margin = 0,
                 object auto_close_date=None,
                 object exchange_name=None,
                 object calendar_name=None,
                 object ccy=CCY.LOCAL,
                 bool fractional=False):
        from blueshift.interfaces.assets.utils import get_ccy_details
        super(Forex, self).__init__(
                 sid=sid,
                 symbol=symbol,
                 name=name,
                 security_id=security_id,
                 broker_symbol=broker_symbol,
                 upper_circuit=upper_circuit,
                 lower_circuit=lower_circuit,
                 start_date=start_date,
                 end_date=end_date,
                 mult=mult,
                 tick_size=tick_size,
                 can_trade=can_trade,
                 auto_close_date=auto_close_date,
                 exchange_name=exchange_name,
                 calendar_name=calendar_name,
                 ccy=ccy,
                 fractional=fractional)
        base, quote, pair = get_ccy_details(self.symbol)
        self.ccy_pair = ccy_pair or pair
        self.base_ccy = base_ccy or base
        self.quote_ccy = quote_ccy or quote
        
        if self.quote_ccy in ['JPY', CCY.JPY]:
            self.tick_size=100
        
        ccy = self.quote_ccy
        if isinstance(ccy, CCY):
            self.ccy = ccy
        else:
            try:
                self.ccy=CCY[ccy.upper()]
            except Exception:
                raise ValidationError(f'unsupported currency {ccy}')
            
        self.asset_class = AssetClass.FOREX
        self.instrument_type = InstrumentType.MARGIN
        self.buy_roll = buy_roll
        self.sell_roll = sell_roll
        self.initial_margin = initial_margin
        self.maintenance_margin = maintenance_margin

    def __reduce__(self):
        _, data = super(Forex, self).__reduce__()
        return(self.__class__,(*data,
                               self.ccy_pair,
                               self.base_ccy,
                               self.quote_ccy,
                               self.buy_roll,
                               self.sell_roll,
                               self.initial_margin,
                               self.maintenance_margin))

    def to_dict(self):
        d = super(Forex,self).to_dict()
        d['ccy_pair']=self.ccy_pair
        d['base_ccy']=self.base_ccy
        d['quote_ccy']=self.quote_ccy
        d['buy_roll']=self.buy_roll
        d['sell_roll']=self.sell_roll
        d['initial_margin']=self.initial_margin
        d['maintenance_margin']=self.maintenance_margin
        return d

    def update_from_dict(self, dict data):
        super(Forex, self).update_from_dict(data)
        if 'ccy_pair' in data:
            self.ccy_pair = data.pop('ccy_pair')
        if 'base_ccy' in data:
            self.base_ccy = data.pop('base_ccy')
        if 'quote_ccy' in data:
            self.quote_ccy = data.pop('quote_ccy')
            self.ccy = self.quote_ccy
        if 'buy_roll' in data:
            self.buy_roll = data.pop('buy_roll')
        if 'sell_roll' in data:
            self.sell_roll = data.pop('sell_roll')
        if 'initial_margin' in data:
            self.initial_margin = data.pop('initial_margin')
        if 'maintenance_margin' in data:
            self.maintenance_margin = data.pop('maintenance_margin')
            
    def get_product_type(self):
        return ProductType.MARGIN
            
cdef class Crypto(Asset):
    """
        Crypto models for margin traded assets. This is 
        derived from the ``Asset`` class. This has the following 
        attributes in addition to the attributes of the parent 
        class. All attributes are read-only from a strategy code.
        
        +------------------+----------------------+-------------------------------+
        |Attribute         | Type                 |Description                    |
        +==================+======================+===============================+
        |ccy_pair          | ``str``              |currency pair (XXX/YYY)        |
        +------------------+----------------------+-------------------------------+
        |base_ccy          | ``str``              |Base currency (XXX)            |
        +------------------+----------------------+-------------------------------+
        |quote_ccy         | ``str``              |Quote currency (YYY)           |
        +------------------+----------------------+-------------------------------+
        |buy_roll          | ``float``            |Roll cost for a long position  |
        +------------------+----------------------+-------------------------------+
        |sell_roll         | ``float``            |Roll cost for a short position |
        +------------------+----------------------+-------------------------------+
        |initial_margin    | ``float``            |Initial margin                 |
        +------------------+----------------------+-------------------------------+
        |maintenance_margin| ``float``            |Maintenance margin             |
        +------------------+----------------------+-------------------------------+
        
        .. note::
            The ``asset_class`` is set to ``FOREX`` and the 
            ``instrument_type`` is ``MARGIN``. 
            
            The roll costs are for 1 unit - the overnight roll is 
            calculated as roll cost multiplied by the quantity 
            in open position.
    """
    cdef readonly object ccy_pair
    cdef readonly object base_ccy
    cdef readonly object quote_ccy
    cdef readonly float buy_roll
    cdef readonly float sell_roll
    cdef readonly float initial_margin
    cdef readonly float maintenance_margin

    def __init__(self,
                 int sid,
                 object symbol,
                 object ccy_pair=None,
                 object base_ccy=None,
                 object quote_ccy=None,
                 object name=None,
                 object security_id=None,
                 object broker_symbol=None,
                 float upper_circuit=np.nan,
                 float lower_circuit=np.nan,
                 object start_date=None,
                 object end_date=None,
                 np.float64_t mult=1,
                 float tick_size=10000,
                 bool can_trade=True,
                 float buy_roll=0,
                 float sell_roll=0,
                 float initial_margin = 0,
                 float maintenance_margin = 0,
                 object auto_close_date=None,
                 object exchange_name=None,
                 object calendar_name=None,
                 object ccy=CCY.LOCAL,
                 bool fractional=True):
        from blueshift.interfaces.assets.utils import get_ccy_details
        super(Crypto, self).__init__(
                 sid=sid,
                 symbol=symbol,
                 name=name,
                 security_id=security_id,
                 broker_symbol=broker_symbol,
                 upper_circuit=upper_circuit,
                 lower_circuit=lower_circuit,
                 start_date=start_date,
                 end_date=end_date,
                 mult=mult,
                 tick_size=tick_size,
                 can_trade=can_trade,
                 auto_close_date=auto_close_date,
                 exchange_name=exchange_name,
                 calendar_name=calendar_name,
                 ccy=ccy,
                 fractional=fractional)
        base, quote, pair = get_ccy_details(self.symbol)
        self.ccy_pair = ccy_pair or pair
        self.base_ccy = base_ccy or base
        self.quote_ccy = quote_ccy or quote
        
        ccy = self.quote_ccy
        if isinstance(ccy, CCY):
            self.ccy = ccy.name
        elif isinstance(ccy, str):
            self.ccy = ccy
        else:
            raise ValidationError(f'unknown type for currency {ccy}')
            
        self.asset_class = AssetClass.CRYPTO
        self.instrument_type = InstrumentType.MARGIN
        self.buy_roll = buy_roll
        self.sell_roll = sell_roll
        self.initial_margin = initial_margin
        self.maintenance_margin = maintenance_margin

    def __reduce__(self):
        _, data = super(Crypto, self).__reduce__()
        return(self.__class__,(*data,
                               self.ccy_pair,
                               self.base_ccy,
                               self.quote_ccy,
                               self.buy_roll,
                               self.sell_roll,
                               self.initial_margin,
                               self.maintenance_margin))

    def to_dict(self):
        d = super(Crypto,self).to_dict()
        d['ccy_pair']=self.ccy_pair
        d['base_ccy']=self.base_ccy
        d['quote_ccy']=self.quote_ccy
        d['buy_roll']=self.buy_roll
        d['sell_roll']=self.sell_roll
        d['initial_margin']=self.initial_margin
        d['maintenance_margin']=self.maintenance_margin
        return d

    def update_from_dict(self, dict data):
        super(Crypto, self).update_from_dict(data)
        if 'ccy_pair' in data:
            self.ccy_pair = data.pop('ccy_pair')
        if 'base_ccy' in data:
            self.base_ccy = data.pop('base_ccy')
        if 'quote_ccy' in data:
            self.quote_ccy = data.pop('quote_ccy')
            self.ccy = self.quote_ccy
        if 'buy_roll' in data:
            self.buy_roll = data.pop('buy_roll')
        if 'sell_roll' in data:
            self.sell_roll = data.pop('sell_roll')
        if 'initial_margin' in data:
            self.initial_margin = data.pop('initial_margin')
        if 'maintenance_margin' in data:
            self.maintenance_margin = data.pop('maintenance_margin')
            
    def get_product_type(self):
        return ProductType.MARGIN

cdef class FXFutures(Forex):
    """ FX Futures - exchange traded"""
    
    def __init__(self,
                 int sid,
                 object symbol,
                 object ccy_pair=None,
                 object base_ccy=None,
                 object quote_ccy=None,
                 object name=None,
                 object security_id=None,
                 object broker_symbol=None,
                 float upper_circuit=np.nan,
                 float lower_circuit=np.nan,
                 object start_date=None,
                 object end_date=None,
                 np.float64_t mult=1,
                 float tick_size=10000,
                 bool can_trade=True,
                 float buy_roll=0,
                 float sell_roll=0,
                 float initial_margin = 0,
                 float maintenance_margin = 0,
                 object auto_close_date=None,
                 object exchange_name=None,
                 object calendar_name=None,
                 object ccy=CCY.LOCAL,
                 bool fractional=False):
        super(FXFutures, self).__init__(
                 sid=sid,
                 symbol=symbol,
                 ccy_pair = ccy_pair,
                 base_ccy = base_ccy,
                 quote_ccy = quote_ccy,
                 name=name,
                 security_id=security_id,
                 broker_symbol=broker_symbol,
                 upper_circuit=upper_circuit,
                 lower_circuit=lower_circuit,
                 start_date=start_date,
                 end_date=end_date,
                 mult=mult,
                 tick_size=tick_size,
                 can_trade=can_trade,
                 buy_roll=buy_roll,
                 sell_roll=sell_roll,
                 initial_margin=initial_margin,
                 maintenance_margin=maintenance_margin,
                 auto_close_date=auto_close_date,
                 exchange_name=exchange_name,
                 calendar_name=calendar_name,
                 ccy=ccy,
                 fractional=fractional)
        self.instrument_type = InstrumentType.FUTURES

cdef class SpotFX(Forex):
    """ Spot FX, fully funded trading. """
    
    def __init__(self,
                 int sid,
                 object symbol,
                 object ccy_pair=None,
                 object base_ccy=None,
                 object quote_ccy=None,
                 object name=None,
                 object security_id=None,
                 object broker_symbol=None,
                 float upper_circuit=np.nan,
                 float lower_circuit=np.nan,
                 object start_date=None,
                 object end_date=None,
                 np.float64_t mult=1,
                 float tick_size=10000,
                 bool can_trade=True,
                 float buy_roll=0,
                 float sell_roll=0,
                 object auto_close_date=None,
                 object exchange_name=None,
                 object calendar_name=None,
                 object ccy=CCY.LOCAL,
                 bool fractional=False):
        super(SpotFX, self).__init__(
                 sid=sid,
                 symbol=symbol,
                 ccy_pair = ccy_pair,
                 base_ccy = base_ccy,
                 quote_ccy = quote_ccy,
                 name=name,
                 security_id=security_id,
                 broker_symbol=broker_symbol,
                 upper_circuit=upper_circuit,
                 lower_circuit=lower_circuit,
                 start_date=start_date,
                 end_date=end_date,
                 mult=mult,
                 tick_size=tick_size,
                 can_trade=can_trade,
                 buy_roll=buy_roll,
                 sell_roll=sell_roll,
                 initial_margin=0,
                 maintenance_margin=0,
                 auto_close_date=auto_close_date,
                 exchange_name=exchange_name,
                 calendar_name=calendar_name,
                 ccy=ccy,
                 fractional=fractional)
        self.instrument_type = InstrumentType.SPOT

cdef class Option(Futures):
    """
        Exchange traded equity options. This is derived from 
        the ``EquityFutures`` class. This has the following 
        attributes in addition to the attributes of the parent 
        class. All attributes are read-only from a strategy code.
        
        +------------------+----------------------+-------------------------------+
        |Attribute         | Type                 |Description                    |
        +==================+======================+===============================+
        |strike            | ``float``            |Option strike                  |
        +------------------+----------------------+-------------------------------+
        |strike_type       | :ref:`StrikeType`    |Option strike type             |
        +------------------+----------------------+-------------------------------+
        |option_type       | :ref:`OptionType`    |Type of option                 |
        +------------------+----------------------+-------------------------------+
        
        .. note::
            The ``asset_class`` is set to ``EQUITY`` and the 
            ``instrument_type`` is ``OPT``. The parameter 
            ``strike_type`` determines the offset from the ATM strike,
            and can be either absolute, relative from ATM (e.g. +100 
            or -100) or in terms of delta (multiplied by 100, e.g. 
            25D or -10D).
    """
    cdef readonly float strike
    cdef readonly int strike_type
    cdef readonly int option_type

    def __init__(self,
                 int sid,
                 object symbol,
                 object underlying=None,
                 object root=None,
                 object name=None,
                 object security_id=None,
                 object broker_symbol=None,
                 float upper_circuit=np.nan,
                 float lower_circuit=np.nan,
                 object start_date=None,
                 object end_date=None,
                 object expiry_date=None,
                 object roll_day=-1,
                 float strike = 0,
                 int strike_type = StrikeType.ABS,
                 np.float64_t mult=1,
                 float tick_size=100,
                 bool can_trade=True,
                 object option_type = None,
                 float initial_margin = 0,
                 float maintenance_margin = 0,
                 object auto_close_date=None,
                 object exchange_name=None,
                 object calendar_name=None,
                 object ccy=CCY.LOCAL,
                 bool fractional=False):
        from blueshift.interfaces.assets.utils import get_occ_opt_details, get_opt_occ_sym

        if not symbol and expiry_date and underlying and option_type and strike:
            opt_type = ''
            try:
                if isinstance(option_type, str):
                    if option_type in ('CE','CA','C','CALL'):
                        opt_type = OptionType.CALL
                    elif option_type in ('PE','PA','P','PUT'):
                        opt_type = OptionType.PUT
                elif option_type in (OptionType.CALL, OptionType.PUT):
                    opt_type = option_type
                else:
                    raise ValueError(f'could not infer option type, got {type(option_type)}, {option_type}')

                symbol = get_opt_occ_sym(underlying, expiry_date, opt_type, strike)
            except Exception as e:
                msg = f'failed to infer option symbol from underlying:{underlying}, expiry:{expiry_date}, '
                msg += f'type:{option_type} and strike:{strike}:{str(e)}'
                raise ValidationError(msg)
        
        super(Option, self).__init__(
                 sid=sid,
                 underlying=underlying,
                 symbol=symbol,
                 root=root,
                 name=name,
                 security_id=security_id,
                 broker_symbol=broker_symbol,
                 upper_circuit=upper_circuit,
                 lower_circuit=lower_circuit,
                 start_date=start_date,
                 end_date=end_date,
                 expiry_date=expiry_date,
                 roll_day=roll_day,
                 mult=mult,
                 tick_size=tick_size,
                 can_trade=can_trade,
                 initial_margin=initial_margin,
                 maintenance_margin=maintenance_margin,
                 auto_close_date=auto_close_date,
                 exchange_name=exchange_name,
                 calendar_name=calendar_name,
                 ccy=ccy,
                 fractional=fractional)
        opt=None
        
        if 'CE' in self.symbol or 'PE' in self.symbol:
            _, _, opt, stk = get_occ_opt_details(self.symbol)
        
        self.strike = strike
        
        self.asset_class = AssetClass.EQUITY
        self.instrument_type = InstrumentType.OPT
        
        if isinstance(strike_type, str):
            strike_type = strike_type.upper()
            if strike_type == 'ABS':
                self.strike_type = StrikeType.ABS
                self.strike = to_int_or_float(self.strike)
            elif strike_type == 'REL':
                self.strike_type = StrikeType.REL
            elif strike_type == 'DEL':
                self.strike_type = StrikeType.DEL
            elif strike_type == 'PREMIUM':
                self.strike_type = StrikeType.PREMIUM
            else:
                raise ValueError("strike type is not valid.")
        else:
            self.strike_type = int(strike_type)

        option_type = option_type or opt
        if option_type is None:
            raise ValidationError('option type must be specified.')
        if isinstance(option_type, str):
            option_type = option_type.upper()
            if option_type in ('CE','CA','C','CALL'):
                self.option_type = OptionType.CALL
            elif option_type in ('PE','PA','P','PUT'):
                self.option_type = OptionType.PUT
            else:
                self.option_type = int(option_type)
        else:
            self.option_type = int(option_type)

    def __reduce__(self):
        _, data = super(Option, self).__reduce__()
        return(self.__class__,(*data,
                               self.option_type,
                               self.strike,
                               self.strike_type))

    def to_dict(self):
        d = super(Option,self).to_dict()
        d['strike']=self.strike
        d['strike_type']=self.strike_type
        d['option_type']=self.option_type
        return d

    
    def update_from_dict(self, dict data):
        super(Option, self).update_from_dict(data)
        if 'strike' in data:
            self.strike = data.pop('strike')
        if 'strike_type' in data:
            strike_type = data.pop('strike_type')
            if isinstance(strike_type, str):
                strike_type = strike_type.upper()
                if strike_type == 'ABS':
                    self.strike_type = StrikeType.ABS
                elif strike_type == 'REL':
                    self.strike_type = StrikeType.REL
                elif strike_type == 'DEL':
                    self.strike_type = StrikeType.DEL
                elif strike_type == 'PREMIUM':
                    self.strike_type = StrikeType.PREMIUM
                else:
                    raise ValueError("strike type is not valid.")
            else:
                self.strike_type = int(strike_type)
                
        if 'option_type' in data:
            option_type = data.pop('option_type')
            if isinstance(option_type, str):
                option_type = option_type.upper()
                if option_type in ('CE','CA','C','CALL'):
                    self.option_type = OptionType.CALL
                elif option_type in ('PE','PA','P','PUT'):
                    self.option_type = OptionType.PUT
                else:
                    raise ValueError("option type is not valid.")
            else:
                self.option_type = int(option_type)


cdef class EquityOption(Option):
    """ exchange traded equity options. """
    pass