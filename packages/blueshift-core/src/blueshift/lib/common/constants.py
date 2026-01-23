import numpy as np
from enum import Enum
import numbers

# common constants
KB = 1 << 10
MB = KB << 10
PRECISION = 10
DISPLAY_PRECISION = 5
CONNECTION_TIMEOUT = 60
ALMOST_ZERO = 0.00000009
NANO_SECOND = 1000000000
MICRO_SECOND = 1000000
MILLI_SECOND = 1000
NANO_IN_SECONDS = 1000000000
MINUTE_IN_NANO = 60*NANO_IN_SECONDS
HOUR_IN_NANO = 60*MINUTE_IN_NANO
DAY_IN_NANO = 24*HOUR_IN_NANO
ANNUALIZATION_FACTOR = 252
APPROX_BDAYS_PER_MONTH = 21
APPROX_BDAYS_PER_YEAR = 252
MONTHS_PER_YEAR = 12
WEEKS_PER_YEAR = 52
QTRS_PER_YEAR = 4
    
class Frequency:
    """ A class to handle frequency. """
    _VALID_INPUTS = {"minute":"m1",
                     "min":"m1",
                     "1min":"m1",
                     "m":"m1",
                     "1m":"m1",
                     "minutes":"m1",
                     "m1":"m1",
                     "t":"m1",
                     "1t":"m1",
                     "5min":"m5",
                     "5m":"m5",
                     "5minute":"m5",
                     "5minutes":"m5",
                     "m5":"m5",
                     "5t":"m5",
                     "10min":"m10",
                     "10m":"m10",
                     "10minute":"m10",
                     "10minutes":"m10",
                     "m10":"m10",
                     "10t":"m10",
                     "15min":"m15",
                     "15m":"m15",
                     "15minute":"m15",
                     "15minutes":"m15",
                     "m15":"m15",
                     "15t":"m15",
                     "30min":"m30",
                     "30m":"m30",
                     "30minute":"m30",
                     "30minutes":"m30",
                     "m30":"m30",
                     "30t":"m30",
                     "h":"H",
                     "1h":"H",
                     "hour":"H",
                     "hourly":"H",
                     "1hour":"H",
                     "d":"D",
                     "1d":"D",
                     "d1":"D",
                     "day":"D",
                     "1day":"D",
                     "daily":"D",
                     "weekly":"W",
                     "W":"W",
                     "monthly":"M",
                     "M":"M",
                     "quarterly":"Q",
                     "3M":"Q",
                     "Q":"Q",
                     "annually":"A",
                     "A":"A",
                     "Y":"A",
                     "tick":"TBT",
                     "tbt":"TBT",
                     }
    
    _FREQUENCY = ["m1", "m5", "m10", "m15", "m30", "H", "D", 
                  "W", "M", "Q", "A", "TBT"]
    
    _FREQUENCY_DES = {"m1":"1 minute",
                      "m5":"5 minute", 
                      "m10":"10 minute", 
                      "m15":"15 minute", 
                      "m30":"30 minute", 
                      "H":"1 hour", 
                      "D":"daily",
                      "W":"weekly",
                      "M":"monthly",
                      "Q":"quarterly",
                      "A":"annually",
                      "TBT":"tick-by-tick",
                      }
    
    _PERIOD = {"m1":"min",
               "m5":"5min", 
               "m10":"10min", 
               "m15":"15min", 
               "m30":"30min", 
               "H":"h",
               "D":"D",
               "W":"W",
               "M":"M",
               "Q":"Q",
               "A":"A",
               "TBT": None,
               }
    
    _PERIODS_NANOS = {"m1":MINUTE_IN_NANO,
                      "m5":5*MINUTE_IN_NANO,
                      "m10":10*MINUTE_IN_NANO,
                      "m15":15*MINUTE_IN_NANO,
                      "m30":30*MINUTE_IN_NANO,
                      "H":HOUR_IN_NANO,
                      "D":DAY_IN_NANO,
                      "W":7*DAY_IN_NANO,
                      "M":30*DAY_IN_NANO,
                      "Q":90*DAY_IN_NANO,
                      "A":365*DAY_IN_NANO,
                      "TBT": None,
                      }
    
    @classmethod
    def _from_timedelta(cls, dt):
        days = dt.days
        seconds = dt.seconds
        
        if days > 0 and days == 1 and seconds == 0:
            return 'D'
        elif days == 0 and seconds > 0:
            hour = seconds/3600
            if hour > 0 and hour == int(hour):
                if int(hour) == 1:
                    return 'H'
                return f'{int(hour)}H'
            elif int(hour) == 0:
                minutes = seconds/60
                if minutes> 0 and minutes == int(minutes):
                    if int(minutes) == 1:
                        return 'T'
                    return f'{int(minutes)}T'
        return ':cannot parse timedelta.'
    
    def __new__(cls, name=None):
        if isinstance(name, Frequency):
            return name
        return super(Frequency, cls).__new__(cls)
    
    def __init__(self, name=None):
        from pandas import Timedelta
        if isinstance(name, Frequency):
            return
        
        self._PERIODS_TIMEDELTA = {"m1":Timedelta(minutes=1), 
                          "m5":Timedelta(minutes=5),
                          "m10":Timedelta(minutes=10),
                          "m15":Timedelta(minutes=15),
                          "m30":Timedelta(minutes=30),
                          "H":Timedelta(hours=1),
                          "D":Timedelta(days=1),
                          "W":Timedelta(weeks=1),
                          "M":Timedelta(days=30),
                          "Q":Timedelta(days=90),
                          "A":Timedelta(days=365),
                          "TBT": None,
                          }
        
        if isinstance(name, numbers.Number) and \
            name == int(name) and name > 0: # type: ignore
            name = str(int(name))+'m' # type: ignore
        elif isinstance(name, Timedelta):
            name = self._from_timedelta(name)
        
        if name is None:
            name = 'D'
        elif isinstance(name, str) and \
            name in self._VALID_INPUTS:
            name = self._VALID_INPUTS[name]      
        elif isinstance(name, str) and \
            name.lower() in self._VALID_INPUTS:
            name = self._VALID_INPUTS[name.lower()]           
        else:
            raise ValueError(f"illegal frequency {name}.")
        
        self._name = name
        self._value = self._PERIODS_NANOS[name]
        
    @property
    def nano(self):
        return self._value
    
    @property
    def timedelta(self):
        return self._PERIODS_TIMEDELTA[self._name]
    
    @property
    def period(self):
        return self._PERIOD[self._name]
    
    @property
    def aligned(self):
        return self.period not in ['TBT','W','M','Q','A']
    
    @property
    def minute_bars(self):
        return self._value/MINUTE_IN_NANO
    
    def __str__(self):
        return f"Frequency:{self._FREQUENCY_DES[self._name]}"
    
    def __repr__(self):
        return self.__str__()
    
    def __hash__(self):
        return self._value
    
    def __int__(self):
        if not self._value:
            return 0
        return int(self._value/MINUTE_IN_NANO)
    
    def __eq__(self, obj):
        return int(self) == int(obj)
    
    def __lt__(self, obj):
        return int(self) < int(obj)
    
    def __gt__(self, obj):
        return int(self) > int(obj)
    
    def __le__(self, obj):
        return int(self) <= int(obj)
    
    def __ge__(self, obj):
        return int(self) >= int(obj)
    
_valid_inputs = {
            'LOCAL':('Local currency', None),
            'AED': ('United Arab Emirates dirham',2),
            'ARS': ('Argentine peso',2),
            'AUD': ('Australian dollar',2),
            'BRL': ('Brazilian real',2),
            'CAD': ('Canadian dollar',2),
            'CHF': ('Swiss franc',2),
            'CLP': ('Chilean peso',0),
            'CNY': ('Chinese yuan[8]',2),
            'CZK': ('Czech koruna',2),
            'DKK': ('Danish krone',2),
            'EUR': ('Euro',2),
            'GBP': ('Pound sterling',2),
            'HKD': ('Hong Kong dollar',2),
            'HUF': ('Hungarian forint',2),
            'IDR': ('Indonesian rupiah',2),
            'ILS': ('Israeli new shekel',2),
            'INR': ('Indian rupee',2),
            'JPY': ('Japanese yen',0),
            'KRW': ('South Korean won',0),
            'KWD': ('Kuwaiti dinar',3),
            'MXN': ('Mexican peso',2),
            'MYR': ('Malaysian ringgit',2),
            'NOK': ('Norwegian krone',2),
            'NZD': ('New Zealand dollar',2),
            'RUB': ('Russian ruble',2),
            'SAR': ('Saudi riyal',2),
            'SEK': ('Swedish krona/kronor',2),
            'SGD': ('Singapore dollar',2),
            'THB': ('Thai baht',2),
            'TRY': ('Turkish lira',2),
            'TWD': ('New Taiwan dollar',2),
            'USD': ('United States dollar',2),
            'USN': ('United States dollar (next day) (funds code)',2),
            'XAG': ('Silver (one troy ounce)',None),
            'XAU': ('Gold (one troy ounce)',None),
            'XBA': ('European Composite Unit (EURCO) (bond market unit)',None),
            'XBB': ('European Monetary Unit (E.M.U.-6) (bond market unit)',None),
            'XBC': ('European Unit of Account 9 (E.U.A.-9) (bond market unit)',None),
            'XBD': ('European Unit of Account 17 (E.U.A.-17) (bond market unit)',None),
            'XDR': ('Special drawing rights',None),
            'XPD': ('Palladium (one troy ounce)',None),
            'XPT': ('Platinum (one troy ounce)',None),
            'XXX': ('No currency',None),
            'ZAR': ('South African rand',2),
            'USDT': ('USD Tether',8),
            }

class Currency(Enum):
    """ Supported Currency types. """
    @property
    def ccy(self):
        return self.name
    
    @property
    def precision(self):
        return _valid_inputs[self.name][1]
        
    def __eq__(self, obj):
        if isinstance(obj, str):
            return self.name == obj.upper()
        if isinstance(obj, Currency):
            return super().__eq__(obj)
        else:
            raise ValueError('cannot compare currency with {}'.format(
                    type(obj)))
            
    def __hash__(self):
        return hash(self.name)
    
    def __str__(self):
        return f"Currency[{self.name}]"
    
    def __repr__(self):
        return self.__str__()

class CurrencyPair:
    
    def __new__(cls, code, *args, **kwargs):
        if isinstance(code, CurrencyPair):
            return code
        return super(CurrencyPair, cls).__new__(cls)
    
    def __init__(self, code, ccy=None, exchange=None):
        if isinstance(code, CurrencyPair):
            return
        
        if isinstance(exchange, str):
            exchange = exchange.upper()
        self._exchange = exchange
        
        if isinstance(code, Currency):
            if not ccy:
                raise ValueError('quote currency missing.')
            self._base = code
            self._quote = Currency(ccy)
            return
        
        if not isinstance(code, str):
            raise ValueError('must be Currency type of a string.')
        
        code = code.split(':')
        if len(code) > 1:
            if exchange and code[0].upper() != exchange:
                raise ValueError('exchange names do not match.')
            self._exchange = code[0].upper()
            code = code[1]
        else:
            code = code[0]
            
        pairs = code.split('/')
        if len(pairs) > 1:
            if ccy and ccy.upper() != pairs[1].upper():
                raise ValueError('quote currencies do not match.')
            self._quote = pairs[1]
            self._base = pairs[0]
        else:
            if not ccy:
                raise ValueError('quote currency is missing.')
            self._quote = ccy
            self._base = pairs[0]
            
        self._base = Currency(self._base)
        self._quote = Currency(self._quote)
        
    @property
    def pair(self) -> str:
        return self._base.ccy + '/' + self._quote.ccy # type: ignore
    
    @property
    def name(self) -> str:
        return self._base.name + ' vs. ' + self._quote.name # type: ignore
    
    @property
    def precision(self) -> int:
        return self._quote.precision # type: ignore
    
    @property
    def quote(self):
        return self._quote
    
    @property
    def base(self):
        return self._base
    
    @property
    def exchange(self):
        return self._exchange
                
    def __hash__(self):
        return hash(self.pair)
        
    def __eq__(self, obj):
        if isinstance(obj, CurrencyPair):
            return hash(self) == hash(obj)
        elif isinstance(obj, str):
            pairs = obj.split(':')[-1].split('/')
            if len(pairs) != 2:
                return False
            hash_ = hash(pairs[0].upper() + '/' + pairs[1].upper())
            return hash(self) == hash_
        else:
            raise ValueError('cannot compare currency pair with {}'.format(
                    type(obj)))
            
    def __str__(self):
        return self.pair
    
    def __repr__(self):
        return self.__str__()

OHLC_COLUMNS = ['open','high','low','close']
OHLCV_COLUMNS = ['open','high','low','close','volume']
OHLCV_DTYPE = {'open': np.float64, 
               'high': np.float64,
               'low': np.float64,
               'close': np.float64,
               'volume': np.int32}
OHLCVX_COLUMNS = ['open','high','low','close','volume','open_interest']
GREEK_COLUMNS = ['atmf','implied_vol','delta','gamma','vega','theta']
OHLCVX_DTYPE = {'open': np.float64, 
               'high': np.float64,
               'low': np.float64,
               'close': np.float64,
               'volume': np.int32,
               'open_interest': np.int32}

GREEK_COLUMNS = ['atmf','implied_vol','delta','gamma','vega','theta']
GREEK_DTYPE = {'atmf': np.float64, 
               'implied_vol': np.float64,
               'delta': np.float64,
               'gamma': np.float64,
               'vega': np.float64,
               'theta': np.float64,
               }

CCY = Currency('Currency', {k:k for k in _valid_inputs.keys()})
CCYPair = CurrencyPair


MAX_COLLECTION_SIZE = 10000
MAX_TICKERS_SIZE = 200
MAX_RETRIES = 30
LARGE_TIMEOUT = 2*CONNECTION_TIMEOUT