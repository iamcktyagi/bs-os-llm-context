from __future__ import annotations
from collections import OrderedDict
from time import time
from abc import ABC, abstractmethod
from collections.abc import Iterable
from io import StringIO
from collections import namedtuple
from typing import TYPE_CHECKING
from io import TextIOBase
import logging

from blueshift.lib.common.enums import AlgoMessageType, ChunkerType
from blueshift.lib.common.functions import create_message_envelope

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.interfaces.streaming import StreamingPublisher
else:
    import blueshift.lib.common.lazy_pandas as pd

class BlueshiftPositionDict(dict):
    def __init__(self, *args, **kwargs):
        from blueshift.lib.exceptions import PositionNotFound

        super().__init__(*args, **kwargs)
        self.PositionNotFound = PositionNotFound

    def __getitem__(self, key):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            raise self.PositionNotFound(f'No position found for {key}')
            
class BlueshiftOrderDict(dict):
    def __init__(self, *args, **kwargs):
        from blueshift.lib.exceptions import OrderNotFound

        super().__init__(*args, **kwargs)
        self.OrderNotFound = OrderNotFound

    def __getitem__(self, key):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            raise self.OrderNotFound(f'No order found for {key}')

class ListLike(ABC):
    @classmethod
    def __subclasshook__(cls, C):
        if issubclass(C, str):
            return False
        elif issubclass(C, Iterable):
            return True
        else:
            return NotImplemented
        
class AttrDict(dict):
    """ A dict subclass to read values as attributes. """
    def __getattr__(self, attr):
        try:
            return self[attr]
        except KeyError:
            raise AttributeError(attr)

    def __setattr__(self, attr, value):
        raise AttributeError(f'cannot set attribute for config')

    def __delattr__(self, attr):
        raise AttributeError(f'cannot delete attribute for config')
        
    def validate(self):
        pass
        
class DeprecationFilter:
    def __init__(self):
        self._records = set()
        
    def filter(self, record):
        import hashlib

        if 'Deprecation' not in record.getMessage():
            return True
        msg = record.getMessage()
        key = hashlib.md5(msg.encode()).hexdigest() #nosec
        
        if key not in self._records:
            self._records.add(key)
            return True
        
        return False
    
class OnetoOne(dict):
    '''
        A data structure to enable a one-to-one mapping. This
        stores two dict objects, so not mighty useful for large
        dicts. Use with cautions. There is a primary order defined
        which defines insert operations. The fetch operations are either
        primary - the get or the reverse, i.e. the `teg`.
    '''

    def __init__(self, *args, **kwargs):
        '''
            Store a reverse of the dict. If there are repeated
            keys when reverse, it will automatically be truncated.
            Be careful.
        '''
        super(OnetoOne, self).__init__(*args, **kwargs)
        self.__reversed_dict = dict((v,k) for k, v in \
                                   self.items())
    def __setitem__(self, key, value):
        super(OnetoOne, self).__setitem__(key, value)
        self.__reversed_dict.__setitem__(value, key)

    def get(self,key,default=None):
        return dict.get(self, key, default)

    def teg(self, key, default=None):
        return self.__reversed_dict.get(key, default)

    def flip(self):
        return self.__reversed_dict
    
class MaxSizedOrderedDict(OrderedDict):
    '''
        Extends OrderedDict to force a limit. Delete in FIFO when
        this limit exceeds. Delete items in chunks to avoid keep
        hitting the limits after a given number of insertions
    '''
    MAX_ENTRIES = 1000000
    CHUNK_SIZE = 1

    def __init__(self, *args, **kwargs):
        self._last_popped = None
        self.max_size = kwargs.pop("max_size",self.MAX_ENTRIES)
        self.chunk_size = kwargs.pop("chunk_size",self.CHUNK_SIZE)
        super(MaxSizedOrderedDict,self).__init__(*args, **kwargs)
        
    @property
    def last_popped(self):
        return self._last_popped

    def __setitem__(self, key, value):
        self._ensure_size()
        OrderedDict.__setitem__(self, key, value)

    def _ensure_size(self):
        self._last_popped = None
        if self.max_size is None:
            return
        if self.max_size > len(self):
            return

        for i in range(self.chunk_size):
            self._last_popped = self.popitem(last=False)
            
class MaxSizedList(list):
    '''
        Extends OrderedDict to force a limit. Delete in FIFO when
        this limit exceeds. Delete items in chunks to avoid keep
        hitting the limits after a given number of insertions
    '''
    MAX_ENTRIES = 1000000
    CHUNK_SIZE = 1

    def __init__(self, *args, **kwargs):
        self._max_size = kwargs.pop("max_size",self.MAX_ENTRIES)
        self._chunk_size = kwargs.pop("chunk_size",self.CHUNK_SIZE)
        super(MaxSizedList,self).__init__(*args, **kwargs)

    def append(self, obj):
        self._ensure_size()
        list.append(self, obj)

    def extend(self, iterable):
        self._ensure_size(len(iterable))
        list.extend(self, iterable)

    def _ensure_size(self, n=1):
        if self._max_size is None:
            return
        if n > self._max_size:
            raise ValueError("too large iterable.")
        if self._max_size > len(self)+(n-1):
            return

        n_to_pop = max(self._chunk_size, n)
        for i in range(n_to_pop):
            self.pop(0)

class DatasetOrderedDict(OrderedDict):
    '''
        Extends OrderedDict
        1. Handle max size
        2. Check expiration of key
    '''
    MAX_ENTRIES = 10
    CHUNK_SIZE = 1
    MAX_EXPIRY = 3 * 60 * 60 * 1000

    def __init__(self, *args, **kwargs):
        self.max_size = kwargs.pop("max_size",self.MAX_ENTRIES)
        self.chunk_size = kwargs.pop("chunk_size",self.CHUNK_SIZE)
        self.expiry_time = kwargs.pop("expiry_time", self.MAX_EXPIRY)
        super(DatasetOrderedDict, self).__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        self._ensure_size()
        now = time()
        expire = now + self.expiry_time
        OrderedDict.__setitem__(self, key, (expire, value))

    def _is_expired(self, key):
        now = time()
        try:
            expire, _value = OrderedDict.__getitem__(self, key)
            if expire and expire < now:
                return True, _value
            else:
                return False, _value
        except Exception:
            return True, ''

    def __getitem__(self, key):
        is_valid, value = self._is_expired(key)
        if not is_valid:
            return value

    def _ensure_size(self):
        if self.max_size is None:
            return
        if self.max_size > len(self):
            return

        for i in range(self.chunk_size):
            self.popitem(last=False)
        
class MsgQStringIO(StringIO):
    """
        Redirect IO to a pub-sub publisher. The publisher must support 
        the property `is_connected` and the method `send`.
    """

    _newline = b'\n'

    def __init__(self, *args, **kwargs):
        from blueshift.lib.exceptions import InitializationError

        if "publisher" not in kwargs or not kwargs['publisher']:
            raise InitializationError(msg="missing publisher handle")
        
        self._topic = kwargs.pop("topic", None)
        if not self._topic:
            raise InitializationError(msg="missing topic name")

        self._stream:TextIOBase|None = kwargs.pop("stream", None)
        self._publisher:StreamingPublisher = kwargs.pop("publisher")
        
        self._count = 0
        self._buffer_size = int(kwargs.pop("buffer_size",0))
        self._logger = kwargs.pop('logger', None)

        super(MsgQStringIO, self).__init__(*args, **kwargs)

    def _flush(self):
        self.truncate(0)
        self.seek(0)
        self._count = 0

    def _send_message(self):
        if not self._publisher.is_connected:
            if self._stream is None:
                return
            else:
                self._stream.write(self.getvalue())
                return

        lines = self.getvalue()
        packet = create_message_envelope(self._topic, AlgoMessageType.STDOUT, msg=lines)

        try:
            self._publisher.send_to_topic(self._topic, packet)
        except Exception as e:
            if self._logger:
                self._logger.error(f'MsgQIO Failed to send:{str(e)}')
            pass

    def _eol(self, msg):
        try:
            is_eol = bytes(msg, 'utf-8') == self._newline
        except Exception:
            return False
        else:
            return is_eol
    
    def isatty(self) -> bool:
        return False

    def write(self, msg, *args, **kwargs):
        ret = super(MsgQStringIO, self).write(msg)

        self._count = self._count + 1

        if self._eol(msg) or self._count > self._buffer_size:
            self._send_message()
            self._flush()

        return ret

class DailyLRUExpiry(ABC):
    def __init__(self, resets, tz):
        from datetime import time as datetime_time
        from functools import lru_cache

        self.tz = tz
        self.last_updated = None
        self.resets = datetime_time(*resets)
        self.nano_resets = (
                self.resets.hour*60 + self.resets.minute)*60*1000000000
        self._next_reset_dt()
        self._func = lru_cache()(self.func)
        
    def _next_reset_dt(self):
        
        next_dt = pd.Timestamp.now(tz=self.tz).normalize() + pd.Timedelta(days=1)
            
        next_resets = next_dt.value + self.nano_resets
        self.next_dt = pd.Timestamp(next_resets, tz=self.tz)
    
    @abstractmethod
    def func(self, *args, **kwargs):
        raise NotImplementedError(f'Implement this method.')
        
    def reset(self):
        self._func.cache_clear()
    
    def __call__(self, *args, **kwargs):
        forced = kwargs.pop('forced', False)
        if not self.last_updated:
            forced = True
            
        current_dt = pd.Timestamp.now(tz=self.tz)
        if not self.next_dt or self.next_dt < current_dt:
            self._next_reset_dt()
            forced = True
            
        if forced:
            self.reset()
            self.last_updated = current_dt
            
        return self._func(*args, **kwargs)

class ExpiryObject:
    __slots__ = ['value', 'validity']
    def __init__(self, value, validity):
        self.value = value
        self.validity = validity

class ExpiringCache:
    def __init__(self, method='ttl', expiry=None, max_size=None, 
                 **kwargs):
        self._expiry = None
        
        if max_size is None:
            self._cache = {}
        else:
            self._cache = MaxSizedOrderedDict(max_size=max_size, **kwargs)
        
        if method.lower() == 'timestamp':
            self._is_ttl = False
            if expiry is not None:
                self._expiry = self._to_utc(expiry)
        elif method.lower() == 'ttl':
            self._is_ttl = True
            if expiry is not None:
                self._expiry = int(expiry)
        else:
            raise ValueError(
                    f'method must be one of "ttl" or "timestamp".')
    
    def _to_utc(self, dt):
        dt = pd.Timestamp(dt)
        if dt.tz:
            dt = dt.tz_convert('Etc/UTC')
        else:
            dt = dt.tz_localize('Etc/UTC')
        return dt
        
    def set(self, key, value, expiry=None):
        if expiry is None and self._expiry is None:
            self._cache[key] = ExpiryObject(value, None)
            return
            
        if not self._is_ttl:
            if expiry:
                expiry = self._to_utc(expiry)
            else:
                expiry = self._expiry
        else:
            if expiry:
                expiry = int(expiry)
            else:
                expiry = self._expiry
            now = pd.Timestamp.now(tz='Etc/UTC')
            expiry = now + self.Timedelta(seconds=expiry) # type: ignore
            
        obj = ExpiryObject(value, expiry)
        self._cache[key] = obj
        
    def get(self, key, now=None):
        if key not in self._cache:
            raise KeyError(f'{key} not in cache.')
            
        obj = self._cache[key]
        if obj.validity is None:
            return obj.value
        
        if now:
            now = self._to_utc(now)
        else:
            now = pd.Timestamp.now(tz='Etc/UTC')
        if now > obj.validity:
            self._cache.pop(key)
            raise KeyError(f'entry for {key} expired.')
        return obj.value
    
# for order groups - e.g. bracket, cover, oco etc
OrderGroup = namedtuple('OrderGroup', ['active','inactive'])

# for cases when an order generates two or more orders (e.g. unwinds)
PartialOrder = namedtuple('PartialOrder',['oid','quantity'])

'''
    Data type for passing command on the command channel to the running
    algorithm.
'''
Command = namedtuple("Command",("cmd","args","kwargs"))

class BrokerConfig:
    """ Placeholder for storing broker specific config parameters. """
    pass

class BlueshiftMsgLogHandler(logging.Handler):
    """
        A log handler derived from zmq pub-handler, with socket
        emit overwritten by pubisher send emit.

        Args:
            ``publisher(object)``: A ZMQ publisher.

            ``formatter (logging.Formatter)``: A formatter object for logs.

            ``alert_rules (dict)``: A dict of alert rules settings.
    """

    def __init__(self, topic:str, publisher:StreamingPublisher, formatter, alert_rules):
        logging.Handler.__init__(self)

        self.publisher = publisher
        self.formatter = formatter
        self.topic = topic
        self.setFormatter(formatter)

        if "msg" in alert_rules['error']:
            self.setLevel(logging.ERROR)

        if "msg" in alert_rules['warning']:
            self.setLevel(logging.WARNING)

        if "msg" in alert_rules['log']:
            self.setLevel(logging.INFO)

    def format(self,record):
        """Format a record."""
        if self.formatter is None:
            self.formatter = logging.Formatter()
        return self.formatter.format(record)

    def emit(self, record):
        """Emit a log message on the publisher."""
        # quit if no socket
        if self.publisher.socket is None:
            return

        msg = self.format(record)
        packet = create_message_envelope(
            self.topic, AlgoMessageType.LOG, msg=msg, level=record.levelname) # type:ignore
        self.publisher.send_to_topic(self.topic, packet)

class PubStream(TextIOBase):
    """
        A bare-bone stream object for publising to 0MQ. This implements
        the `write`, `flush` buffer interface. This is **NOT** fork 
        safe. The stream object oursource all actual 0MQ writes to a 
        daemon thread.
    """
    def __init__(self, publisher:StreamingPublisher, topic:str, max_prints:int=0):
        from blueshift.lib.exceptions import InitializationError

        self._encoding = "utf-8"
        self._publisher = publisher
        self._topic = topic

        if not self._topic:
            raise InitializationError(msg="missing topic name")
        
        self._max_prints = 0
        if max_prints:
            self._max_prints = int(max_prints)
        self._count = 0

    def isatty(self) -> bool:
        return False

    @property
    def closed(self):
        return self._publisher is None

    def close(self):
        self._publisher = None
        
    def writable(self):
        return True
    
    def readable(self):
        return False

    def write(self, line):
        if self._max_prints and self._count > self._max_prints:
            return 0
        
        if isinstance(line, bytes):
            line = line.decode(self._encoding)
        else:
            line = str(line)
        if line == '\n' or line == '\r' or line == '\r\n':
            return 0

        if self._publisher is None:
            raise ValueError("This stream is closed")
        else:
            try:
                packet = create_message_envelope(self._topic, AlgoMessageType.STDOUT, msg=line)
                self._publisher.send_to_topic(self._topic, packet)
                self._count += 1
                
                if self._max_prints and self._count >= self._max_prints:
                    line = 'You have exceeded max print statements limit.'
                    packet = create_message_envelope(
                        self._topic, AlgoMessageType.PLATFORM, msg=line, level='error')
                    self._publisher.send_to_topic(self._topic, packet)
            except Exception:
                return 0
            
        return len(line)

    def writelines(self, lines):
        if self._max_prints and self._count > self._max_prints:
            return
        if self._publisher is None:
            return
        
        try:
            message = "\n".join(lines)
            packet = create_message_envelope(self._topic, AlgoMessageType.STDOUT, msg=message)
            self._publisher.send_to_topic(self._topic, packet)
            self._count += len(lines)
            
            if self._max_prints and self._count >= self._max_prints:
                line = 'You have exceeded max print statements limit.'
                packet = create_message_envelope(
                        self._topic, AlgoMessageType.PLATFORM, msg=line, level='error')
                self._publisher.send_to_topic(self._topic, packet)
        except Exception:
            pass

class Chunker(ABC):
    """ Base interface for data chunk key generation. """
    
    @property
    @abstractmethod
    def type(self) -> ChunkerType:
        raise NotImplementedError
    
    @abstractmethod
    def key_gen(self, *args, **kwargs):
        """ 
            Generate chunking keys based on inputs. Must be an iterator
            or return an iterable, that produces tuples. The first 
            element of the tuple must be the chunking key.
        """
        raise NotImplementedError
        
    def map_key(self, *args, **kwargs):
        """
            Map the chunking parameter to a particular chunking key. 
            The one-step process for `key_gen`. Must return a string,
            the chunking key. This method, iterated over the input 
            chunking parameter is equivalent to `key_gen` in the sense 
            that they should produce the same chunking keys.
        """
        raise NotImplementedError
        
    def get_epochs(self, *args, **kwargs):
        """
            Get the start and end date for a given chunking key. Must 
            return a tuple (start_date, end_date). If the particular 
            chunking method cannot determine start or end dates, must 
            return None for the particular case. Start and end dates 
            must be of time datetime.date.
        """
        raise NotImplementedError