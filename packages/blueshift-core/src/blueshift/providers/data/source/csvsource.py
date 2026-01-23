from __future__ import annotations
from typing import TYPE_CHECKING, cast, Callable, Any
import sys
import os
import zipfile
import tarfile
import re
from functools import partial
import logging

from blueshift.lib.common.constants import Frequency, KB
from blueshift.lib.common.types import ListLike
from blueshift.lib.common.sentinels import NOTHING
from blueshift.lib.common.enums import ConnectionStatus
from blueshift.calendar import TradingCalendar, get_calendar
from blueshift.lib.exceptions import DataSourceException
from blueshift.lib.common.sentinels import MissingSymbol
from blueshift.interfaces.data.source import DataSource, DataSourceType, register_data_source
from blueshift.interfaces.logger import get_logger

if TYPE_CHECKING:
    import pandas as pd
    from pandas.tseries.frequencies import to_offset
else:
    import blueshift.lib.common.lazy_pandas as pd
    from blueshift.lib.common.lazy_pandas.tseries.frequencies import to_offset

_supported_compressions = ['gz', 'gzip', 'bz2', 'zip', 'xz']
_compression_type = {item:item for item in _supported_compressions}
_compression_type['gz'] = 'gzip'

_RAR_SUPPORT = True
try:
    import rarfile # type: ignore -> optional dependency
except ImportError:
    _RAR_SUPPORT = False
else:
    if sys.platform == 'win32':
        if os.path.exists(os.path.join(os.environ['ProgramFiles'],'WinRAR')):
            rarfile.UNRAR_TOOL = os.path.join(
                    os.environ['ProgramFiles'],'WinRAR','UnRAR.exe')
        elif os.path.exists(os.path.join(os.environ['ProgramFiles(x86)'],'WinRAR')):
            rarfile.UNRAR_TOOL = os.path.join(
                    os.environ['ProgramFiles(x86)'],'WinRAR','UnRAR.exe')
        else:
            raise FileNotFoundError('WinRAR not installed.')

class SourceWalker:
    """
        A class to traverse a file source (a file, or directory or a 
        zipped archive) and iterate over each of the file like object 
        contained therein. If the source is an archive and the param 
        `match_archived_names` is set to True, the internal full path 
        name is matched, instead of the archive file name.
        
        Example:
            Instaintiate an object of the class and iterate through 
            it to get all files, including files in compressed 
            archives (but excluding any such archive files themselves).
            
            To filter filenames, optionally specify the extension or a 
            filename pattern to match.
            
            To control the order of files and directory iteration, 
            optionally specify a key function (similar to key function
            in list `sort` method).
            
            >> walker = SourceWalker(src,'.csv')
            >> for size, name, fp in walker:
            ..     print(f"{name} ({size} KB)")
            
        Note:
            The file objects will automatically be closed once an 
            iteration completes and resource be released.
        
        Args:
            `root (str)`: A source location or file.
            
            `extension (str)`: A file extension to filter target files
            
            `pattern (str)`: A file name pattern to search (regex)
            
            `dir_sort (callable)`: A key func for sorting directories.
            
            `file_sort (callable)`: A key func for sorting files.
            
            `skip_hidden (bool)`: Skip hidden files/ folders.
            
            `match_all (bool)`: match all pattern (as opposed to any).
            
            `match_archived_names(bool)`: Match archived names.
            
            `name_func(callable)`: Apply transformation on file names.
            
        Returns:
            Tuple. Returns the file size, base file name (after removing
            extension, if supplied) and a file buffer for read, for each
            file in the `root`.
    """
    SUPPORTED_FORMATS = ['zip','tar', 'rar']
    
    def __init__(self, root:str, extension:str, patterns:list|str|None=None,
                 dir_sort:Callable|None=None, file_sort:Callable|None=None, skip_hidden:bool=True,
                 match_all:bool=True, match_archived_names:bool=False, name_func:Callable|None=None,
                 logger:logging.Logger|None=None):
        self._root = os.path.expanduser(root)
        self._extension = extension
        self._skip_hidden = skip_hidden
        self._match_all = match_all
        self._match_archived_names = match_archived_names
        self._name_func = self._wrap_key_func(name_func, 'fp')
        self._logger = logger if logger is not None else get_logger()
        
        self._patterns = []
        if patterns is not None:
            if not isinstance(patterns, ListLike):
                patterns = [patterns]
            patterns = cast(list, patterns)
            for pattern in patterns:
                self._patterns.append(r'.*'+pattern+r'.*')
        
        self._dir_key = self._wrap_key_func(dir_sort, 'root')
        self._file_key = self._wrap_key_func(file_sort, 'root')
        
        self._file_key.identity = False         # type: ignore
        if file_sort is None:
            self._file_key.identity=True        # type: ignore
        
        self._dir_key.identity = False          # type: ignore
        if dir_sort is None:
            self._dir_key.identity=True         # type: ignore
        
        self._open_mode = 'r'
        if self._extension.split('.')[-1] in _supported_compressions:
            self._open_mode = 'rb'
        
    def _wrap_key_func(self, f, name):
        import inspect
        keyword = False
        
        if callable(f):
            try:
                sig = inspect.signature(f)
                for param in sig.parameters.values():
                    if param.kind in (param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY) \
                        and param.name==name:
                        keyword = True
                        break
                    elif param.kind == param.VAR_KEYWORD:
                        keyword = True
                        break
            except Exception:
                pass
        
        def wrapped(x, **kwargs):
            if f is None:return x
            
            if keyword:
                return f(x, **kwargs)
            
            return f(x)
            
        return wrapped
    
    def _match_fname(self, fname, fp, patterns):
        if not patterns:
            return True
        
        if fp and self._match_archived_names:
            if not self._match_all:
                for pattern in patterns:
                    if re.search(pattern,fp.name) is not None:
                        return True
                return False
            else:
                for pattern in patterns:
                    if re.search(pattern,fp.name) is None:
                        return False
                return True
        
        if not self._match_all:
            for pattern in patterns:
                if re.search(pattern,fname) is not None:
                    return True
            return False
        else:
            for pattern in patterns:
                if re.search(pattern,fname) is None:
                    return False
            return True
    
    @staticmethod
    def _get_zipfiles(src, file_key):
        if file_key.identity:
            file_key = lambda x,root=None:x.filename
        
        with zipfile.ZipFile(src) as source:
            keyfunc = partial(file_key, root=source)
            zipped = sorted(source.infolist(), key=keyfunc)
            for zp in zipped:
                with source.open(zp.filename) as fp:
                    yield zp.file_size, zp.filename, fp
                    
    @staticmethod
    def _get_tarfiles(src, file_key):
        if file_key.identity:
            file_key = lambda x,root=None:x.filename
            
        with tarfile.TarFile(src) as source:
            keyfunc = partial(file_key, root=source)
            zipped = sorted(source.infolist(), key=keyfunc) # type: ignore
            for zp in zipped:
                with source.open(zp.filename) as fp:
                    yield zp.file_size, zp.filename, fp
                    
    @staticmethod
    def _get_rarfiles(src, file_key):
        if _RAR_SUPPORT:
            if file_key.identity:
                file_key = lambda x,root=None:x.filename
                
            with rarfile.RarFile(src) as source: # type: ignore -> optional dependency
                keyfunc = partial(file_key, root=source)
                zipped = sorted(source.infolist(), key=keyfunc)
                for zp in zipped:
                    with source.open(zp.filename) as fp:
                        yield zp.file_size, zp.filename, fp
    
    def _handle_files(self, file, patterns, file_key, skip_hidden, open_mode):
        self._logger.debug(f'handling file {file}')
        if skip_hidden:
            base_name = os.path.basename(file)
            if base_name.startswith('.'):
                return
            
        if tarfile.is_tarfile(file):
            for fsize, fname, fp in self._get_tarfiles(file, file_key):
                if not self._match_fname(file, fp, patterns):
                    continue
                yield int(fsize/KB), os.path.basename(fname), fp
        elif zipfile.is_zipfile(file):
            for fsize, fname, fp in self._get_zipfiles(file, file_key):
                if not self._match_fname(file,fp, patterns):
                    continue
                yield int(fsize/KB), os.path.basename(fname), fp
        elif _RAR_SUPPORT and rarfile.is_rarfile(file): # type: ignore -> optional dependency
            for fsize, fname, fp in self._get_rarfiles(file, file_key):
                if not self._match_fname(file,fp,patterns):
                    continue
                yield int(fsize/KB), os.path.basename(fname), fp
        elif self._match_fname(file, None, patterns):
            with open(file, open_mode) as fp:
                yield int(os.stat(file).st_size/KB),os.path.basename(file), fp
                
    def _walk_files(self, src, patterns, dir_key, file_key, skip_hidden, open_mode):
        if skip_hidden:
            base_name = os.path.basename(src)
            if base_name.startswith('.'):
                return
            
        if os.path.isfile(src):
            for size, name, fp in self._handle_files(
                    src, patterns, file_key, skip_hidden, open_mode):
                yield size, name, fp
        else:
            for root, dirs, files in os.walk(src, topdown=True):
                if skip_hidden:
                    dirs[:] = [d for d in dirs if not d.startswith('.')]
                
                dirs.sort(key=dir_key)
                if files:
                    keyfunc = partial(file_key, root=root)
                    files.sort(key=keyfunc)
                    files = [os.path.join(root, f) for f in files]
                    for file in files:
                        for size, name, fp in self._handle_files(
                                file, patterns, file_key, skip_hidden,
                                open_mode):
                            yield size, name, fp
    
    def __iter__(self):
        for size, name, fp in self._walk_files(
                self._root, self._patterns, self._dir_key,
                self._file_key, self._skip_hidden, self._open_mode):
            if self._skip_hidden and name.startswith('.'):
                continue
            if self._extension:
                if name.lower().endswith(self._extension):
                    yield size, self._name_func(name.split(self._extension)[0], fp=fp), fp
            else:
                yield size, self._name_func(name,fp=fp), fp

class CSVReader:
    """
        Class to process input csv file and produce time-series ready
        chunks for ingestion.
        
        Note:
            Data extracted can be modified by a varities of methods. The 
            easiest way is to specify the `transform_fn` argument, which 
            takes in raw data read from the file and returns whatever the 
            function specifies. Simpler transformations can be directly 
            specified. To drop columns (and rename), specify the `field` 
            argument. Specify the `date_col` (and optionally `time_col` 
            if date and time is split) for extracting timestamp. Specify 
            `frequency` (and `agg_funcs` for aggregation functions ) for 
            resampling data before writing. Specify `symbol_col` for the 
            column with the symbol name.
        
        Example:
            Instantiate an object of the class (fields to extract from 
            source files and the time frequency of the input data must 
            be specified). This will return a callable that can be 
            passed a file object and other parameters to iterate through
            source and generate a series of tuples in the form of 
            (sym, data), where `sym` is a string symbol of the underlying 
            asset and `data` is a dataframe.
            
            >> reader = CSVReader(fp, fields=['open','close'], frequency='T')
            >> for sym, chunk in reader(fp, name):
            ..     do_something(sym, chunk)
            

    """
    _chunksize = 10000
    _default_tz = 'Etc/UTC'
    _default_market_open = (9,30,0)
    _default_market_close = (16,0,0)
    _index_name = 'datetime'
    _default_agg_fn = 'mean'
    _agg_funcs = {'open':'first',
                  'high':'max',
                  'low':'min',
                  'close':'last',
                  'volume':'sum',
                  'open_interest':'sum'
                  }
    
    _fill_value = {'volume':0}
    _fill_method = 'ffill'
    _frequencies = {'m':'T',
                    'min':'T','mins':'T',
                    'minutes':'T',
                    'T':'T',
                    'day':'D',
                    'days':'D',
                    'D':'D'}
    
    def __init__(self, fields, frequency=None, calendar=None, symbol_col=None, 
                 date_col=None,  time_col=None, datetime_format=None, 
                 transform_fn=None, agg_funcs={}, no_fill=False, 
                 csv_kwargs={}, input_tz=None, 
                 drop_duplicate_timestamps=True, filter_fn=None):
        """
            A class to read, normalize and dispatch csv files in a 
            chunked manner.
            
            Args:
                `fields`: specify the columns to extract, can be either 
                a list of column names or a list of column indices or a 
                dict. If list, represents the column or indices to 
                extract. If a dict, the columns are extracted based on
                the values and renamed based on the keys.
                
                `frequency`: is the expected data frequency of the input.
                Data will be resampled at this frequency and aggregated
                and missing valued will be filled (`ffill`)
                
                `calendar`: A trading calendar object - this supplies info
                like the timezone, opening time and closing time of day
                to interpret the timestamps properly.
                
                `symbol_col`: is the column name (or index) for the 
                ticker). If unspecified, the file name is assumed to be 
                a symbol.
                
                `date_col`: if unspecified is assumed to be the first 
                column (index 0), else it can be a string (to match 
                the column name) or a number (column index). 
                
                `time_col`: Similar as `date_col`. If unspecified, no 
                separate time column is assumed.
                
                `datetime_format`: if unspecified, is inferred, else is 
                passed on to pandas `to_datetime` function. If it is a 
                dict, it is passed on as keyword arguments, as as the 
                `format` value.
                
                `transform_fn`: A callable to transform input data. Must
                accept a time-indexed dataframe and return a time-indexed
                dataframe. This will be applied on raw input data before
                any other transformations.
                
                `agg_funcs`: Dict of functions for dataframe aggregation 
                after resampling.
                
                `no_fill (bool)`: If true, read timestamps as is, without
                filling in for missing data.
                
                `csv_kwargs`: Keyword arguments for csv reader.
                
                `input_tz (str)`: The source input timezone, defaults None.
                
                `drop_duplicate_timestamps (Bool)`: Drop duplicate timestamps.
                
                `filter_fn`: A callable to filter input data based on both 
                the data as well as the symbol being processed at the 
                moment.
        """
        
        if isinstance(fields, dict):
            self._fields = list(fields.values())
            self._colnames = list(fields.keys())
        else:
            self._fields = fields
            self._colnames = None
        
        self._sym_col = symbol_col
        self._date_col = date_col
        self._time_col = time_col
        
        if not isinstance(datetime_format, dict):
            datetime_format = {'format':datetime_format}
        self._datetime_format:dict = datetime_format
        
        self._freq = Frequency(frequency)
        
        if calendar is None:
            self._cal = TradingCalendar(
                    "default_calendar",tz=self._default_tz,
                    opens=self._default_market_open,
                    closes=self._default_market_close)
        elif isinstance(calendar,TradingCalendar):
            self._cal = calendar
        else:
            try:
                self._cal = get_calendar(calendar)
            except Exception:
                raise DataSourceException(
                        'calendar not found.')
        
        if self._freq.period == 'D':
            self._label = self._closed = "left"
        else:
            self._label = self._closed = "right"
        
        if transform_fn:
            self._transform_func = transform_fn
        else:
            self._transform_func = lambda x:x
        
        self._nofill = no_fill
        self._csv_kwargs = csv_kwargs
        self._user_agg_funcs = agg_funcs
        
        self._dummy_df = pd.DataFrame(columns=self._fields)
        self._input_tz = input_tz
        self._drop_duplicate_timestamps = drop_duplicate_timestamps
        
        self._filter_fn = filter_fn
        
    def _extract_cols(self, chunk):
        chunk = chunk[self._fields]
        if self._colnames:
            chunk.columns = self._colnames
        
        return chunk
        
    def _align_data(self, chunk, funcs={}, fills={}):
        cols = list(chunk.columns)
        if self._freq.aligned and \
            to_offset(self._freq.period) < to_offset('D'):
            sampler = chunk.resample(
                    self._freq.period, closed=self._closed, label=self._label)
            
            funcs = {**funcs, **self._agg_funcs, **self._user_agg_funcs}
            agg_func = {**{c:self._default_agg_fn for c in cols}, 
                        **{key:funcs[key] for key in funcs if key in cols}}
            chunk = sampler.agg(agg_func)
            
            chunk = chunk.between_time(
                    start_time=self._cal.open_time, 
                    end_time=self._cal.close_time,
                    inclusive='right')
            
        if self._nofill:
            return chunk.dropna()
        
        fill_values = {
                key:self._fill_value[key] for key in self._fill_value \
                   if key in cols}
        fill_values = {**fill_values,
                       **{key:fills[key] for key in fills if key in cols}}
        
        if fill_values:
            chunk = chunk.fillna(value=fill_values)
            
        if self._freq.aligned and self._freq >= Frequency('D'):
            chunk.index = chunk.index.normalize()
        
        if self._fill_method=='ffill':
            return chunk.ffill()
        elif self._fill_method=='bfill':
            return chunk.bfill()
        else:
            # TODO: what else?
            return chunk.fillna(method=self._fill_method)
    
    def _set_index(self, chunk:pd.DataFrame):
        if self._date_col is None:
            self._date_col = chunk.columns[0]
            
        if self._time_col:
            idx = pd.to_datetime(
                    chunk[self._date_col].astype(str) + ' ' + chunk[self._time_col],
                    **self._datetime_format)
        else:
            idx = pd.to_datetime(
                    chunk[self._date_col], **self._datetime_format)
        
        try:
            idx = pd.DatetimeIndex(idx)
        except Exception:
            idx = pd.DatetimeIndex(pd.to_datetime(idx, utc=True))
            
        idx.name = self._index_name
        
        if idx.tz is None:
            if self._input_tz:
                idx = idx.tz_localize(
                        self._input_tz).tz_convert(self._cal.tz)
            else:
                idx = idx.tz_localize(self._cal.tz)
        else:
            idx = idx.tz_convert(self._cal.tz)
            
        chunk.index = idx
        if self._drop_duplicate_timestamps:
            chunk = chunk[~chunk.index.duplicated(keep='last')]
        chunk = chunk.sort_index()
        
        return chunk
    
    def _ensure_sorted(self, chunk):
        if self._sym_col is None:
            return chunk
        
        return chunk.sort_values(by=self._sym_col)
    
    def _filter_for_sym(self, chunk, sym):
        if self._sym_col is None:
            return chunk
        
        return chunk[chunk[self._sym_col]==sym]
    
    def _get_sym_list(self, chunk, fname):
        if self._sym_col is None:
            return [fname]
        return list(chunk[self._sym_col].unique())
    
    def __call__(self, fp, fname, fsize=None, skip_symbol=None, 
                 **kwargs):
        """
            The main callable of the class to read and dispatch 
            chunks of time-indexed dataframes as read from a csv 
            source.
            
            Args:
                `fp`: A file object buffer to read from. Must be a 
                csv file with time series data.
                
                `fname`: A string with the file name.
                
                `skip_symbol`: A list or a callable to specify if a
                symbol should be skipped while reading. If a list, will 
                be skipped if a match found. If a callable, must accept 
                a string (the ticker) and return True (to skip) or 
                False (to keep).
                
                `kwargs`: Dict with extra options to be passed on to 
                the pandas `read_csv` function.
                
            Returns:
                Tuple - (`symbol`, `data`), where `symbol` is a string 
                (the ticker name) and `data` is a dataframe.
        """
        csv_kwargs = {**{'chunksize':self._chunksize}, 
                      **self._csv_kwargs, **kwargs}
        
        if skip_symbol is None:
            skip_symbol = lambda sym: False
            
        if 'usecols' in csv_kwargs:
            self._fields = csv_kwargs.pop('usecols')
        if 'index_col' in csv_kwargs:
            csv_kwargs.pop('index_col')
        
        if hasattr(fp,'encoding') and 'encoding' in csv_kwargs:
            if csv_kwargs['encoding'] != fp.encoding:
                # wrong encoding, open with filename
                fp = fp.name
        
        #cache
        dataframes = {}
        for chunk in pd.read_csv(fp, **csv_kwargs):
            chunk.columns = [c.strip() if isinstance(c, str) else c \
                             for c in chunk.columns]
            
            chunk = self._transform_func(chunk)
            
            if len(chunk) < 1:
                continue
            
            # ensure current chunk is sorted by symbol, if applicable.
            chunk = self._ensure_sorted(chunk)
            # get a list of symbols in the current chunk
            syms = self._get_sym_list(chunk, fname)
            for sym in syms:
                if skip_symbol(sym):
                    continue
                # read fresh sym data
                new = self._filter_for_sym(chunk, sym)
                if len(new) == 0:
                    continue
                # set index extracting date/ time columns if required
                new = self._set_index(new)
                # extract and rename columns
                new = self._extract_cols(new)
                # align data as per specification
                new = self._align_data(new)
                # apply filtering if any
                if self._filter_fn:
                    new = self._filter_fn(sym, new)
                    
                if len(new) == 0:
                    continue
                
                # check if we have any old sym in cache and emit
                for key in list(dataframes.keys()):
                    if key != sym:
                        yield key, dataframes.pop(key)
                
                # check if we have anything in cache
                self._dummy_df.columns = new.columns
                existing = dataframes.pop(sym, self._dummy_df)
                # if not large enough, put back in cache
                if len(new) + len(existing) < csv_kwargs['chunksize']:
                    if existing.empty:
                        dataframes[sym] = new
                    else:
                        dataframes[sym] = pd.concat(
                                [existing, new], sort=False)
                    continue
                
                # else emit and pop from cache
                if not existing.empty:
                    data = pd.concat([existing, new], sort=False)
                else:
                    data = new
                
                if sym in dataframes:
                    dataframes.pop(sym)
                yield sym, data
                
        for sym in dataframes:
            yield sym, dataframes[sym]

class CSVSource(DataSource):
    """
        Implementation of data source interface for csv files, including 
        in known zipped formats.
        
        Example:
            Instantiate an object of this class with required params. 
            This will produce an iterator that emits a tuple of 
            (metadata, data) that can be consumed in a loop. `metadata` 
            is an arbitrary dictionary with key and value, but must 
            contain keys `symbol` and `size` for the data chunk. If the 
            `size` is greater than zero, must also contain `start` and 
            `end` for the starting and ending year (as integer) for the 
            data chunk. `data` is a dataframe chunk with date-time index.
            
            >> source = CSVSource(root, fields, ..)
            >> for mdata, data in source:
            ..     do_something(mdata, data)
            
        
    """
    def __init__(self, root:str, fields:list|dict, frequency:str|Frequency, 
                 calendar:TradingCalendar|None=None, 
                 symbol_col:str|int|None=None, symbol_format:str|Callable[[Any,Any],str]|None=None, 
                 date_col:str|int|None=None,  time_col:str|int|None=None, 
                 datetime_format:str|dict[str,str]|None=None, extension:str='.csv', 
                 patterns:str|list[str]|None=None, dir_sort:Callable|None=None, file_sort:Callable|None=None, 
                 skip_symbol:list|Callable|None=None, 
                 transform_fn:Callable[[pd.DataFrame],pd.DataFrame]|None = None, agg_funcs={}, 
                 no_fill:bool=False, csv_kwargs:dict={}, master_data:pd.DataFrame=pd.DataFrame(),
                 input_tz=None, logger:logging.Logger|None=None, skip_hidden:bool=True,
                 match_all:bool=True, match_archived_names:bool=False, name_func:Callable[[str],str]|None=None,
                 drop_duplicate_timestamps:bool=True, filter_fn=None):
        """
            Specify a source for data to read from.
            
            Args:
                `root (str)`: A source location or file.
            
                `extension (str)`: A file extension to filter target files
                
                `patterns (list)`: A lsit of file name pattern to search (regex)
                
                `dir_sort (callable)`: A key func for sorting directories.
                
                `file_sort (callable)`: A key func for sorting files.
                
                `fields`: specify the columns to extract, can be either 
                a list of column names or a list of column indices or a 
                dict. If list, represents the column or indices to 
                extract. If a dict, the columns are extracted based on
                the values and renamed based on the keys.
                
                `frequency`: is the expected data frequency of the input.
                Data will be resampled at this frequency and aggregated
                and missing valued will be filled (`ffill`)
                
                `calendar`: A trading calendar object - this supplies info
                like the timezone, opening time and closing time of day
                to interpret the timestamps properly.
                
                `symbol_col`: is the column name (or index) for the 
                ticker). If unspecified, the file name is assumed to be 
                a symbol.
                
                `symbol_format`: A string or a callable to extract ticker
                names from symbol column (or file name) input. If a string
                a regex is carried out to extract substring. Else should 
                be a callable taking a single srting and a date as arguments 
                and returning another string (aaplicable on that date).
                
                `date_col`: if unspecified is assumed to be the first 
                column (index 0), else it can be a string (to match 
                the column name) or a number (column index). 
                
                `time_col`: Similar as `date_col`. If unspecified, no 
                separate time column is assumed.
                
                `datetime_format`: if unspecified, is inferred, else is 
                passed on to pandas `to_datetime` function. If it is a 
                dict, it is passed on as keyword arguments, as as the 
                `format` value.
                
                `skip_symbol`: A list or a callable to specify if a
                symbol should be skipped while reading. If a list, will 
                be skipped if a match found. If a callable, must accept 
                a string (the ticker) and return True (to skip) or 
                False (to keep).
                
                `transform_fn`: A callable to transform input data. Must
                accept a time-indexed dataframe and return a time-indexed
                dataframe.
                
                `no_fill (bool)`: If true, read timestamps as is, without
                filling in for missing data.
                
                `master_data (Dataframe)`: A dataframe with master 
                data (metadata) information about each symbol. If 
                found, the matching row will be passed on as part of 
                metadata for the symbol during iteration. It must have a 
                column named `symbol` to match the row.
                
                `input_tz (str)`: The source input timezone, defaults None.
                
                `skip_hidden (bool)`: Skip hidden files/ folders.
                
                `match_all (bool)`: match all pattern (as opposed to any).
            
                `match_archived_names(bool)`: Match archived names.
                
                `name_func(callable)`: Apply transformation on file names.
                
                `drop_duplicate_timestamps (Bool)`: Drop duplicate timestamps.
                
                `filter_fn`: A callable to filter input data based on both 
                the data as well as the symbol being processed at the 
                moment. Filtering is applied after the data has been fully
                processed (e.g. datetime index is created and target 
                columns are extracted).
                
            Returns:
                Tuple - (`metadata`, `data`), where `metada` is a dict 
                and `data` is a dataframe. `metadata` can contain any 
                arbitrary user-defined key-value pair, but guaranteed 
                to have at least the `symbol` name and `size` for the 
                `data`. If the `size` is greater than zero, will also 
                contain `start` and `end` for the starting and ending 
                year (as integer) for the data chunk.
                
                
        """
        super(CSVSource, self).__init__()
        self._type = DataSourceType.FILE
        
        if len(master_data) >0 and 'symbol' not in master_data.columns:
            raise DataSourceException('master data missing symbol column.')
        self._master_data = master_data
        
        self._walker = SourceWalker(
                root=root, extension=extension, patterns=patterns,
                dir_sort=dir_sort, file_sort=file_sort, 
                skip_hidden=skip_hidden, match_all=match_all,
                match_archived_names=match_archived_names, name_func=name_func)
        
        csv_kwargs_ = csv_kwargs.copy()
        compression = extension.split('.')[-1]
        if compression in _supported_compressions:
            if 'compression' not in csv_kwargs_:
                csv_kwargs_['compression']=_compression_type[compression]
        
        self._reader = CSVReader(
                fields, frequency, calendar=calendar, 
                symbol_col=symbol_col, date_col=date_col, time_col=time_col, 
                datetime_format=datetime_format, transform_fn=transform_fn, 
                agg_funcs=agg_funcs, no_fill=no_fill, csv_kwargs=csv_kwargs_,
                input_tz=input_tz, 
                drop_duplicate_timestamps=drop_duplicate_timestamps,
                filter_fn=filter_fn)
        
        self._skip_symbol = skip_symbol
        self._sym_func = self._get_sym_func(symbol_format)
        self._metadata = {'frequency':self.frequency}
        
        if logger:
            self._logger = logger
        else:
            self._logger = get_logger()

    @property
    def timeout(self) -> int:
        return 0
    
    @property
    def logger(self):
        return self._logger
    
    @property
    def frequency(self):
        return self._reader._freq.period
        
    @property
    def root(self):
        return self._walker._root
        
    @property
    def connected(self):
        """ we can iterated anytime, so always connected. """
        self.connect()
        return True
    
    @property
    def connection_status(self):
        return ConnectionStatus.ESTABLISHED
    
    @property
    def metadata(self):
        return self._metadata
    
    def connect(self, *args, **kwargs):
        if not os.path.exists(self._walker._root):
            raise DataSourceException("input source root missing.")
        
    def close(self):
        pass
    
    
    def add_masterdata(self, master_data):
        """
            Add a master data dataframe, with a `symbol` column that 
            provides additional fields for enriching metadata emitted 
            for a given symbol.
        """
        if len(master_data) >0 and 'symbol' not in master_data.columns:
            raise DataSourceException('master data missing symbol column.')
        self._master_data = master_data
    
    def add_metadata(self, **kwargs):
        """ Add extra metadata to be applied for **all** symbols. """
        self._metadata = {**self._metadata, **kwargs}
        
        for key in list(self._metadata.keys()):
            if self._metadata[key] == NOTHING:
                self._metadata.pop(key)
                
    def _get_sym_func(self, ticker_format):
        if callable(ticker_format):
            return ticker_format
        
        if isinstance(ticker_format, str):
            def f(s, dt):
                try:
                    return re.search(ticker_format,s).group(1) # type: ignore
                except AttributeError:
                    return MissingSymbol
            return f
        
        return lambda s,dt:s
    
    def _rename_tickers(self, syms, dt):
        syms = cast(list, syms)
        if isinstance(syms, ListLike):
            return [self._sym_func(sym, dt) for sym in syms]
        else:
            return self._sym_func(syms, dt)
    
    def _apply_metadata(self, sym, fsize, fname, data):
        if len(data) > 0:
            dt = data.index[-1]
            if dt.tz:
                dt = dt.tz_convert('Etc/UTC').tz_localize(None).normalize()
            else:
                dt = dt.normalize()
        else:
            dt = pd.Timestamp.now().normalize()
        sym = self._rename_tickers(sym, dt)
            
        metadata = {}
        metadata['symbol'] = sym
        metadata['filesize'] = fsize
        metadata['filename'] = fname
        metadata['size'] = len(data)
        
        if len(data) > 0:
            metadata['start'] = data.index[0].year
            metadata['end'] = data.index[-1].year
            metadata['start_date'] = data.index[0].normalize()
            metadata['end_date'] = data.index[-1].normalize()
        
        mdata = {}
        if len(self._master_data)>0 and sym in self._master_data.symbol.values:
            mdata = self._master_data[\
                    self._master_data.symbol==sym].iloc[0].to_dict()
            mdata.pop('symbol', None)
        
        return {**metadata, **mdata}
    
    def __iter__(self):
        self.connect()
        self._file_processed = 0
        self._last_fname = self._current_fname = None
        
        for size, name, fp in self._walker:
            for sym, chunk in self._reader(fp, name, size, 
                                           self._skip_symbol):
                metadata = self._apply_metadata(sym, size, name, chunk)
                
                if self._last_fname != name:
                    self._last_fname = name
                    metadata['update_progress'] = True
                    self._file_processed = self._file_processed + 1
                    metadata['file_processed'] = self._file_processed
                else:
                    metadata['update_progress'] = False
                yield {**self.metadata, **metadata}, chunk
                
    def __len__(self):
        """ number of input files to process. """
        return len([name for size, name, fp in self._walker])
    
    def update(self, root, *args, **kwargs):
        extension=kwargs.get('extension', self._walker._extension)
        patterns=kwargs.get('patterns', self._walker._patterns)
        skip_symbol=kwargs.get('skip_symbol', self._skip_symbol)
        skip_hidden=kwargs.get('skip_hidden', self._walker._skip_hidden)
        match_all=kwargs.get('match_all', self._walker._match_all)
        match_archived_names=kwargs.get('match_archived_names', self._walker._match_archived_names)
        name_func=kwargs.get('name_func', self._walker._name_func)
        
        dir_key = self._walker._dir_key
        file_key = self._walker._file_key
        
        self._walker = SourceWalker(
                root=root, extension=extension, patterns=patterns,
                skip_hidden=skip_hidden, match_all=match_all,
                match_archived_names=match_archived_names, name_func=name_func)
        self._walker._dir_key = dir_key
        self._walker._file_key = file_key
        self._skip_symbol = skip_symbol

register_data_source('csv', CSVSource)