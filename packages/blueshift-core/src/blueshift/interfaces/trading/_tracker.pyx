# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
from cpython cimport bool
from os import path as os_path
from os import listdir as os_listdir
import re
import bisect

import blueshift.lib.common.lazy_pandas as pd
from blueshift.lib.common.constants import CCY
from blueshift.lib.exceptions import ValidationError


cdef class Tracker:
    """
        Class to track and save historical record. This
        implements the tracking of orders and positions etc., reloading (
        rolling) of carried forward orders and positions from last
        sessions and methods to save these data. It also provides a
        method to generate or update positions from a list of orders,
        which can be checked against positions reported by the broker.

        Args:
            ``name (object)``: name of the run.

            ``mode (int)``: Run mode - BACKTEST(0) or LIVE(1).

            ``timestamp (str)``: Timestamp of creation.

            ``ccy (object)``: Currency in which to track.

    """

    def __init__(self, object name, object mode, object timestamp=None,
                 object ccy=CCY.LOCAL):
        self.FILE_FORMAT = "[0-9]{8}.json"
        self._last_reconciled = None
        self._last_saved = None
        self._needs_reconciliation = False
        self._last_reconciliation_status = False

        self._name = name
        self._mode = mode
        self._ccy = ccy
        self._timestamp = timestamp

        if self._ccy == CCY.LOCAL:
            self._local_ccy = True
        else:
            self._local_ccy = False
            
    @property
    def name(self):
        return self._name
    
    @property
    def mode(self):
        return self._mode

    cpdef set_timestamp(self, object timestamp):
        """
            A function to set the current timestamp.

            Args:
                ``timestapm (Timestamp)``: Timestamp to set to.

            Returns:
                None.
        """
        self._timestamp = timestamp

    def _prefix_fn(self, object param, bool fname_flag=True,
                   prefix="", suffix=".json", dt_format="%Y%m%d"):
        """
            a function to map a date to a file name (if flag is `True`) or
            reverse. if `fname_flag` is True, the `param` should be a
            Timestamp, and is converted to a string with YYYYMMDD.json
            format. Else, `param` should be a string of YYYYMMDD.json
            format, and the date part is reutrned.

            Args:
                ``param (Timestamp or str)``: timestamp or file name.

                ``fname_flag (bool)``: conversion method.

                ``prefix (str)``: Prefix before date identifier.

                ``suffix (str)``: Usually file extension.

                ``dt_format (str)``: Format string for date identifier.

            Returns:
                str: a formatted str (timestamp or json file name).
        """
        if fname_flag:
            try:
                return prefix + param.date().strftime(dt_format) + suffix
            except AttributeError:
                raise ValidationError(msg=f"{param} not a timestamp")
        else:
            # we assume a YYYYMMDD.json filename format
            if prefix == "" and suffix == "":
                return param
            elif prefix == "":
                return param.split(suffix)[0]
            else:
                splits = param.split(suffix)[0]
                splits = splits.split(prefix)
                result = splits[1] if len(splits) > 1 else splits[0]
                return result


    cdef _skip_ts(self, object ts, object timestamp=None):
        """
            A filter to skip entries if the date part in `ts` does not
            match timestap provided.

            Args:
                ``ts (Timestamp)``: current timestamp

                ``timestamp (Timestamp)``: timestamp to match date

            Returns:
                Bool. `False` if dates do not matches, else `False`.
        """
        if timestamp is None:
            return False

        try:
            if ts.date() == timestamp.date():
                return False
            else:
                return True
        except AttributeError:
            return False

    cdef _last_saved_date(self, object directory, object timestamp,
                          object pattern = None, object prefix="",
                          object suffix = ".json"):
        """
            Method to find the most recent date saved on a folder (we
            assume the file name format as YYYYMMDD.json).

            Args:
                ``directory (str)``: A directory to search.

                ``timestamp (Timestamp)``: Timestamp to filter on.

                ``pattern (str)``: Pattern to match, default "[0-9]{8}.json"

                ``prefix (str)``: Prefix before date identifier.

                ``suffix (str)``: Usually file extension.

            Returns:
                Timestamp. The last date in Timestamp format, or `None`.
        """
        cdef Py_ssize_t size

        if not os_path.exists(directory):
            return None

        if pattern is None:
            pattern = self.FILE_FORMAT
        p = re.compile(pattern)

        _files = os_listdir(directory)
        _files = [f for f in _files if re.match(p,f)]

        if _files:
            _dts = [self._prefix_fn(f,False,prefix=prefix) for \
                         f in _files]
            _dts = pd.to_datetime(_dts).sort_values()
            ts = timestamp.normalize().tz_localize(None)
            idx = bisect.bisect_left(_dts, ts)
            size = len(_dts)
            if idx >= size:
                return _dts[size-1]
            last_dt = _dts[idx]
            if last_dt > ts:
                return None
            return last_dt

        return None

    def roll(self, *args, **kwargs):
        """
            Method to re-load data from the last session (if any) and
            set up for the current session. This will typically be called
            at the beginning of the session (``before_trading_start``), or
            at the end of a session (``after_trading_hours``).
        """
        raise NotImplementedError


    def _save(self, *args, **kwargs):
        """
            Save the transaction based on defined storage target.
        """
        raise NotImplementedError

    def _read(self, *args, **kwargs):
        """
            Read the transaction based on defined storage target.
        """
        raise NotImplementedError

    cpdef if_update_required(self, object broker):
        """ flag if any update required for this tracker """
        raise NotImplementedError
