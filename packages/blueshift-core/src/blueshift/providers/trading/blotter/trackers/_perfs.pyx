# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
cimport numpy as np

import numpy as np
from os import path as os_path
import pandas as pd
import json
from json import JSONDecodeError
from numpy cimport float64_t, ndarray


from blueshift.lib.trades._accounts cimport BlotterAccount
from ._tracker cimport Tracker

from blueshift.config import blueshift_saved_performance_path
from blueshift.lib.exceptions import (
        DataReadException, DataWriteException, InitializationError,
        TerminationError, BlueshiftException, ValidationError, ExceptionHandling)
from blueshift.lib.common.enums import AlgoMode as MODE
from blueshift.lib.common.sentinels import nan_op

from ..analytics.stats import compute_eod_point_stats_report
from ..analytics.pertrade import compute_eod_per_trade_report, compute_eod_txn_report
                         

cdef int _PRECISION = 2
cdef int _P_PRECISION = _PRECISION+2

ROLLING_STATS = False

def import_pyfolio():
    global ROLLING_STATS
    
    try:
        import pyfolio.timeseries as pft
        ROLLING_STATS = True
        return pft
    except ImportError:
        class PFT():
            pass
        pft = PFT()
        pft.rolling_regression = nan_op
        pft.rolling_volatility = nan_op
        pft.rolling_sharpe = nan_op
        ROLLING_STATS = False
        return pft

AccountReportCols = list(["net", "cash", "holdings", "cost_basis",
                          "starting_cash", "margin", "mtm", "realized",
                          "pnls", "liquid_value", "commissions",
                          "charges", "day_pnl", "funding"])

PositionReportCols = list(["gross_leverage", "net_leverage", "gross_exposure",
                           "net_exposure", "long_exposure", "short_exposure"])

PositionCountReportCols = list(["longs_count","shorts_count"])

PerformanceCols = list(["algo_returns", "algo_cum_returns",
                           "algo_volatility", "drawdown"])

RiskReportCols = list(["benchmark_rets", "benchmark_cum_rets",
                       "benchmark_volatility", "alpha", "beta", "sharpe",
                       "sortino", "omega", "stability"])

cdef class PerformanceTracker(Tracker):
    """ Implementation of performance tracker using file system.
        This handles performance and risks metrics calculation, saving
        the current values as a dict and historical values as numpy
        arrays which are converted to DataFrame on demand.

        Note:
            If mode is LIVE, the data are saved daily, else once at the
            end of the backtest. For backtest runs, roll method only
            increments the position pointer.

        Args:
            ``name (str)``: Name of the current run.

            ``mode (int)``: Run MODE of the current run.

            ``timestamp (Timestamp)``: Current timestamp.

            ``max_lookback (int)``: Maximum lookback for rolling measures.

            ``chunk_size (int)``: Chunk size of array allocation.

        Returns:
            None.
        """
    def __init__(self, object name, object mode, object timestamp=None,
                 object tz='Et/UTC', np.uint64_t max_lookback=3650, 
                 np.uint64_t chunk_size=4095, output_dir=None):
        super(PerformanceTracker, self).__init__(name,mode,timestamp)

        self.CHUNK_SIZE = chunk_size
        self._max_lookback = max_lookback
        self.tz = tz

        self.PERF_REPORTS_FILE = "performance_report.csv"

        self.acct_cols = len(AccountReportCols)
        self.pos_cols = len(PositionReportCols)
        self.pos_count_cols = len(PositionCountReportCols)
        self.perf_cols = len(PerformanceCols)
        # TODO: to implement later
        #self.risk_cols = len(RiskReportCols)

        self._perf_dir = output_dir
        self.reset()
        
        self._benchmark_rets = None
        self._benchmark = None

    def reset(self, account=None, last_saved=None,
              last_reconciled=None):
        self._acct_report =  np.zeros((self.CHUNK_SIZE, self.acct_cols),
                                      dtype=np.float64)
        self._pos_report =  np.zeros((self.CHUNK_SIZE, self.pos_cols),
                                     dtype=np.float64)
        self._pos_count_report =  np.zeros((self.CHUNK_SIZE, self.pos_count_cols),
                                           dtype=np.uint64)
        self._perf_report =  np.zeros((self.CHUNK_SIZE, self.perf_cols),
                                      dtype=np.float64)
        self._array_idx = np.zeros((self.CHUNK_SIZE), dtype=np.int64)

        self.current_performance = {}

        if account:
            if isinstance(account, dict):
                self.current_performance = account
            else:
                self._set_current_performance(account)

        self._pos_idx = 0

        self._last_reconciled = last_reconciled    # this is actually last updated.
        self._last_saved = last_saved

    def roll(self, object timestamp):
        """
            We load the last saved account view and reset the tracked
            quantities.
        """
        # save current data.
        if self._mode in (MODE.LIVE, MODE.PAPER):
            self._save(timestamp)
            # carry forward the status flags.
            last_account = self.current_performance
            last_saved = self._last_saved
            last_reconciled = self._last_reconciled

            self.reset(last_account, last_saved, last_reconciled)

            # reload last saved.
            self._read(timestamp)

        # advance index position to the next day.
        self._pos_idx = self._pos_idx + 1

    def to_dataframe(self):
        nrows = self._pos_idx + 1

        try:
            if self._array_idx[self._pos_idx] == 0:
                """ we just did a roll and yet to write stuff """
                nrows = max(0,nrows - 1)
        except IndexError:
            """ Same case, we just did a roll and yet to write stuff """
            nrows = max(0,nrows - 1)

        if nrows == 0:
            return pd.DataFrame()

        idx = pd.to_datetime(self._array_idx[:nrows])
        acct = pd.DataFrame(self._acct_report[:nrows],
                            columns=AccountReportCols, index=idx)
        pos = pd.DataFrame(self._pos_report[:nrows],
                           columns=PositionReportCols, index=idx)
        pos_count = pd.DataFrame(self._pos_count_report[:nrows],
                                 columns=PositionCountReportCols, index=idx)
        perfs = pd.DataFrame(self._perf_report[:nrows],
                             columns=PerformanceCols, index=idx)
        df = pd.concat([acct, pos, pos_count, perfs], axis=1)
        df.index = df.index.tz_localize('Etc/UTC').tz_convert(self.tz)
        df.index = df.index.normalize()
        df.index.name = 'date'

        return df

    def to_dataframe_performance(self):
        nrows = self._pos_idx + 1
        try:
            if self._array_idx[self._pos_idx] == 0:
                """ we just did a roll and yet to write stuff """
                nrows = max(0,nrows - 1)
        except IndexError:
            """ Same case, we just did a roll and yet to write stuff """
            nrows = max(0,nrows - 1)

        if nrows == 0:
            return pd.DataFrame()
        
        idx = pd.to_datetime(self._array_idx[:nrows])
        perfs = pd.DataFrame(self._perf_report[:nrows],
                             columns=PerformanceCols, index=idx)
        perfs.index = perfs.index.tz_localize('Etc/UTC').tz_convert(self.tz)
        perfs.index = perfs.index.normalize()
        perfs.index.name = 'date'
        
        return perfs

    def _save(self, timestamp):
        ROLLING_PERIOD = 60
        if self._array_idx[0] == 0:
            """ we have not yet written anything to save. """
            return pd.DataFrame()
        
        if not self._perf_dir:
            self._perf_dir = blueshift_saved_performance_path(self._name)

        df = self.to_dataframe()
        fname = os_path.join(self._perf_dir, self.PERF_REPORTS_FILE)
        
        pft = import_pyfolio()
        init_cap = df.starting_cash.iloc[0]
        
        if ROLLING_STATS:
            df['sharpe'] = pft.rolling_sharpe(df.algo_returns, ROLLING_PERIOD)
            df['strategy_vol'] = pft.rolling_volatility(
                    df.algo_returns, ROLLING_PERIOD)
        
        if self._benchmark_rets is not None and len(self._benchmark_rets)>0:
            try:
                bennchmark_perf, benchmark_rets, bennchmark_pnls = \
                    self._get_aligned_benchmark(
                        df.algo_returns, df.funding, init_cap, self._benchmark_rets)
            except Exception:
                pass
            else:
                df['benchmark'] = bennchmark_perf
                df['benchmark_returns'] = benchmark_rets
                df['bennchmark_pnls'] = bennchmark_pnls
                
                if ROLLING_STATS:
                    factors = pd.DataFrame(df.benchmark_returns)
                    factors.columns = ['beta']
                    coeffs = pft.rolling_regression(
                            df.algo_returns, factors, ROLLING_PERIOD)
                    df['alpha'] = coeffs['alpha']
                    df['beta'] = coeffs['beta']

        try:
            with open(fname, "w") as fp:
                df.to_csv(fp, float_format="%.4f", lineterminator='\n')
        except Exception as e:
            if isinstance(e, TerminationError):
                raise
            msg = f"failed to write blotter data to {fname}:"
            msg = msg + str(e)
            handling = ExceptionHandling.WARN
            raise DataWriteException(msg=msg, handling=handling)
        else:
            self._last_saved = timestamp
            return df

    def _read(self, timestamp):
        if not self._perf_dir:
            self._perf_dir = blueshift_saved_performance_path(self._name)
        try:
            fname = os_path.join(self._perf_dir, self.PERF_REPORTS_FILE)
            df = pd.read_csv(fname, index_col = 0)

            last_report_dt = self._last_saved_date(
                    self._perf_dir, timestamp,
                    pattern="report_[0-9]{8}.json",
                    prefix="report_")
        except (FileNotFoundError, JSONDecodeError):
            # we can recover from this, we will be missing past 
            # performance data, but the algo will run OK
            pass
        except Exception as e:
            if isinstance(e, TerminationError):
                raise
            msg = f"failed to read blotter data from {fname}:"
            msg = msg + str(e)
            handling = ExceptionHandling.TERMINATE
            raise DataReadException(msg=msg, handling=handling)
        else:
            if len(df) < 1:
                return
            
            df.index = pd.to_datetime(df.index, utc=True)
            self._array_idx = np.uint64(df.index.values)
            self._acct_report = np.float64(df[AccountReportCols].values)
            self._pos_count_report = np.float64(df[PositionReportCols].values)
            self._pos_count_report = np.uint64(df[PositionCountReportCols].values)
            self._perf_report = np.float64(df[PerformanceCols].values)

            self._pos_idx = len(df)-1
            self.current_performance = df.iloc[-1].to_dict()

    cdef _set_current_performance(self, BlotterAccount account):
        """ set the values of the account keys here. """
        self.current_performance["net"] = account.net
        self.current_performance["cash"] = account.cash
        self.current_performance["holdings"] = account.holdings
        self.current_performance["cost_basis"] = account.cost_basis
        self.current_performance["starting_cash"] = account.starting_cash
        self.current_performance["funding"] = account.funding
        self.current_performance["margin"] = account.margin
        self.current_performance["mtm"] = account.mtm
        self.current_performance["realized"] = account.realized
        self.current_performance["pnls"] = account.pnls
        self.current_performance["liquid_value"] = account.liquid_value
        self.current_performance["commissions"] = account.commissions
        self.current_performance["charges"] = account.charges
        #self.current_performance["day_pnl"] = account.day_pnl
        # initialize at 0, update during performance computation
        self.current_performance["day_pnl"] = 0

        self.current_performance["gross_leverage"] = account.gross_leverage
        self.current_performance["net_leverage"] = account.net_leverage
        self.current_performance["gross_exposure"] = account.gross_exposure
        self.current_performance["net_exposure"] = account.net_exposure
        self.current_performance["long_exposure"] = account.long_exposure
        self.current_performance["short_exposure"] = account.short_exposure

        self.current_performance["longs_count"] = account.longs_count
        self.current_performance["shorts_count"] = account.shorts_count

    cdef _check_and_allocate_array(self):
        """
            This is costly, we will be copying the arrays.
            This should be called rarely during an algo run.
        """
        if self._pos_idx < len(self._array_idx):
            return
        
        acct_report =  np.zeros((self.CHUNK_SIZE, self.acct_cols),
                                dtype=np.float64)
        self._acct_report = np.concatenate((self._acct_report,acct_report))

        pos_report =  np.zeros((self.CHUNK_SIZE, self.pos_cols),
                               dtype=np.float64)
        self._pos_report = np.concatenate((self._pos_report,pos_report))

        pos_count_report =  np.zeros((self.CHUNK_SIZE, self.pos_count_cols),
                                     dtype=np.uint64)
        self._pos_count_report = np.concatenate((self._pos_count_report,
                                                 pos_count_report))

        perf_report =  np.zeros((self.CHUNK_SIZE, self.perf_cols),
                                dtype=np.float64)
        self._perf_report = np.concatenate((self._perf_report,perf_report))

        array_idx = np.zeros((self.CHUNK_SIZE), dtype=np.int64)
        self._array_idx = np.concatenate((self._array_idx,array_idx))

    cpdef update_metrics(self, BlotterAccount account, object orders,
                         object positions, np.int64_t timestamp):
        """
            Update the latest performance metrics. This checks and allocate
            memory dynamically, updates the account metrics, computes
            position metrics, risks and performance metrics.

            Args:
                ``account (obj)``: Blotter virtual account with current data.

        """
        self._check_and_allocate_array()
        self._update_account_metrics(account)
        self._update_positions_metrics(account)
        self._update_positions_counts(account)
        self._set_current_performance(account)
        self._update_perf_metrics()
        self._update_risk_metrics()

        cdef np.float64_t [:, :] acct_view = self._acct_report

        self._array_idx[self._pos_idx] = timestamp
        account.set_day_pnl(acct_view[self._pos_idx,12])

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef _update_account_metrics(self, BlotterAccount account):
        """ assing the values to the current index position. """
        cdef np.float64_t [:, :] acct_view = self._acct_report

        acct_view[self._pos_idx,0] = round(account.net, _PRECISION)
        acct_view[self._pos_idx,1] = round(account.cash, _PRECISION)
        acct_view[self._pos_idx,2] = round(account.holdings, _PRECISION)
        acct_view[self._pos_idx,3] = round(account.cost_basis, _PRECISION)
        acct_view[self._pos_idx,4] = round(account.starting_cash, _PRECISION)
        acct_view[self._pos_idx,5] = round(account.margin, _PRECISION)
        acct_view[self._pos_idx,6] = round(account.mtm, _PRECISION)
        acct_view[self._pos_idx,7] = round(account.realized, _PRECISION)
        acct_view[self._pos_idx,8] = round(account.pnls, _PRECISION)
        acct_view[self._pos_idx,9] = round(account.liquid_value, _PRECISION)
        acct_view[self._pos_idx,10] = round(account.commissions, _PRECISION)
        acct_view[self._pos_idx,11] = round(account.charges, _PRECISION)
        #acct_view[self._pos_idx,12] = account.day_pnl
        # initialize at 0, update during performance computation
        acct_view[self._pos_idx,12] = 0
        acct_view[self._pos_idx,13] = round(account.funding, _PRECISION)

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef _update_positions_metrics(self, BlotterAccount account):
        """ assing the values to the current index position. """
        cdef np.float64_t [:, :] pos_view = self._pos_report

        pos_view[self._pos_idx,0] = round(account.gross_leverage, _PRECISION)
        pos_view[self._pos_idx,1] = round(account.net_leverage, _PRECISION)
        pos_view[self._pos_idx,2] = round(account.gross_exposure, _PRECISION)
        pos_view[self._pos_idx,3] = round(account.net_exposure, _PRECISION)
        pos_view[self._pos_idx,4] = round(account.long_exposure, _PRECISION)
        pos_view[self._pos_idx,5] = round(account.short_exposure, _PRECISION)



    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef _update_positions_counts(self, BlotterAccount account):
        """ assing the values to the current index position. """
        cdef np.uint64_t [:, :] pos_count_view = self._pos_count_report

        pos_count_view[self._pos_idx,0] = account.longs_count
        pos_count_view[self._pos_idx,1] = account.shorts_count



    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef _update_perf_metrics(self):
        """ do a check on valid index pos and assign the values. """

        cdef np.float64_t [:, :] account_view = self._acct_report
        cdef np.float64_t [:, :] perf_view = self._perf_report
        cdef np.float64_t vol = <np.float64_t>(0)
        cdef np.float64_t draw_down = <np.float64_t>(0)
        cdef np.float64_t annualization_factor = <np.float64_t>(15.874508)

        if account_view[self._pos_idx, 0]==0:
            return
        
        cdef np.float64_t starting_cash = self.current_performance["starting_cash"]
        cdef np.float64_t funding = self.current_performance["funding"]
        cdef np.float64_t deployed = 0
        cdef np.float64_t pnls = account_view[self._pos_idx, 8]
        cdef np.float64_t returns = 0
        
        if self._pos_idx==0:
            deployed = starting_cash + funding
            returns = round(pnls/deployed, _P_PRECISION)
            self.current_performance["algo_returns"] = returns
            # we are on the first day. Period return same as 
            # cumulative returns. Day returns just the diff between
            # current net and starting cash (capital). Also we 
            # update the account report day_pnl here, in addition 
            # to current performance day pnl
            self.current_performance["algo_cum_returns"] = returns
            account_view[self._pos_idx,12] = pnls
            self.current_performance["day_pnl"] = account_view[self._pos_idx,12]
            perf_view[self._pos_idx,0] = returns
            perf_view[self._pos_idx,1] = returns
            vol = np.sqrt(returns*returns)*annualization_factor
            vol = round(vol,_P_PRECISION)
            draw_down = round(min(0, returns),_P_PRECISION)
            perf_view[self._pos_idx,2] = vol
            perf_view[self._pos_idx,3] = draw_down
            self.current_performance["algo_volatility"] = vol
            self.current_performance["drawdown"] = draw_down
            return

        # we have rolled at least once
        cdef np.float64_t last_pnls = account_view[self._pos_idx-1, 8]
        cdef np.float64_t cum_returns = perf_view[self._pos_idx-1,1]
        # deployed is cash + last day's pnls
        deployed = starting_cash + funding + account_view[self._pos_idx-1,12]
        #returns = round((pnls-last_pnls)/(deployed+last_pnls), _P_PRECISION)
        returns = round((pnls-last_pnls)/deployed, _P_PRECISION)
        self.current_performance["algo_returns"] = returns
        cum_returns = round(
                (1+cum_returns)*(1+returns) - 1, _P_PRECISION)

        perf_view[self._pos_idx,0] = returns
        perf_view[self._pos_idx,1] = cum_returns
        # update algo returns, day pnls for current perofmrance 
        # as well as day pnl for account report.
        self.current_performance["algo_cum_returns"] = cum_returns
        account_view[self._pos_idx,12] = round(
                pnls - last_pnls, _PRECISION)
        self.current_performance["day_pnl"] = account_view[self._pos_idx,12]

        self._calc_vol_drawdown(account_view, perf_view)

    cdef _update_risk_metrics(self):
        pass

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef _calc_vol_drawdown(self,
                            np.float64_t [:, :] account_view,
                            np.float64_t [:, :] perf_view):

        cdef np.float64_t annualization_factor = <np.float64_t>(15.874508)
        cdef np.float64_t vol = <np.float64_t>(0)
        cdef int n = self._pos_idx+1
        cdef np.float64_t current_ret = perf_view[self._pos_idx,0]
        cdef np.float64_t last_vol = perf_view[self._pos_idx-1,2]\
                                            /annualization_factor

        vol = np.sqrt((last_vol*last_vol*(n-1) + current_ret*current_ret)/n)
        vol = round(vol*annualization_factor, _P_PRECISION)
        perf_view[self._pos_idx,2] = vol
        self.current_performance["algo_volatility"] = vol

        cdef np.float64_t current_net = account_view[self._pos_idx,0]
        cdef np.float64_t net_max = 0
        cdef np.float64_t [:] net_view = self._acct_report[:,0]

        for i in range(self._pos_idx+1):
            if net_view[i] > net_max:
                net_max = net_view[i]

        cdef np.float64_t draw_down = round(current_net/net_max - 1, _P_PRECISION)
        draw_down = round(draw_down, _P_PRECISION)
        perf_view[self._pos_idx,3] = draw_down
        self.current_performance["drawdown"] = draw_down
        
    def set_benchmark(self, benchmark):
        """ set or update the benchmark for perormance analytics. """
        if not isinstance(benchmark, pd.Series):
            raise InitializationError('benchmark must be a pandas series.')
        
        if len(benchmark) == 0:
            returns = pd.Series()
        else:
            returns = benchmark.pct_change()
#            if self._mode == MODE.BACKTEST:
#                # shift the returns backward for a day, as we compute the EOD
#                # returns and tag it to the same date.
#                returns = returns.shift(1)
            returns = returns.fillna(0)
        
        self._benchmark_rets = returns
        self._benchmark = benchmark
        
    def update_benchmark(self, timestamp=None, benchmark=None):
        """ 
            Should be called before create_eod_report and save so that 
            benchmark performance is properly captured.
        """
        if self._benchmark is None:
            return
        
        if timestamp.tz:
            timestamp = timestamp.tz_convert(self.tz)
        else:
            timestamp.tz_localize(self.tz)
            
        timestamp = timestamp.normalize()
        new = pd.Series([benchmark],index=[timestamp])
        self._benchmark = self._benchmark.append(new)
        # refresh return computation, it can be optimized, but since it is 
        # used in live trading, not a performance blocker.
        self.set_benchmark(self._benchmark)
        
    def create_current_performance(self, trackers=None):
        if not trackers:
            return self.current_performance
        
        cols = AccountReportCols + PositionReportCols + \
            PositionCountReportCols + ['algo_returns']
            
        cp = self.current_performance.copy()
        
        for tracker in trackers:
            current_performance = tracker.current_performance
            for c in cols:
                if c in current_performance:
                    if c in cp:
                        cp[c] += current_performance[c]
                    else:
                        cp[c] = current_performance[c]
                    
        cp['algo_returns'] = cp['pnls']/(cp['starting_cash']+cp['funding'])
        cp['gross_leverage'] = cp['gross_exposure']/cp['net']
        cp['net_leverage'] = cp['net_exposure']/cp['net']
        
        return cp
    
    def create_performance(self, trackers=None):
        if not trackers:
            return self.to_dataframe()
        
        cols = AccountReportCols + PositionReportCols + \
            PositionCountReportCols + ['algo_returns']
        perfs = self.to_dataframe()
        if perfs.empty:
            return perfs
            
        perfs = perfs[cols]
        for tracker in trackers:
            df = tracker.to_dataframe()
            if not df.empty:
                df = df[cols]
                perfs = perfs.add(df, fill_value=0)
            
        if perfs.empty:
            return perfs
            
        # compute combined metrics
        deployed_caps = perfs['pnls'].shift(1) + perfs['starting_cash'] + perfs['funding']
        deployed_caps.iloc[0] = perfs.starting_cash.iloc[0] + perfs.funding.iloc[0]
        perfs['gross_leverage'] = perfs['gross_exposure']/perfs['net']
        perfs['net_leverage'] = perfs['net_exposure']/perfs['net']
        perfs['algo_returns'] = perfs['net']/deployed_caps -1
        perfs['algo_cum_returns'] = (1+perfs['algo_returns']).cumprod()
        perfs['algo_cum_returns'] = perfs['algo_cum_returns'] - 1
        perfs['algo_volatility'] = np.sqrt(
                (perfs['algo_returns']*perfs['algo_returns']).cumsum(
                        )/(1+np.arange(len(perfs))))
        perfs['algo_volatility'] = perfs['algo_volatility']*15.874508
        perfs['drawdown'] = perfs['algo_cum_returns']/perfs['algo_cum_returns'].cummax() - 1
        
        return perfs
        
    def create_eod_report(self, quick_mode=False, trackers=None):
        """ 
            Create end of day packet. For Live, this includes account 
            and risk metrics. For quick backtest, it will have a reduced 
            set of risk metrics. This method also accepts a list of 
            other trackers to create a combined report.
        """
        if not trackers:
            perfs = self.to_dataframe()
        else:
            perfs = self.create_performance(trackers)
        return self._create_eod_report(perfs, quick_mode=quick_mode)
    
    def _create_eod_report(self, perfs, quick_mode=False):
        base = {}
        
        if perfs.empty:
            base['performance'] = 0
            base['cumulative_returns'] = 0
            base['volatility'] = 0
            base['drawdown'] = 0
            return base
        
        perfs.index = perfs.index.normalize()
        returns = perfs['algo_returns']
        funding = perfs['funding']
        init_cap = perfs.starting_cash.iloc[0]
        algo_perf = perfs.net.iloc[-1]
        algo_vol = perfs.algo_volatility.iloc[-1]
        drawdown = min(perfs.drawdown)
        algo_cum_ret= perfs.algo_cum_returns.iloc[-1]
        full = not quick_mode
        
        base['performance'] = algo_perf
        base['cumulative_returns'] = algo_cum_ret
        base['volatility'] = algo_vol
        base['drawdown'] = drawdown
        
        bennchmark_perf = np.nan
        benchmark_rets = None
        bennchmark_pnls = None
        if self._benchmark_rets is not None and len(self._benchmark_rets)>0:
            try:
                bennchmark_perf, benchmark_rets, bennchmark_pnls = \
                    self._get_aligned_benchmark(
                        returns, funding, init_cap, self._benchmark_rets)
                bennchmark_perf = bennchmark_perf[-1]
                bennchmark_pnls = bennchmark_pnls.iloc[-1]
            except Exception:
                pass
        
        risks = {}
        try:
            risks = compute_eod_point_stats_report(
                    returns, benchmark_rets, full=full)
        except Exception:
            pass
        
        risks['benchmark'] = bennchmark_perf
        risks['benchmark_pnls'] = bennchmark_perf
        return {**base, **risks}
    
    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef ndarray[double] _cumulative_benchmark(
            self, float64_t[:] capitals, float64_t[:] returns):
        cdef Py_ssize_t i
        cdef Py_ssize_t n = len(returns)
        cdef double deployed = 0
        cdef ndarray[double] benchmark = np.zeros(n)
        cdef ndarray[double] pnls = np.zeros(n)
        
        deployed = capitals[0]
        pnls[0] = round(deployed*returns[0], _P_PRECISION)
        benchmark[0] = round(deployed + pnls[0], _P_PRECISION)
        
        for i in range(1, n):
            deployed = benchmark[i-1] + capitals[i] - capitals[i-1]
            pnls[i] = round(deployed*returns[i], _P_PRECISION)
            benchmark[i] = round(deployed + pnls[i], _P_PRECISION)
        
        return benchmark
    
    cpdef _get_aligned_benchmark(
            self, object returns, object funding, float init_cap, 
            object benchmark_rets):
        cdef ndarray[double] bennchmark_perf
        
        data = pd.DataFrame({'returns':returns,
                             'benchmark_rets':benchmark_rets})
        data = data.loc[returns.index]
        benchmark_rets = data['benchmark_rets'].fillna(0)
        capitals = init_cap + funding
        #bennchmark_perf = (1+benchmark_rets).cumprod()
        bennchmark_perf = self._cumulative_benchmark(capitals.values, benchmark_rets.values)
        shifted_benchmark = np.roll(bennchmark_perf, 1)
        shifted_benchmark[0] = 0
        bennchmark_pnls = (shifted_benchmark * benchmark_rets).cumsum()
  
        return bennchmark_perf, benchmark_rets, bennchmark_pnls