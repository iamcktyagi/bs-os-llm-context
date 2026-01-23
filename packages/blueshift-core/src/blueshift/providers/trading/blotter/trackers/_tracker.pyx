# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
from cpython cimport bool
cimport numpy as np
import numpy as np

import time
import warnings
from os import path as os_path
from os import listdir as os_listdir
import re
import pandas as pd
import bisect
import json
from collections import OrderedDict, deque
from json import JSONDecodeError

from blueshift.lib.trades._order cimport Order
from blueshift.lib.trades._position cimport Position
from blueshift.lib.trades._accounts cimport BlotterAccount
from blueshift.interfaces.trading._tracker cimport Tracker
from blueshift.interfaces.assets._assets cimport InstrumentType
from blueshift.lib.common._cutils cimport cdict_diff

from blueshift.lib.common.constants import CCY
from blueshift.lib.common.types import MaxSizedOrderedDict
from blueshift.lib.serialize.serialize import (
        read_positions_from_dict,read_transactions_from_dict, read_orders,
        read_blotter_account_from_disk, save_blotter_account_to_disk,
        read_portfolio_from_disk, save_portfolio_to_disk,
        save_transactions_to_disk, save_orderbook_to_disk,
        save_roundtrips_to_disk, read_roundtrips_from_disk, 
        read_orderbook_from_disk, read_transactions_from_disk)

from blueshift.config import (blueshift_saved_orders_path,
                               blueshift_saved_positions_path,
                               blueshift_saved_transactions_path,
                               blueshift_saved_performance_path,)
from blueshift.lib.exceptions import (
        BlueshiftException, ValidationError, ExceptionHandling,
        TerminationError, DataReadException, DataWriteException,
        BlueshiftWarning)


cdef float ALMOST_ZERO = 0.001

cdef class TransactionsTracker(Tracker):
    """ Implementation of transaction tracker using file system.
        This handles `transaction` as dict which stores a list of orders
        keyed by the dates. `open_order` is a dict that stores open orders
        keyed by an order id. `known_orders` is a dict of orders which are
        orders the algorithm knows about (past or current orders). This
        helps us to tally a order received from the broker. If the
        order is in known order set we process further. Else we flag
        it as a reconciliation mismatch. `unprocessed_orders` is a
        list of orders added during `add_transaction` and is yet to
        be reconciled. `last_saved`, `last_reconciled` etc. keeps
        track of save and reconciliation etc.

        Note:
            Transactions stores dated list of orders executed (or cancelled
            ) by the algo. This is stored within a `deque` structure with a
            max length of ``MAX_TRANSACTIONS``, currently set at `5000`.
            Algos ordering more than this limit will lose some details.
            Similarly, known orders dict is also a `FIFO` dict with the
            same max size. Usually this is sufficient, but for algos which
            generates orders close to this limit per day may malfunction.

            If mode is LIVE, the data are saved daily, else once at the
            end of the backtest. Hence no read/save during a backtest
            run (except at the end).

        Args:
            ``name (str)``: Name of the current run.

            ``mode (int)``: Run MODE of the current run.

            ``timestamp (Timestamp)``: Current timestamp.

            ``ccy (object)``: Currency in which to track.

        Returns:
            None.
        """

    def __init__(self, object name, object mode, object timestamp=None,
                 object ccy = CCY.LOCAL):
        """
            if mode is LIVE, the data are saved daily, else once at the
            end of the backtest. Hence no read/save during a backtest
            run (except at the end).
        """
        super(TransactionsTracker, self).__init__(name,mode,timestamp,ccy)
        self.MAX_TRANSACTIONS = 50000
        self.ROUND_TRIPS_FILE = "round_trips.json"
        self.OPEN_TRADES_FILE = "open_trades.json"
        self.UNPROCESSED_FILE = "unprocessed.json"

        self._orders_dir = None
        self._txn_dir = None
        self._known_orders = MaxSizedOrderedDict(
                    {},max_size=self.MAX_TRANSACTIONS,chunk_size=5000)

        self.reset()

    def reset(self, object last_saved=None, object last_reconciled=None,
                bool needs_reconciliation=False,
                bool last_reconciliation_status=False,
                object initial_positions={}):
        """
            Reset internal data. This carries forward flags from the
            last known state, if supplied.

            Args:
                ``last_saved (Timestamp)``: last save time.

                ``last_reconciled (Timestamp)``: last reconciliation time.

                ``needs_reconciliation (bool)``: if reconciliation needed.

                ``last_reconciliation_status (bool)``: if last was success.
        """
        self._transactions = OrderedDict({})
        self._open_orders = {}
        self._unprocessed_orders = {}
        self._round_trips = deque([], maxlen=self.MAX_TRANSACTIONS)
        self._open_trades = initial_positions
        self._missing_orders = {}

        # for tracking round trip total realized pnls
        self._round_trips_realized = 0
        # cash adjustment as we add cash outflow from orders as well as
        # realized profits from positions. This will doboule-count the
        # pnls on cash instruments.
        self._cash_adj = 0

        self._last_reconciled = last_reconciled
        self._last_saved = last_saved
        self._needs_reconciliation = needs_reconciliation
        self._last_reconciliation_status = last_reconciliation_status

    def roll(self, asset_finder, timestamp):
        """
            We load the last saved transactions and positions and reset
            / reload the tracked quantities.

            Args:
                ``asset_finder (object)``: An asset finder object.

                ``timestamp (Timestamp)``: current timestamp .
        """
        # save current data.
        self._save(timestamp)
        # carry forward the status flags and unprocessed orders if any
        # rest we read back from saved files anayways
        last_saved = self._last_saved
        last_reconciled = self._last_reconciled
        needs_reconciliation = self._needs_reconciliation
        last_reconciliation_status = self._last_reconciliation_status
        unprocessed_orders = self._unprocessed_orders
        self.reset(last_saved, last_reconciled, needs_reconciliation,
                    last_reconciliation_status)
        # reset the unprocessed orders
        self._unprocessed_orders = unprocessed_orders

        # reload last saved.
        self._read(asset_finder, timestamp)
        
    def _save(self, timestamp):
        """
            Save the current transaction list and open orders.

            Note:
                The timestamp argument is used to generate the file name.

            Args:
                ``timestamp (Timestamp)``: timestamp of the call.
        """
        txns_write = {}
        open_orders = {}
        unprocessed = {}
        open_trades = {}
        
        try:
            # process the closed transactions.
            txns_write = save_transactions_to_disk(self._transactions)
            txns_write = json.loads(txns_write)
        except Exception as e:
            msg = f'Failed to process transactions data, (but continued '
            msg += f'saving blotter data):{str(e)}.'
            warnings.warn(msg, BlueshiftWarning)

        # process the open orders, if any
        try:
            open_orders = save_orderbook_to_disk(self._open_orders)
            open_orders = json.loads(open_orders)
        except Exception as e:
            msg = f'Failed to process open orders data, (but continued '
            msg += f'saving blotter data):{str(e)}.'
            warnings.warn(msg, BlueshiftWarning)
            
        # process the _unprocessed orders orders, if any
        try:
            unprocessed = save_orderbook_to_disk(self._unprocessed_orders)
            unprocessed = json.loads(unprocessed)
        except Exception as e:
            msg = f'Failed to process unprocessed orders data, (but continued '
            msg += f'saving blotter data):{str(e)}.'
            warnings.warn(msg, BlueshiftWarning)

        # process the current open positions, if any
        try:
            open_trades = save_portfolio_to_disk(self._open_trades)
            open_trades = json.loads(open_trades)
        except Exception as e:
            msg = f'Failed to process open trades data, (but continued '
            msg += f'saving blotter data):{str(e)}.'
            warnings.warn(msg, BlueshiftWarning)

        try:
            # write down cumulative pnls on round trips plus open trades.
            open_trades_write = {'pnls':self._round_trips_realized,
                                 'cash_adj':self._cash_adj,
                                 'positions':open_trades}
            if not self._txn_dir:
                self._txn_dir = blueshift_saved_transactions_path(self._name)
            fname = os_path.join(self._txn_dir, self.OPEN_TRADES_FILE)
            with open(fname, 'w') as fp:
                json.dump(open_trades_write, fp)

            if txns_write:
                # dated output based on the timestamp.
                fname = self._prefix_fn(timestamp)
                fname = os_path.join(self._txn_dir, fname)
                with open(fname, 'w') as fp:
                    json.dump(txns_write, fp)
                    self._last_saved = timestamp
            
            if not self._orders_dir:
                self._orders_dir = blueshift_saved_orders_path(self._name)
            if open_orders:
                fname = self._prefix_fn(timestamp)
                fname = os_path.join(self._orders_dir, fname)
                with open(fname, 'w') as fp:
                    json.dump(open_orders, fp)
                    self._last_saved = timestamp
                    
            if unprocessed:
                fname = os_path.join(self._orders_dir, self.UNPROCESSED_FILE)
                with open(fname, 'w') as fp:
                    json.dump(unprocessed, fp)
                    self._last_saved = timestamp

            if self._round_trips:
                fname = os_path.join(self._txn_dir, self.ROUND_TRIPS_FILE)
                with open(fname, 'w') as fp:
                    json.dump(list(self._round_trips), fp)
        except (TypeError, OSError) as e:
            msg = f"failed to write blotter data to {fname}: {str(e)}."
            handling = ExceptionHandling.WARN
            raise DataWriteException(msg=msg, handling=handling)

    def _save2(self, timestamp):
        """
            Save the current transaction list and open orders.

            Note:
                The timestamp argument is used to generate the file name.

            Args:
                ``timestamp (Timestamp)``: timestamp of the call.
        """
        txns_write = OrderedDict()
        open_orders = {}
        open_trades = {}
        unprocessed = {}

        # process the closed transactions.
        for ts in self._transactions:
            if self._skip_ts(pd.Timestamp(ts), None):
                continue
            txns = self._transactions[ts]
            txns_list = []
            for txn in txns:
                txns_list.append(txn.to_json())
            txns_write[str(ts.normalize().tz_localize(None))] = txns_list

        # process the open orders, if any
        for order_id in self._open_orders:
            open_orders[str(order_id)] = self._open_orders[order_id].to_json()
            
        # process the _unprocessed orders orders, if any
        for order_id in self._unprocessed_orders:
            unprocessed[str(order_id)] = self._unprocessed_orders[order_id].to_json()

        # process the current open positions, if any
        for asset in self._open_trades:
            open_trades[asset.exchange_ticker] = self._open_trades[asset].to_json()

        try:
            # write down cumulative pnls on round trips plus open trades.
            open_trades_write = {'pnls':self._round_trips_realized,
                                 'cash_adj':self._cash_adj,
                                 'positions':open_trades}
            if not self._txn_dir:
                self._txn_dir = blueshift_saved_transactions_path(self._name)
            fname = os_path.join(self._txn_dir, self.OPEN_TRADES_FILE)
            with open(fname, 'w') as fp:
                json.dump(open_trades_write, fp)

            fname = self._prefix_fn(timestamp)
            if txns_write:
                # dated output based on the timestamp.
                fname = os_path.join(self._txn_dir, fname)
                with open(fname, 'w') as fp:
                    json.dump(txns_write, fp)
                    self._last_saved = timestamp
            
            if not self._orders_dir:
                self._orders_dir = blueshift_saved_orders_path(self._name)
            if open_orders:
                fname = os_path.join(self._orders_dir, fname)
                with open(fname, 'w') as fp:
                    json.dump(open_orders, fp)
                    self._last_saved = timestamp
                    
            if unprocessed:
                fname = os_path.join(self._orders_dir, self.UNPROCESSED_FILE)
                with open(fname, 'w') as fp:
                    json.dump(unprocessed, fp)
                    self._last_saved = timestamp

            if self._round_trips:
                fname = os_path.join(self._txn_dir, self.ROUND_TRIPS_FILE)
                with open(fname, 'w') as fp:
                    json.dump(list(self._round_trips), fp)
        except (TypeError, OSError) as e:
            msg = f"failed to write blotter data to {fname}:"
            msg = msg + str(e)
            handling = ExceptionHandling.WARN
            raise DataWriteException(msg=msg, handling=handling)

    def _read(self, asset_finder, timestamp):
        """
            read the last saved the transaction list at start-up or roll.
            This function reads any saved transactions for the last save
            date and also any open orders and populates internal lists.

            Note:
                The timestamp argument is forced to make sure we have
                timezone information. Else we would have to pass around
                a calendar object here or be okay with no tz. The asset
                finder object is required to convert serialized asset
                symbol (str) to an asset object.

            Args:
                ``asset_finder (object)``: asset finder to make assets.

                ``timestamp (Timestamp)``: current timestamp.
        """

        # read the open trades and cumulative pnls
        if not self._txn_dir:
                self._txn_dir = blueshift_saved_transactions_path(self._name)
        fname = os_path.join(self._txn_dir, self.OPEN_TRADES_FILE)
        if os_path.exists(fname):
            try:
                with open(fname) as fp:
                    open_trades_dict = dict(json.load(fp))
                self._round_trips_realized = open_trades_dict['pnls']
                self._cash_adj = open_trades_dict['cash_adj']
                open_trades = open_trades_dict["positions"]
                
                if 'version' in open_trades_dict["positions"]:
                    self._open_trades = read_portfolio_from_disk(
                            open_trades, asset_finder=asset_finder)
                else:
                    # legacy format
                    self._open_trades = read_positions_from_dict(
                            open_trades_dict["positions"], asset_finder)
            except Exception as e:
                if isinstance(e, TerminationError):
                    raise
                msg = f"illegal positions data in {fname}. "
                msg = msg + str(e)
                handling = ExceptionHandling.TERMINATE
                raise DataReadException(msg=msg, handling=handling)

        # read the list of round trips.
        fname = os_path.join(self._txn_dir, self.ROUND_TRIPS_FILE)
        if os_path.exists(fname):
            try:
                with open(fname) as fp:
                    round_trips = json.load(fp)
                if 'version' in round_trips:
                    self._round_trips = deque(read_roundtrips_from_disk(
                            round_trips), maxlen=self.MAX_TRANSACTIONS)
                else:
                    # legacy format
                    self._round_trips = deque(
                            round_trips, maxlen=self.MAX_TRANSACTIONS)
            except JSONDecodeError:
                # recoverable, perf stats will be off,
                # but algo will run OK
                pass
            except Exception as e:
                if isinstance(e, TerminationError):
                    raise
                msg = f"illegal round trips data in {fname}:"
                msg = msg + str(e)
                handling = ExceptionHandling.WARN
                raise DataReadException(msg=msg, handling=handling)


        # now read the daily transactions list
        transactions = {}
        last_txn_dt = self._last_saved_date(self._txn_dir, timestamp)

        if last_txn_dt:
            fname = os_path.join(
                    self._txn_dir, self._prefix_fn(last_txn_dt))
            if os_path.exists(fname):
                # expected transactions keyed to date-time
                try:
                    with open(fname) as fp:
                        txns_dict = dict(json.load(fp))
                    if 'version' in txns_dict:
                        txns = read_transactions_from_disk(
                                txns_dict, asset_finder=asset_finder)
                        ids = [k2 for k1,v1 in txns.items() for k2 in v1]
                    else:
                        # legacy format
                        txns, ids = read_transactions_from_dict(
                                txns_dict, asset_finder, pd.Timestamp,
                                tz=timestamp.tz)
                    
                    transactions = txns
                    d = {k:True for k in ids}
                    self._known_orders.update(d)
                except JSONDecodeError:
                    # recoverable, we may have reconciliation errors,
                    # but the algo should run OK.
                    pass
                except Exception as e:
                    if isinstance(e, TerminationError):
                        raise
                    msg = f"illegal transactions data in {fname}:"
                    msg = msg + str(e)
                    handling = ExceptionHandling.TERMINATE
                    raise DataReadException(msg=msg, handling=handling)

            # convert transactions elements to deque
            txns_keys = transactions.keys()
            for key in txns_keys:
                transactions[key] = deque(
                        transactions[key], maxlen=self.MAX_TRANSACTIONS)
            self._transactions = transactions
            
        if not self._orders_dir:
            self._orders_dir = blueshift_saved_orders_path(self._name)
        last_order_dt = self._last_saved_date(self._orders_dir, timestamp)
        if last_order_dt:
            fname = os_path.join(
                    self._orders_dir, self._prefix_fn(last_order_dt))
            if os_path.exists(fname):
                try:
                    with open(fname) as fp:
                        open_orders = dict(json.load(fp))
                    if 'version' in open_orders:
                        self._open_orders = read_orderbook_from_disk(
                                open_orders, asset_finder=asset_finder)
                        ids = [k for k in self._open_orders]
                    else:
                        # legacy format
                        self._open_orders, ids = read_orders(
                                open_orders, asset_finder)
                    d = {k:True for k in ids}
                    self._known_orders.update(d)
                except Exception as e:
                    if isinstance(e, TerminationError):
                        raise
                    msg = f"illegal orders data in {fname}:"
                    msg = msg + str(e)
                    handling = ExceptionHandling.TERMINATE
                    raise DataReadException(msg=msg, handling=handling)
                    
        # read the list of unprocessed orders.
        fname = os_path.join(self._orders_dir, self.UNPROCESSED_FILE)
        if os_path.exists(fname):
            try:
                with open(fname) as fp:
                    unprocessed = json.load(fp)
                    if 'version' in unprocessed:
                        self._unprocessed_orders = read_orderbook_from_disk(
                                unprocessed, asset_finder=asset_finder)
                    else:
                        # legacy format
                        self._unprocessed_orders, ids = read_orders(
                                unprocessed, asset_finder)
            except Exception as e:
                if isinstance(e, TerminationError):
                    raise
                msg = f"illegal unprocessed orders data in {fname}:"
                msg = msg + str(e)
                handling = ExceptionHandling.TERMINATE
                raise DataReadException(msg=msg, handling=handling)

    cpdef add_transaction(self, object order_id, Order order):
        """
            Log a transaction with the tracker. This include registering
            an unprocessed order, updating the `known_orders` set, and
            setting the `needs_reconciliation` flag to True.

            Args:
                ``order_id (str)``: ID for the order.
                ``order (object)``: Details of the order.
        """
        self._unprocessed_orders[order_id] = order
        self._known_orders[order_id] = True
        self._needs_reconciliation = True

    cdef _update_transactions(self, Order order, object timestamp):
        dt = order.timestamp.normalize().tz_convert(
                        tz=timestamp.tz)
        order_list = self._transactions.get(
                dt,deque([], maxlen=self.MAX_TRANSACTIONS))
        order_list.append(order)
        self._transactions[dt] = order_list

    cpdef reconcile(self, object timestamp, object orders,
                    object synthetic_orders):
        """
            process the un-processed orders list. If missing order
            leave it in the un-processed list to check in the next run.

            Args:
                ``timestamp (Timestamp)``: Timestamp of the call.

                ``orders (dict)``: A dict of orders receivecd from broker.

                ``synthetic_orders (dict)``: A dict of synthetic orders receivecd from broker.

            Returns:
                Tuple. ``matched`` is True if reconciliation is achieved.
                ``missing`` orders are orders that are being tracked but
                does not appear in the list fetched from the broker.
                ``extra`` are the orders which are not tracked but in the
                list fetched from the broker (probably placed manually).
                Finally, ``matched`` orders is a list of matched orders.
        """

        cdef bool matched = False
        open_orders_missing = []
        cdef Order order
        missing_orders = []
        extra_orders = []
        matched_orders = {}
        unprocessed_orders = list(self._unprocessed_orders.keys())

        # first check the open orders from last batch.
        # if a key not in orders, that means it is partially cancelled
        # else, we received update. Check if it is open. If still open,
        # we just update the open order. Else if closed, add it to the
        # matched orders list, as well as transactions list.
        open_order_keys = list(self._open_orders.keys())
        for key in open_order_keys:
            o = self._open_orders[key]
            
            if key not in orders:
                # if synthetic order, we handle them separately
                if key in synthetic_orders:
                    continue
                # we should not be here. We raise error in blotter.
                o.update_from_dict(
                        {'status_message':f'Missing order {key} from broker data.'}
                        )
                o.set_exchange_timestamp(timestamp)
                if key in self._missing_orders:
                    self._missing_orders[key] = \
                            self._missing_orders[key] + 1
                else:
                    self._missing_orders[key] = 1
            else:
                # remove from missing order
                if key in self._missing_orders:
                    self._missing_orders.pop(key, None)
                
                # get the latest versions
                order = orders[key]
                
                # preserver entry time & update exchange timestamp
                order.set_timestamp(o.timestamp)
                order.set_exchange_timestamp(timestamp)
                
                # update the existing object, preserving trackers
                o.update_from_dict(order.to_dict(), preserve_trackers=True)
                
                if not o.is_open():
                    # else add to matched list and remove from list.
                    self._open_orders.pop(key, None)
                    self._update_transactions(o, timestamp)
                    matched_orders[key] = o

        # now check the current batch of unprocessed orders
        # if found in orders list, check if it is closed. If closed, add
        # to matched orders and transaction list. Else if open, add it to
        # the open orders list.
        for order_id in unprocessed_orders:
            o = self._unprocessed_orders[order_id]
            
            if order_id in orders:
                # get the latest version
                order = orders[order_id]
                
                # preserver entry time & update exchange timestamp
                order.set_timestamp(o.timestamp)
                order.set_exchange_timestamp(timestamp)
                    
                # update the existing object while preserving trackers
                o.update_from_dict(order.to_dict(), preserve_trackers=True)

                if not o.is_open():
                    # they can no longer change resulting positions
                    self._update_transactions(o, timestamp)
                    matched_orders[order_id] = o
                    self.pop_missing_order(order_id)
                else:
                    # these still can, as they get filled.
                    self._open_orders[order_id] = o
                    self.pop_missing_order(order_id)
            else:
                # we should not be here!
                o.update_from_dict(
                        {'status_message':f'Missing new order {order_id} from broker data.'}
                        )
                o.set_exchange_timestamp(timestamp)
                if order_id in self._missing_orders:
                    self._missing_orders[order_id] = \
                            self._missing_orders[order_id] + 1
                else:
                    self._missing_orders[order_id] = 1

        for key in orders:
            if key in synthetic_orders or key in self._known_orders:
                continue
            else:
                # manual orders or other algo order mangling!
                extra_orders.append(orders[key])

        # all synthetic orders are generated by this algo, but the ids
        # are not known to the blotter. We add to matched order and
        # clear the list.
        sythnetic_list = list(synthetic_orders.keys())
        for key in sythnetic_list:
            self._known_orders[key] = True
            o, margin = synthetic_orders[key]
            if not o.is_open():
                matched_orders[key] = o
                synthetic_orders.pop(key, None)
                self._open_orders.pop(key, None)
            else:
                self._open_orders[key] = o

        missing_orders = list(self._missing_orders.keys())
        if missing_orders:
            matched = False
        else:
            matched = True

        self._needs_reconciliation = False
        self._last_reconciled = timestamp

        self._last_reconciliation_status = matched
        return matched, missing_orders, extra_orders, matched_orders, open_orders_missing

    cpdef update_positions_from_orders(self, object orders, object broker,
                                       object data_portal):
        """
            create a list of positions from orders - loop throug orders
            and convert them to positions, then loop through open orders
            and do the same. The handling of positions against closed
            and open orders are different. For closed orders, we update
            the existing positions against that asset. This is safe as
            we will have no further updates from the order. For open
            order, we can get multiple updates on the same open order
            for different calls, with different fills. So each time, we
            just overwrite the positions against open orders.

            Note:
                they may not have the correct price and pnl information.
                This requires further update.

            Args:
                ``orders (dict)``: A dict of orders keyed by ID.

                ``broker (object)``: The broker object.

                ``data_portal (object)``: A data portal object.

            Returns:
                Dict. Dictionary of open positions keyed by assets.
        """
        # first loop through the matched orders and update the positions
        # these are orders which are fronzen (can no longer change) and
        # hence can no longer create any incremental positions

        cdef Position pos
        cdef Order order
        cdef np.float64_t margin=0

        keys = list(orders.keys())
        for key in keys:
            order = orders.pop(key)

            if order.filled == 0:
                continue

            asset = order.asset
            last_fx = asset.fx_rate(self._ccy, data_portal)
            pos = Position.from_order(order, margin=margin,
                                      last_fx=last_fx)

            if asset in self._open_trades:
                self._open_trades[asset].add_to_position(pos)
            else:
                self._open_trades[asset] = pos

        open_trades_keys = list(self._open_trades.keys())
        for asset in open_trades_keys:
            if self._open_trades[asset].if_closed():
                pos = self._open_trades.pop(asset)
                self._round_trips.append(pos.to_json())
                self._round_trips_realized = pos.realized_pnl \
                                    + self._round_trips_realized
                if asset.is_funded() or asset.is_opt():
                    self._cash_adj = self._cash_adj + pos.realized_pnl

        # now if there is any open orders, they still can have incremental
        # impact on positions for each reconciliation call
        open_positions = {}
        for key in self._open_orders:
            order = self._open_orders[key]
            if order.filled > 0:
                last_fx = order.asset.fx_rate(self._ccy, data_portal)
                pos = Position.from_order(order, margin=margin,
                                          last_fx=last_fx)
                # in case we already have some position
                if pos.asset in open_positions:
                    open_positions[pos.asset].add_to_position(pos)
                else:
                    open_positions[pos.asset] = pos

        # the overall outstanding position is sum of frozen (open trades)
        # and open positions.
        return open_positions

    cpdef flatten_transactions_dict(self):
        """ flatten the txns stored by dates to a dict by order id. """
        cdef Order txn

        txns = MaxSizedOrderedDict(
                {}, max_size=self.MAX_TRANSACTIONS, chunk_size=5000)
        for ts in self._transactions:
            transactions = self._transactions[ts]
            for txn in transactions:
                txns[txn.oid] = txn
        return txns

    cpdef open_order_cash_outlay(self, data_portal):
        """ track the cashout lay for open orders. """
        cdef float cash = 0

        for order_id in self._open_orders:
            cash = cash + self._open_orders[order_id].\
                                    cash_outlay(self._ccy, data_portal)

        return cash

    cpdef if_update_required(self, object broker):
        """
            flag if any update required for this tracker. It checks
            if there are outstanding unprocessed orders (added but
            not yet reconciled), or open positions implied from orders
            or from brokers, and triggers a positive response.

            Args:
                ``broker (object)``: The broker object
        """
        if self._unprocessed_orders or self._open_orders or\
                        broker.synthetic_orders:
            return True

        return False

    def pop_missing_order(self, order_id):
        self._unprocessed_orders.pop(order_id, None)
        self._missing_orders.pop(order_id, None)


cdef class PositionTracker(Tracker):
    """
        Position tracker implements a tracking class to follow positions
        out of an algorithm. At the heart, it keeps track of all open
        positions (including positions from partial fills), update the
        positions valuation on each reconciliation call and handle
        saving daily positions report. Since all positions are generated
        by transactions, this tracker depends entirely on transactions
        tracker to update it with information.

        Note:
            Positions data are saved to disk only for live runs.

        Args:
            ``name (str)``: Name of the current run.

            ``mode (int)``: Run MODE of the current run.

            ``timestamp (Timestamp)``: Current timestamp.

            ``ccy (object)``: Currency in which to track.

        Returns:
            None.

    """

    def __init__(self, object name, object mode, object timestamp=None,
                 object ccy=CCY.LOCAL):
        super(PositionTracker, self).__init__(name,mode,timestamp, ccy)
        self._pos_dir = None
        self._init_pos = {}

        self.reset()

    def reset(self, object last_saved=None, object last_reconciled=None,
               bool needs_reconciliation=False,
               bool last_reconciliation_status=False,
               object initial_positions={}):
        
        self._current_pos = initial_positions
        self._init_pos = initial_positions

        self._last_reconciled = last_reconciled
        self._last_saved = last_saved
        self._needs_reconciliation = needs_reconciliation
        self._last_reconciliation_status = last_reconciliation_status

    def roll(self, object asset_finder, object timestamp):
        """
            We load the last saved positions and reset the tracked
            quantities.
        """
        # save current data.
        self._save(timestamp)
        # carry forward the status flags
        last_saved = self._last_saved
        last_reconciled = self._last_reconciled
        needs_reconciliation = self._needs_reconciliation
        last_reconciliation_status = self._last_reconciliation_status
        self.reset(last_saved, last_reconciled, needs_reconciliation,
                    last_reconciliation_status)

        # reload last saved.
        self._read(asset_finder, timestamp)

    cpdef update_current_pos(self, object open_trades,
                             object open_order_pos):
        """
            we modify the open order pos, as otherwise this can impact
            open_trades without deepcopy.
        """
        cdef Position pos

        self._current_pos = open_order_pos
        for asset in open_trades:
            pos = open_trades[asset]
            if asset in self._current_pos:
                self._current_pos[asset].add_to_position(pos)
            else:
                self._current_pos[asset] = pos

    def _read(self, object asset_finder, object timestamp):
        """
            Read the latest saved positions from disk. This function
            looks for the last saved positions and read them in to
            open and closed positions, and finally combine to total
            positions.

            Args:
                ``asset_finder (object)``: An asset finder object

                ``timestamp (Timestamp)``: Timestamp of call

            Returns:
                None. Updates internal data.
        """
        current_pos = {}
        
        if not self._pos_dir:
            self._pos_dir = blueshift_saved_positions_path(self._name)
        last_pos_dt = self._last_saved_date(self._pos_dir, timestamp)
        if last_pos_dt:
            fname = os_path.join(
                    self._pos_dir, self._prefix_fn(last_pos_dt))
            if os_path.exists(fname):
                try:
                    with open(fname) as fp:
                        positions_dict = dict(json.load(fp))
                    if 'version' in positions_dict:
                        current_pos = read_portfolio_from_disk(
                                positions_dict, asset_finder=asset_finder)
                    else:
                        # legacy format
                        current_pos = read_positions_from_dict(
                                positions_dict, asset_finder)
                except Exception as e:
                    if isinstance(e, TerminationError):
                        raise
                    msg = f"illegal positions data in {fname}:"
                    msg = msg + str(e)
                    handling = ExceptionHandling.TERMINATE
                    raise DataReadException(msg=msg, handling=handling)

        if current_pos:
            self._current_pos = current_pos

    def _save(self, object timestamp):
        """
            Save positions data to disk. This saves the "closed" positions
            and the "open" positions (against open order which can further
            change) under "closed" and "open" keys in a dict (converted
            to json).

            Args:
                ``timestamp (Timestamp)``: Timestamp of call.

            Returns:
                None. Updates internal data.
        """
        pos_write = {}
        if not self._pos_dir:
            self._pos_dir = blueshift_saved_positions_path(self._name)
                
        try:
            fname = self._prefix_fn(timestamp)
            fname = os_path.join(self._pos_dir, fname)
            x = save_portfolio_to_disk(self._current_pos)
            save_portfolio_to_disk(self._current_pos, path=fname)
        except Exception as e:
            msg = f"failed to write positions data to {fname}:"
            msg = msg + str(e)
            handling = ExceptionHandling.WARN
            raise DataWriteException(msg=msg, handling=handling)

    @property
    def positions(self):
        return self._current_pos

    cpdef reconcile(self, object timestamp, object positions,
                    object data_portal):
        """
            Reconciliation of positions. The argument `positions` is
            NOT used for virtual trackers (but used for exclusive 
            trackers).

            Note:
                Before calling this function, make sure the current
                position of this tracker is updated.

            Args:
                ``timestamp (Timestamp)``: Timestamp of call

                ``positions (dict)``: Dict of positions from broker

                ``data_portal (obj)``: to query current account FX conversion

            Returns:
                Tuple. A boolean indicating success of the reconciliation
                , and a dict indicating conflicting positions reported
        """
        cdef bool matched
        cdef np.float64_t last_price, last_fx

        matched = True
        for asset in self._current_pos:
            if self._local_ccy and asset.ccy == CCY.LOCAL:
                last_fx = 1.0
            else:
                last_fx = asset.fx_rate(self._ccy, data_portal)
            
            # add the case where the position can be missing from broker
            last_price = data_portal.current(asset, 'close')
                
            self._current_pos[asset].update_price(
                    last_price, last_fx)

        self._needs_reconciliation = False
        self._last_reconciled = timestamp
        self._last_reconciliation_status = matched

        return matched, {}
    
    def unexplained_positions(self, positions):
        """ 
            Difference in positions in broker account vs algo. This 
            is meaningless for a virtual account.
        """
        unexplained_pos = cdict_diff(self._current_pos, positions)

        if unexplained_pos:
            matched = False
        else:
            matched = True
            
        return matched, unexplained_pos

    def update_valuation(self, data_portal, timeout = None,
                         broker=None):
        """
            Update the portfolio valuations. This method will only
            update valuation if the underlying data portal support
            a streaming data interface.

            Note:
                This runs synchronously, as it must. But it cannot take
                a long time. This is usually called from a monitor
                co-routine with lower priority than the main algo. So
                we must implement a co-operative timeout within the
                function itself to check amount of time it is taking
                and break if a threshold (timeout) is breached.

            Args:
                ``data_portal (object)``: data portal to fetch prices.

                ``timeout (float)``: timeout in seconds.

                ``broker (obj)``: Broker API object.

            Returns:
                Bool. True if the valuation changed from last.
        """
        if not data_portal.has_streaming():
            return False

        if not self._current_pos:
            return False

        before = {}
        after = {}

        chunks = 10
        loop_count = 0
        start = time.time()

        for asset in self._current_pos:
            pos = self._current_pos[asset]
            before[pos.asset.exchange_ticker] = pos.pnl
            last_price = data_portal.current(asset, 'close')
            last_fx = asset.fx_rate(self._ccy, data_portal)
            pos.update_price(last_price, last_fx)
            after[pos.asset.exchange_ticker] = pos.pnl

            # add co-operative timeout handling.
            loop_count = loop_count + 1
            if loop_count % chunks == 0 and timeout is not None:
                now = time.time()
                if now - start > timeout:
                    # no rollback done on the postions already updated.
                    break

        if hash(frozenset(before.items())) == \
                            hash(frozenset(after.items())):
            return False

        return True

    cpdef if_update_required(self, object broker):
        """
            flag if any update required for this tracker. It checks
            if there are outstanding open orders and triggers a
            positive response.

            Args:
                ``broker (object)``: The broker object
        """
        if self._current_pos:
            return True

        return False

cdef class AccountTracker(Tracker):
    """
        Account tracker implements updates to account object in sync with
        transactions and positions tracker. For reconcilation of charges
        against trades, it uses the broker API for charges, margins and
        fees. From the position tracker it derives the current exposures
        and profit and losses and together update the internal account
        object. Finaly it does a check with the broker account and produces
        differences if any.

        Note:
            Account reconcilation failing to match broker version is NOT
            unexpected if multiple algorithms are sharing a single broker
            account.

        Args:
            ``name (str)``: Name of the current run.

            ``mode (int)``: Run MODE of the current run.

            ``init_cash (float)``: The inital cash amount allocated to this algorithm

            ``timestamp (Timestamp)``: Current timestamp.

            ``ccy (object)``: Currency in which to track.

    """

    def __init__(self, object name, object mode, object init_cash,
                 object timestamp=None, object ccy=CCY.LOCAL):
        super(AccountTracker, self).__init__(name,mode,timestamp, ccy)
        self._account = None
        self._acct_dir = None
        self.reset(init_cash)
        self.excess_starting_cash = -1
        
        if init_cash is None:
            self._init_cash = init_cash
        else:
            self._init_cash = self._account.starting_cash

    cdef _reset_account(self, object account):
        """
            Create an account object from a dictionary, injecting the
            run name.
        """
        if account is None:
            self._account = BlotterAccount(
                    name=self._name, cash=1, starting_cash=1,
                    currency=self._ccy)
            return

        if isinstance(account, BlotterAccount):
            self._account = account
        elif isinstance(account, dict):
            self._account = BlotterAccount.from_dict(
                    {**account, **{"name":self._name}})
        elif isinstance(account, float) or isinstance(account, int):
            account = <np.float64_t>account
            self._account = BlotterAccount(
                    name=self._name, cash=account, starting_cash=account,
                    currency=self._ccy)
        else:
            self._account = BlotterAccount(
                    name=self._name, cash=0, starting_cash=0,
                    currency=self._ccy)

    def reset(self, object account=None, object last_saved=None,
                object last_reconciled=None,
                bool needs_reconciliation=False,
                bool last_reconciliation_status=False):

        self._reset_account(account)

        self._last_reconciled = last_reconciled
        self._last_saved = last_saved
        self._needs_reconciliation = needs_reconciliation
        self._last_reconciliation_status = last_reconciliation_status

    def roll(self, object timestamp):
        """
            We load the last saved account view and reset the tracked
            quantities.
        """
        # save current data.
        self._save(timestamp)
        # carry forward the status flags and account stats.
        last_saved = self._last_saved
        last_reconciled = self._last_reconciled
        needs_reconciliation = self._needs_reconciliation
        last_reconciliation_status = self._last_reconciliation_status
        self.reset(self._account, last_saved, last_reconciled, needs_reconciliation,
                    last_reconciliation_status)

        # reload last saved.
        self._read(timestamp)

    def _save(self, object timestamp):
        if self._account:
            fname = self._prefix_fn(timestamp)
            try:
                if not self._acct_dir:
                    self._acct_dir = blueshift_saved_performance_path(self._name)
                fname = os_path.join(self._acct_dir, fname)
#                with open(fname, "w") as fp:
#                    json.dump(self._account.to_json(), fp)
                save_blotter_account_to_disk(self._account, fname)
                self._last_saved = timestamp
            except Exception as e:
                if isinstance(e, TerminationError):
                    raise
                msg = f"failed to write account data to {fname}:{str(e)}"
                handling = ExceptionHandling.WARN
                raise DataWriteException(msg=msg, handling=handling)

    def _read(self, object timestamp):
        acct_dict = {}
        
        if not self._acct_dir:
            self._acct_dir = blueshift_saved_performance_path(self._name)
                    
        last_acct_dt = self._last_saved_date(self._acct_dir, timestamp)
        if last_acct_dt:
            fname = os_path.join(
                    self._acct_dir, self._prefix_fn(last_acct_dt))
            if os_path.exists(fname):
                try:
                    with open(fname) as fp:
                        acct_dict = dict(json.load(fp))
                        assert isinstance(acct_dict, dict)
                        if 'version' in acct_dict:
                            acct_dict = read_blotter_account_from_disk(acct_dict)
                except Exception as e:
                    if isinstance(e, TerminationError):
                        raise
                    msg = f"illegal accounts data in {fname}:"
                    msg = msg + str(e)
                    handling = ExceptionHandling.TERMINATE
                    raise DataReadException(msg=msg, handling=handling)

        if acct_dict:
            self._reset_account(acct_dict)

    cdef _get_cash_outflows_for_orders(self, object orders,
                                       float oo_cash,
                                       object broker,
                                       object data_portal,
                                       object positions,
                                       object timestamp):
        """
            Compute the cash and margin for outstanding orders based on
            the order cash outlay function. This is triggered on the
            matched orders list for this round of recon - which are either
            completed or cancelled/ rejected orders. For still open orders,
            we directly pass on the change in outlay (`oo_cash`). This is
            computed in the blotter recon cycle from the txn tracker.

            Args:
                ``orders (dict)``: Matched orders.

                ``oo_cash (float)``: Settlement so far for open orders.

                ``broker (object)``: Broker api object.

                ``data_portal (object)``: Data portal object.
                
                ``positions (dict)``: Existing positions.

            Returns:
                Tuple. A tuple of cash, margin, fees, tax and charges
                to settle.
        """
        cdef np.float64_t margin, fees, tax_and_charges, cash
        cdef np.float64_t f, c
        cdef Order order
        cdef bool incremental

        margin = fees = tax_and_charges = 0
        cash = oo_cash
        f = c = 0
        
        if not orders:
            # we compute only in case of newly reconciled orders
            return cash, margin, fees, tax_and_charges
        
        try:
            incremental, margin = broker.get_trading_margins(
                    [], positions, ccy=self._ccy, timestamp=timestamp,
                    pre_trade=False)
        except Exception:
            incremental, margin = True, 0
        
        if np.isnan(margin):
            incremental = True # do not overwrite existing margins
            margin = 0
            
        if not incremental:
            margin = max(margin, 0) # total argin cannot be negative
            margin = margin - self._account.margin # we need incremental margin
            if margin < 0:
                # never refund more than existing margin
                margin = -min(-margin, self._account.margin)
        
        for order_id in orders:
            order = orders[order_id]
            f, c = broker.get_trading_costs(order)
            f = 0 if np.isnan(f) else f
            c = 0 if np.isnan(f) else c
            fees = fees + f
            tax_and_charges = tax_and_charges + c
            cash = cash + order.cash_outlay(self._ccy, data_portal)
        return cash, margin, fees, tax_and_charges

    cdef _compute_pnls_exposures(self, object positions,
                                np.float64_t round_trips_realized,
                                np.float64_t cash_adj):
        """
            Computes the current pnls and exposures.

            Args:
                ``positions (dict)``: A dict of current positions.

                ``round_trips_realized (float)``: Pnls from completed round trips.

                ``cash_adj (float)``: cash adjustment for funded instruments.

            Returns:
                Tuples. Realized, unrealized profits and losses, and gross
                and net exposures.
        """
        cdef Position pos
        cdef np.float64_t unrealized, net_exposure, gross_exposure, realized
        cdef np.float64_t exp, long_exposure, short_exposure
        cdef np.float64_t holdings, cost_basis, adj
        cdef np.uint64_t longs_count, shorts_count

        realized = round_trips_realized
        adj = cash_adj
        unrealized = net_exposure = gross_exposure = 0
        exp = long_exposure = short_exposure = 0
        holdings = cost_basis = 0
        longs_count = shorts_count = 0

        for asset in positions:
            pos = positions[asset]
            realized = realized + pos.realized_pnl
            unrealized = unrealized + pos.unrealized_pnl

            if asset.is_funded() or asset.is_opt():
                # we calculate cashflows in both case, passing realized
                # pnl will double count the cashflow.
                adj = adj + pos.realized_pnl

            if pos.if_closed():
                continue

            cost_basis = cost_basis + pos.cost_basis
            holdings = holdings + pos.value
            exp = pos.get_exposure()
            net_exposure = net_exposure + exp
            gross_exposure = gross_exposure + abs(exp)

            if pos.quantity > 0:
                long_exposure = long_exposure + \
                                pos.quantity*pos.last_price*pos.last_fx
                longs_count = longs_count + 1
            else:
                short_exposure = short_exposure - \
                                pos.quantity*pos.last_price*pos.last_fx
                shorts_count = shorts_count + 1

        return realized, unrealized, gross_exposure, net_exposure, \
                    long_exposure, short_exposure, longs_count, \
                    shorts_count, holdings, cost_basis, adj

    cpdef reconcile(self, object timestamp, object account, 
                    object positions, object orders, float oo_cash, 
                    object broker, 
                    object data_portal,
                    np.float64_t round_trips_realized,
                    np.float64_t cash_adj):
        """
            Reconciliation of accounts. This reconciliation is not a
            strict one. A broker account can run multiple algos and as
            long as we can match the trades and positions, we should be
            happy about it. The net account value may never match.

            Note:
                The blotter account will only recon on net value.

            Args:
                ``timestamp (Timestamp)``: Timestamp of the call.
                
                ``account (object)``: Account from broker.

                ``positions (dict)``: A dict of positions to compute pnls.

                ``orders (list)``: closed orders to compute impact.

                ``oo_cash (float)``: cash flow due to open orders.

                ``broker (object)``: Broker object to compute margin etc.

                ``data_portal (object)``: A data portal object.

                ``round_trips_realized(float)``: Pnls from completed round trips.

                ``cash_adj (float)``: cash adjustment for funded instruments.

            Returns:
                Float. Difference of the broker reported cash and blotter
                calculated cash.
        """
        cdef np.float64_t cash, margin, fees, tax_and_charges, net_difference
        cdef np.float64_t realized, unrealized, gross_exposure, net_exposure
        cdef np.float64_t holdings, cost_basis, adj, day_pnl
        cdef np.float64_t long_exposure, short_exposure
        cdef np.uint64_t longs_count, shorts_count
        cdef np.float64_t outflow = 0
        cdef np.float64_t transfer_amount = 0

        # adjust cash outflow by the round trip realized to get cost basis
        # recovered in cash/ funded instruments. We add realized pnls
        # during the account updates. Without this adjustment there will
        # be a double counting error for cash instruments.
        # oo_cash = oo_cash - round_trips_realized

        cash, margin, fees, tax_and_charges = self._get_cash_outflows_for_orders(
                orders, oo_cash, broker, data_portal, positions, timestamp)
        
        outflow = cash + margin + fees + tax_and_charges
        if self._account.cash < outflow:
            if self._init_cash is None:
                transfer_amount = max(outflow - self._account.cash, -self._account.cash)
                self._account.fund_transfer(transfer_amount)

        realized, unrealized, gross_exposure, net_exposure, long_exposure,\
                short_exposure, longs_count, shorts_count, \
                holdings, cost_basis, adj \
                    = self._compute_pnls_exposures(
                            positions, round_trips_realized, cash_adj)

        day_pnl = np.nan
        self._account.incremental_update(
                -cash, margin,fees,tax_and_charges,realized,unrealized,
                gross_exposure,net_exposure, holdings, cost_basis,
                long_exposure, short_exposure, longs_count, shorts_count,
                adj, day_pnl)
        net_difference = 0

        return net_difference
    
    def account_difference(self, account):
        """
            Get the difference in total equity from broker account 
            vs. algo. For a virtual account, this is meaningless.
        """
        return account.get("net",0) - self._account.net

    def update_valuation(self, positions, broker):
        """
            Function to update the account valuation when the portfolio
            valuation is updated. There are no trades, so only the
            exposures and holding values needs to be re-computed.

            Args:
                ``positions (dict)``: Current portfolio.

                ``broker (obj)``: Broker API object.
        """
        net_exposure = gross_exposure = holdings = 0
        exp = long_exposure = short_exposure = unrealized = 0

        for asset in positions:
            pos = positions[asset]
            unrealized = unrealized + pos.unrealized_pnl

            if pos.if_closed():
                continue

            holdings = holdings + pos.value
            exp = pos.get_exposure()
            net_exposure = net_exposure + exp
            gross_exposure = gross_exposure + abs(exp)

            if pos.quantity > 0:
                long_exposure = long_exposure + \
                                pos.quantity*pos.last_price*pos.last_fx
            else:
                short_exposure = short_exposure - \
                                pos.quantity*pos.last_price*pos.last_fx

        realized = self._account.realized
        longs_count = self._account.longs_count
        shorts_count = self._account.shorts_count
        cost_basis = self._account.cost_basis

        day_pnl = np.nan
        self._account.incremental_update(
                0, 0, 0, 0, realized, unrealized,
                gross_exposure, net_exposure, holdings, cost_basis,
                long_exposure, short_exposure, longs_count, shorts_count,
                self._account.cash_adj, day_pnl)

    cpdef if_update_required(self, object broker):
        """
            flag if any update required for this tracker. It checks
            if there are any outstanding exposure and triggers a
            positive response.

            Args:
                ``broker (object)``: The broker object
        """
        if self._account.gross_exposure > ALMOST_ZERO:
            return True

        return False

cdef class DailyTransactionsTracker(TransactionsTracker):
    """ 
        Implementation of TransactionTracker methods without any
        broker API calls and any saved data (which also means no 
        restart).
    """
    def _save(self, timestamp):
        pass

    def _read(self, asset_finder, timestamp):
        pass

cdef class DailyPositionTracker(PositionTracker):
    """
        MicroPositionTracker implements the PositionTracker methods, 
        but does not initiate any broker API calls, relying on 
        complete virtualization instead. Also, read and save 
        methods are no-ops, so no support for re-start either.

    """

    def _read(self, object asset_finder, object timestamp):
        pass

    def _save(self, object timestamp):
        pass

cdef class DailyAccountTracker(AccountTracker):
    """
        MicroAccountTracker implements the AccountTracker interface
        but does not depend on the underlying broker for any API 
        calls or data fetch (except for margins and charges 
        where implemented and required). In addition, it does not 
        have any save or read method and the roll method is a 
        no-op.

    """
    def _save(self, object timestamp):
        pass

    def _read(self, object timestamp):
        pass