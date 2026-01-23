# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
from cpython cimport bool
cimport numpy as np
import numpy as np

from blueshift.lib.trades._order cimport Order
from blueshift.interfaces.trading._tracker cimport Tracker
from blueshift.lib.trades._accounts cimport BlotterAccount

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

            ``asset_finder (object)``: An asset finder object.

        Returns:
            None.
        """

    cdef readonly int MAX_TRANSACTIONS
    cdef readonly object ROUND_TRIPS_FILE
    cdef readonly object OPEN_TRADES_FILE
    cdef readonly object UNPROCESSED_FILE
    cdef readonly object _orders_dir
    cdef readonly object _txn_dir

    cdef readonly dict _open_orders
    cdef readonly dict _unprocessed_orders
    cdef readonly dict _open_trades

    cdef readonly object _known_orders
    cdef readonly object _transactions
    cdef readonly object _round_trips
    cdef readonly object _missing_orders
    cdef readonly np.float64_t _round_trips_realized
    cdef readonly np.float64_t _cash_adj

    cpdef add_transaction(self, object order_id, Order order)
    cdef _update_transactions(self, Order order, object timestamp)
    cpdef reconcile(self, object timestamp, object orders,
                    object synthetic_orders)
    cpdef update_positions_from_orders(
            self, object orders, object broker, object data_portal)
    cpdef flatten_transactions_dict(self)
    cpdef open_order_cash_outlay(self, data_portal)

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

        Returns:
            None.

    """
    cdef readonly object _pos_dir
    cdef readonly dict _current_pos
    cdef readonly dict _init_pos

    cpdef update_current_pos(
            self, object open_trades, object open_order_pos)
    cpdef reconcile(
            self, object timestamp, object positions, object data_portal)

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

    """
    cdef readonly np.float64_t excess_starting_cash
    cdef readonly object _acct_dir
    cdef readonly BlotterAccount _account
    cdef readonly object _init_cash

    cdef _reset_account(self, object account)
    cdef _get_cash_outflows_for_orders(
            self, object orders, float oo_cash, object broker,
            object data_portal, object positions, object timestamp)
    cdef _compute_pnls_exposures(cls, object positions,
                                 np.float64_t round_trips_realized,
                                 np.float64_t cash_adj)
    cpdef reconcile(self, object timestamp, object account,
                    object positions, object orders, float oo_cash,
                    object broker,
                    object data_portal,
                    np.float64_t round_trips_realized,
                    np.float64_t cash_adj)
