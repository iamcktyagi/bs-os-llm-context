# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport numpy as np
import numpy as np

from blueshift.lib.trades._order cimport Order
from blueshift.lib.trades._position cimport Position
from blueshift.interfaces.assets._assets cimport InstrumentType
from ._tracker cimport TransactionsTracker, PositionTracker, AccountTracker

from blueshift.lib.common.constants import CCY
from blueshift.lib.exceptions import InitializationError, ExceptionHandling

cdef class XTransactionsTracker(TransactionsTracker):
    """
        This is a file-based blotter to map an exclusive account. The
        only method over-ride is updating positions from orders. In this
        case, we just take a the account positions from the broker and
        update the round trips (closed positions) and open trades (open
        positions).

        Note: see :class:`blueshift.blotter.trackers._tracker.TransactionsTracker`
    """

    def sanitize(self, broker, restart=False):
        pass

    cpdef update_positions_from_orders(self, object orders, object broker,
                                       object data_portal):
        """
            A function to calculate round trips and open positions for
            an excluesive account. We assume all positions are out of
            the current algo orders. The closed positions from the broker
            are a list maintained by the broker object. We loop through
            it, add them to round trips list of the object and clear
            the broker list. In next round, the fresh closed positions
            are treated the same way. The broker object makes sure a
            closed position is entered only once in its list (by
            maintaining a list of positions processed), and at every
            call, this tracker clears that list, transferring the
            positions to its round trip list.

        """
        cdef Position pos
        self._open_trades = broker.positions
        broker.closed_positions.reverse()

        for pos in reversed(broker.closed_positions):
            self._round_trips.append(pos.to_json())
            broker.closed_positions.pop()
            self._round_trips_realized = pos.realized_pnl +\
                                            self._round_trips_realized
            if pos.asset.is_funded() or pos.asset.is_opt():
                    self._cash_adj = self._cash_adj + pos.realized_pnl

        return {}

    cpdef if_update_required(self, object broker):
        """
            flag if any update required for this tracker. It checks
            if there are any closed positions from the broker and
            trigger a positive response accordingly.

            Args:
                ``broker (object)``: The broker object
        """
        if broker.closed_positions:
            return True

        parent_cond = \
            super(XTransactionsTracker, self).if_update_required(broker)

        return parent_cond


cdef class XPositionTracker(PositionTracker):
    """
        A position tracker that implements an exclusive account.

        Note: see :class:`blueshift.blotter.trackers._tracker.PositionTracker`

    """

    def sanitize(self, broker, restart=False):
        """ """
        open_pos = broker.positions
        # raise error if existing open position
        if not restart and open_pos:
            msg = "Existing open positions, "
            msg = msg + "cannot compute algo pnl reliably."
            handling = ExceptionHandling.TERMINATE
            raise InitializationError(msg=msg, handling=handling)
        # now clear off the existing close positions to avoid pnl mangling
        for pos in reversed(broker.closed_positions):
            broker.closed_positions.pop()

    cpdef update_current_pos(self, object open_trades,
                             object open_order_pos):
        """
            The open_trades are the existing open positions. Open order
            position is a dummy, as we get the entire open positions
            directly from the broker (via the transaction tracker).
        """
        self._current_pos = open_trades

    cpdef reconcile(self, object timestamp, object positions,
                    object data_portal):
        """
            This reconciliation always succeeds as we assume the postions
            are only out of the current algo.
        """
        # exclusive tracker, we copy the broker positions
        self._current_pos = positions.copy()

        self._needs_reconciliation = False
        self._last_reconciled = timestamp
        self._last_reconciliation_status = True

        # this reconciliation always succeeds!
        return True, {}

    def update_valuation(self, data_portal, timeout = None,
                         broker=None):
        """
            Update valuation for positions
        """
        # exclusive tracker, we copy the broker positions
        self._current_pos = broker.positions.copy()

        if broker.streaming_update:
            return True

        return super(XPositionTracker, self).update_valuation(
                data_portal, timeout, broker)


cdef class XAccountTracker(AccountTracker):
    """
        Account tracker for an exclusive account. All margins and
        commissions and charges are assumed to arise from this algo
        run.

        Note: see :class:`blueshift.blotter.trackers._tracker.AccountTracker`
    """
    def sanitize(self, broker, restart=False):
        existing_cash = broker.account["cash"]
        starting_cash = self._account.starting_cash
        self.excess_starting_cash = existing_cash - starting_cash

        if not restart and self.excess_starting_cash < 0:
            msg = f"Existing cash {existing_cash} cannot be "
            msg = msg + f"less than starting cash {starting_cash}."
            handling = ExceptionHandling.TERMINATE
            raise InitializationError(msg=msg, handling=handling)

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
        cdef np.float64_t margin, fees, tax_and_charges, cash, c
        cdef Order order

        margin = fees = tax_and_charges = cash = c = 0

        account = broker.account
        # incremental changes in this round of reconciliation.
        cash = (account["cash"]-self.excess_starting_cash) - self._account.cash
        margin = account["margin"] - self._account.margin
        fees = account["commissions"] - self._account.commissions
        tax_and_charges = account["charges"] - self._account.charges

        # since we will deduct all these in account incremental update,
        # we add them here to arrive at the correct cash as reported
        # by the broker account.
        cash = cash + margin + fees + tax_and_charges

        return cash, margin, fees, tax_and_charges

    cpdef reconcile(self, object timestamp, object account,
                    object positions, object orders, float oo_cash,
                    object broker,
                    object data_portal,
                    np.float64_t round_trips_realized,
                    np.float64_t cash_adj):
        """
            Reconciliation of accounts. This reconciliation maps the
            underlying account report to blotter.

            Note: see :class:`blueshift.blotter.trackers._tracker.AccountTracker`
        """
        cdef np.float64_t cash, margin, fees, tax_and_charges, net_difference
        cdef np.float64_t realized, unrealized, gross_exposure, net_exposure
        cdef np.float64_t holdings, cost_basis, adj, day_pnl
        cdef np.float64_t long_exposure, short_exposure
        cdef np.uint64_t longs_count, shorts_count

        # adjust cash outflow by the round trip realized to get cost basis
        # recovered in cash/ funded instruments. We add realized pnls
        # during the account updates. Without this adjustment there will
        # be a double counting error for cash instruments.
        # oo_cash = oo_cash - round_trips_realized

        cash, margin, fees, tax_and_charges = \
            self._get_cash_outflows_for_orders(
                    None, 0, broker, None, positions, timestamp)

        realized, unrealized, gross_exposure, net_exposure, long_exposure,\
                short_exposure, longs_count, shorts_count, \
                holdings, cost_basis, adj \
                    = self._compute_pnls_exposures(
                            positions, round_trips_realized, cash_adj)

        # adjust cash, it already includes the imapct of incremental
        # realized pnls and incremental cash adjustment.
        cash = cash - (realized - self._account.realized)
        cash = cash + (adj - self._account.cash_adj)

        day_pnl = np.nan
        acct = broker.account
        if "day_pnl" in acct:
            day_pnl = acct["day_pnl"]

        self._account.incremental_update(
                cash, margin,fees,tax_and_charges,realized,unrealized,
                gross_exposure,net_exposure, holdings, cost_basis,
                long_exposure, short_exposure, longs_count, shorts_count,
                adj, day_pnl)

        net_difference = account.get("net",0) - self._account.net \
                            - self.excess_starting_cash

        return net_difference
