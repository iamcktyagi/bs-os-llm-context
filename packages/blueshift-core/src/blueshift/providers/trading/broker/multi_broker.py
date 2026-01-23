# Copyright 2018-2025 Blueshift Technologies Pte. Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations
from typing import TYPE_CHECKING, Callable, Any
from copy import copy
import time

from blueshift.interfaces.trading.broker import ILiveBroker, AccountType
from blueshift.lib.exceptions import (
    InitializationError, 
    BrokerError,
    InsufficientFund,
    PriceOutOfRange,
    PreTradeCheckError,
)
from blueshift.lib.common.types import PartialOrder
from blueshift.lib.common.decorators import with_lock
from blueshift.lib.common.ctx_mgrs import TimeoutRLock
from blueshift.lib.trades._accounts import BlotterAccount, Account
from blueshift.config import LOCK_TIMEOUT

if TYPE_CHECKING:
    import pandas as pd
    from blueshift.lib.trades._position import Position
    from blueshift.lib.trades._order import Order
    from blueshift.calendar.trading_calendar import TradingCalendar
    from blueshift.interfaces.data.store import DataStore
    from blueshift.interfaces.assets._assets import Asset


class MultiBroker(ILiveBroker):
    """
    A broker implementation that routes orders to multiple underlying brokers.
    """

    def __init__(self, brokers: dict[str, ILiveBroker], router: Callable[[Order, dict[str, ILiveBroker]], str] | None = None, *args, **kwargs):
        """
        Initialize the MultiBroker.

        Args:
            brokers: A dictionary of name -> ILiveBroker instances.
            router: A callable that takes an Order and the brokers dict, and returns the name of the broker to use.
                    If None, defaults to the first broker.
            *args: Additional arguments for the base class.
            **kwargs: Additional keyword arguments for the base class.
        """
        if not brokers:
            raise InitializationError("MultiBroker requires at least one underlying broker.")
        
        self._brokers = brokers
        self._router = router or self._default_router
        self._primary_broker_name = next(iter(brokers))
        
        # Use the primary broker's name as the base name, but can be overridden
        name = kwargs.get('name', 'multi_broker')
        super().__init__(name=name, *args, **kwargs)
        
        self._lock = TimeoutRLock(timeout=LOCK_TIMEOUT)
        self._latencies = {name: 0.0 for name in brokers}
        self._latency_alpha = 0.2

    @property
    def brokers(self) -> dict[str, ILiveBroker]:
        return self._brokers

    def _update_latency(self, name: str, duration: float):
        current = self._latencies.get(name, 0.0)
        if current == 0.0:
            self._latencies[name] = duration
        else:
            self._latencies[name] = self._latency_alpha * duration + (1 - self._latency_alpha) * current

    def _get_supporting_brokers(self, asset: Asset) -> list[str]:
        candidates = []
        sym = asset.symbol
        for name, broker in self._brokers.items():
            if not broker.is_connected:
                continue
            try:
                # check if the broker supports the symbol
                if broker.exists(sym):
                    candidates.append(name)
            except Exception:
                pass
        return candidates

    def _default_router(self, order: Order, brokers: dict[str, ILiveBroker]) -> str:
        """Default router: returns the broker with least latency that supports the asset."""
        candidates = self._get_supporting_brokers(order.asset)
        
        if not candidates:
            # Fallback to all connected if none specific found (though unlikely to work)
            candidates = [name for name, b in brokers.items() if b.is_connected]
            if not candidates:
                # Absolute fallback
                return self._primary_broker_name
            
        return min(candidates, key=lambda n: self._latencies.get(n, float('inf')))
    
    def _get_broker(self, name: str) -> ILiveBroker:
        if name not in self._brokers:
            raise BrokerError(f"Broker {name} not found in MultiBroker.")
        return self._brokers[name]

    def _make_composite_id(self, broker_name: str, broker_oid: str) -> str:
        return f"{broker_name}:{broker_oid}"

    def _parse_composite_id(self, composite_oid: str) -> tuple[str, str]:
        parts = composite_oid.split(':', 1)
        if len(parts) != 2:
            # Fallback if not a composite ID (might be from before or direct)
            # We can try to find which broker has this order.
            for name, broker in self._brokers.items():
                if composite_oid in broker.orders:
                    return name, composite_oid
            raise BrokerError(f"Invalid order ID format: {composite_oid}")
        return parts[0], parts[1]

    @property
    def is_connected(self) -> bool:
        """Returns True if the primary broker is connected."""
        return self._brokers[self._primary_broker_name].is_connected

    @property
    def account_type(self) -> AccountType:
        return self._brokers[self._primary_broker_name].account_type

    @property
    def calendar(self) -> TradingCalendar:
        return self._brokers[self._primary_broker_name].calendar

    @property
    def store(self) -> DataStore:
        return self._brokers[self._primary_broker_name].store

    @property
    def tz(self) -> str:
        return self._brokers[self._primary_broker_name].tz

    @property
    def profile(self) -> dict:
        return self._brokers[self._primary_broker_name].profile

    @property
    def algo_user(self) -> str | None:
        return self._algo_user

    @algo_user.setter
    def algo_user(self, value: str):
        self._algo_user = value
        for broker in self._brokers.values():
            broker.algo_user = value

    @property
    def account(self) -> dict:
        """Aggregate account details from all brokers."""
        try:
            return self.get_account().to_dict()
        except BrokerError:
            return {}

    def get_account(self, *args, **kwargs) -> Account:
        """Returns the aggregated Account object."""
        # Initialize totals
        cash = 0.0
        margin = 0.0
        gross = 0.0
        net = 0.0
        holdings = 0.0
        cost_basis = 0.0
        mtm = 0.0
        commissions = 0.0
        charges = 0.0
        
        found = False
        for name, broker in self._brokers.items():
            try:
                t0 = time.time()
                acct = broker.get_account(*args, **kwargs)
                self._update_latency(name, time.time() - t0)
                
                found = True
                cash += acct.cash
                margin += acct.margin
                gross += acct.gross_exposure
                net += acct.net_exposure
                holdings += acct.holdings
                cost_basis += acct.cost_basis
                mtm += acct.mtm
                commissions += acct.commissions
                charges += acct.charges
            except (NotImplementedError, BrokerError):
                continue
                
        if not found:
             raise BrokerError("No accounts available to aggregate.")
             
        # Create aggregated account
        return Account(
            name=self.name,
            cash=cash,
            margin=margin,
            gross_exposure=gross,
            net_exposure=net,
            holdings=holdings,
            cost_basis=cost_basis,
            mtm=mtm,
            commissions=commissions,
            charges=charges,
        )
    
    # ... (skipping other methods if not changing) ...

    # I need to target place_order separately or include it in the replace block if I can match context.
    # Since they are far apart, I'll do get_account first then place_order in another call or just target get_account here.
    # The prompt says "Update get_account... Also update place_order". I should try to do both if I can match unique blocks.
    # But replacing multiple disconnected blocks is hard with one call unless I match the whole file or use multiple replace calls.
    # I will do get_account first.


    @property
    def positions(self) -> dict[Asset, Position]:
        """Aggregate positions from all brokers."""
        aggregated_positions = {}
        
        for broker in self._brokers.values():
            positions = broker.positions
            for asset, pos in positions.items():
                if asset in aggregated_positions:
                    aggregated_positions[asset].add_to_position(pos)
                else:
                    # Create a copy to avoid modifying the broker's position object
                    # Assuming Position.copy() exists (it does in .pyi)
                    aggregated_positions[asset] = pos.copy()
                    
        return aggregated_positions

    @property
    def orders(self) -> dict[str, Order]:
        """Fetch and aggregate orders from all brokers."""
        all_orders = {}
        for b_name, broker in self._brokers.items():
            orders = broker.orders
            for oid, order in orders.items():
                composite_id = self._make_composite_id(b_name, oid)
                # We return a copy of the order with the modified ID to the user
                # This ensures the user uses the composite ID for subsequent calls
                order_copy = copy(order) # Shallow copy might be enough if we only change ID
                # But to be safe, use from_dict/to_dict or assume copy works
                # Order doesn't have a robust deepcopy in .pyi, but let's try shallow + id set
                order_copy.set_order_id(composite_id)
                all_orders[composite_id] = order_copy
        return all_orders

    @property
    def open_orders(self) -> dict[str, Order]:
        all_orders = {}
        for b_name, broker in self._brokers.items():
            orders = broker.open_orders
            for oid, order in orders.items():
                composite_id = self._make_composite_id(b_name, oid)
                order_copy = copy(order)
                order_copy.set_order_id(composite_id)
                all_orders[composite_id] = order_copy
        return all_orders

    def get_order(self, order_id: str, *args, **kwargs) -> Order | None:
        try:
            b_name, b_oid = self._parse_composite_id(order_id)
            broker = self._brokers[b_name]
            order = broker.get_order(b_oid, *args, **kwargs)
            if order:
                order_copy = copy(order)
                order_copy.set_order_id(order_id)
                return order_copy
        except (BrokerError, KeyError):
            return None
        return None

    def open_orders_by_asset(self, asset: Asset, *args, **kwargs) -> list[Order]:
        all_orders = []
        for b_name, broker in self._brokers.items():
            orders = broker.open_orders_by_asset(asset, *args, **kwargs)
            for order in orders:
                composite_id = self._make_composite_id(b_name, order.oid)
                order_copy = copy(order)
                order_copy.set_order_id(composite_id)
                all_orders.append(order_copy)
        return all_orders

    def _place_order_on_broker(self, name: str, order: Order, *args, **kwargs) -> str | list[PartialOrder | str] | None:
        broker = self._brokers[name]
        t0 = time.time()
        result = broker.place_order(order, *args, **kwargs)
        self._update_latency(name, time.time() - t0)
        
        if isinstance(result, str):
            # Single order ID
            return self._make_composite_id(name, result)
        elif isinstance(result, list):
            # List of PartialOrders or strings?
            # Interface says str | list[PartialOrder | str] | None
            # We need to wrap them
            wrapped_results = []
            for item in result:
                if isinstance(item, str):
                    wrapped_results.append(self._make_composite_id(name, item))
                elif isinstance(item, PartialOrder): # If PartialOrder is a type
                    # PartialOrder might have an id field?
                    # PartialOrder is a NamedTuple or class? 
                    # blueshift.lib.common.types.PartialOrder
                    new_oid = self._make_composite_id(name, item.oid)
                    wrapped_results.append(PartialOrder(new_oid, item.quantity))
            return wrapped_results # Simplification
        return result

    def place_order(self, order: Order, *args, **kwargs) -> str | list[PartialOrder | str] | None:
        # Route the order
        target_broker_name = self._router(order, self._brokers)
        
        try:
            return self._place_order_on_broker(target_broker_name, order, *args, **kwargs)
        except (InsufficientFund, PriceOutOfRange, PreTradeCheckError) as e:
            # Retry logic
            candidates = self._get_supporting_brokers(order.asset)
            # Sort by latency
            candidates.sort(key=lambda n: self._latencies.get(n, float('inf')))
            
            for name in candidates:
                if name == target_broker_name:
                    continue # Already tried
                try:
                    return self._place_order_on_broker(name, order, *args, **kwargs)
                except (InsufficientFund, PriceOutOfRange, PreTradeCheckError):
                    continue
            
            # If all failed, raise the original/last exception
            raise e

    def update_order(self, order: Order, *args, **kwargs) -> str:
        # The order object passed here should have the composite ID if it came from our getter
        b_name, b_oid = self._parse_composite_id(order.oid)
        broker = self._get_broker(b_name)
        
        # Clone the order to avoid modifying the user's object
        order_copy = copy(order)
        order_copy.set_order_id(b_oid)
        
        result = broker.update_order(order_copy, *args, **kwargs)
        # Result is likely the new ID (often same)
        return self._make_composite_id(b_name, result)

    def cancel_order(self, order_param: Order | str, *args, **kwargs) -> str:
        if isinstance(order_param, str):
            oid = order_param
            b_name, b_oid = self._parse_composite_id(oid)
            broker = self._get_broker(b_name)
            result = broker.cancel_order(b_oid, *args, **kwargs)
            return self._make_composite_id(b_name, result)
        else:
            # It's an order object
            order = order_param
            b_name, b_oid = self._parse_composite_id(order.oid)
            broker = self._get_broker(b_name)
            
            # Clone the order
            order_copy = copy(order)
            order_copy.set_order_id(b_oid)
            
            result = broker.cancel_order(order_copy, *args, **kwargs)
            return self._make_composite_id(b_name, result)
            
    def add_algo(self, algo, algo_user, *args, **kwargs):
        super().add_algo(algo, algo_user, *args, **kwargs)
        for broker in self._brokers.values():
            broker.add_algo(algo, algo_user, *args, **kwargs)
            
    def remove_algo(self, algo, *args, **kwargs):
        super().remove_algo(algo, *args, **kwargs)
        for broker in self._brokers.values():
            broker.remove_algo(algo, *args, **kwargs)

    def position_by_asset(self, asset: Asset, *args, **kwargs) -> Position | None:
        """Aggregate position for a specific asset."""
        aggregated_pos = None
        
        for broker in self._brokers.values():
            pos = broker.position_by_asset(asset, *args, **kwargs)
            if pos:
                if aggregated_pos is None:
                    aggregated_pos = pos.copy()
                else:
                    aggregated_pos.add_to_position(pos)
                    
        return aggregated_pos

    def login(self, *args, **kwargs) -> None:
        for broker in self._brokers.values():
            broker.login(*args, **kwargs)

    def logout(self, *args, **kwargs) -> None:
        for broker in self._brokers.values():
            broker.logout(*args, **kwargs)
            
    # --- Lifecycle and other methods ---
    
    def before_trading_start(self, timestamp):
        for broker in self._brokers.values():
            broker.before_trading_start(timestamp)

    def after_trading_hours(self, timestamp):
        for broker in self._brokers.values():
            broker.after_trading_hours(timestamp)
            
    def algo_start(self, timestamp):
        for broker in self._brokers.values():
            broker.algo_start(timestamp)

    def algo_end(self, timestamp):
        for broker in self._brokers.values():
            broker.algo_end(timestamp)

    def heart_beat(self, timestamp):
        for broker in self._brokers.values():
            broker.heart_beat(timestamp)
            
    def refresh_data(self, **kwargs):
        # DataPortal method
        for broker in self._brokers.values():
            broker.refresh_data(**kwargs)

    # --- AssetFinder / DataPortal methods (Delegated to primary) ---
    # Since MultiBroker inherits IBroker which inherits IAssetFinder and DataPortal
    # we should probably delegate these lookups to one of the brokers (primary)
    # assuming they share the same universe definition.
    
    def fetch_asset(self, sid):
        return self._brokers[self._primary_broker_name].fetch_asset(sid)

    def symbol(self, sym: str, dt=None, *args, **kwargs):
        return self._brokers[self._primary_broker_name].symbol(sym, dt, *args, **kwargs)

    def exists(self, syms):
        return self._brokers[self._primary_broker_name].exists(syms)

    def lifetimes(self, dates, assets):
        return self._brokers[self._primary_broker_name].lifetimes(dates, assets)

    def can_trade(self, asset: Asset, dt):
        return self._brokers[self._primary_broker_name].can_trade(asset, dt)

    def has_streaming(self) -> bool:
         return self._brokers[self._primary_broker_name].has_streaming()

    def getfx(self, ccy_pair: str | Any, dt) -> float:
        return self._brokers[self._primary_broker_name].getfx(ccy_pair, dt)
        
    def update_market_quotes(self, asset:Asset, data: dict):
         self._brokers[self._primary_broker_name].update_market_quotes(asset, data)
    
    def current(self, assets, columns, **kwargs):
        return self._brokers[self._primary_broker_name].current(assets, columns, **kwargs)
        
    def history(self, assets, columns, nbars, frequency, **kwargs):
        return self._brokers[self._primary_broker_name].history(assets, columns, nbars, frequency, **kwargs)

    def pre_trade_details(self, asset: Asset, **kwargs):
        return self._brokers[self._primary_broker_name].pre_trade_details(asset, **kwargs)

    def quote(self, asset: Asset, column=None, **kwargs):
        return self._brokers[self._primary_broker_name].quote(asset, column, **kwargs)
    
    def reset(self, *args, **kwargs):
        for broker in self._brokers.values():
            broker.reset(*args, **kwargs)
    
    def subscribe(self, assets, level=None, *args, **kwargs):
        # Delegate subscription to the primary broker as it is the source of data
        self._brokers[self._primary_broker_name].subscribe(assets, level, *args, **kwargs)
    
    def unsubscribe(self, assets, level=None, *args, **kwargs):
        self._brokers[self._primary_broker_name].unsubscribe(assets, level, *args, **kwargs)

    def get_expiries(self, asset, start_dt, end_dt, offset=None):
        return self._brokers[self._primary_broker_name].get_expiries(asset, start_dt, end_dt, offset)
    
    def option_chain(self, underlying, series, columns, strikes=None, relative=True, **kwargs):
        return self._brokers[self._primary_broker_name].option_chain(underlying, series, columns, strikes, relative, **kwargs)
    
    def fundamentals(self, assets, metrics, nbars, frequency, **kwargs):
        return self._brokers[self._primary_broker_name].fundamentals(assets, metrics, nbars, frequency, **kwargs)



