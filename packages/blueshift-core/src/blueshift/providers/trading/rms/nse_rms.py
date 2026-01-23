from __future__ import annotations
from typing import TYPE_CHECKING, Callable
import math

from blueshift.interfaces.trading.rms import IRMS, register_rms

from blueshift.lib.trades._order_types import OrderType, OrderSide
from blueshift.lib.exceptions import PreTradeCheckError
from blueshift.interfaces.assets._assets import EquityOption, EquityFutures
from blueshift.lib.common.ctx_mgrs import TimeoutRLock
from blueshift.config import RMS_LIMIT_THRESHOLD

if TYPE_CHECKING:
    from blueshift.interfaces.trading.broker import IBroker
    from blueshift.lib.trades._order import Order
    from blueshift.lib.trades._position import Position

class NSERMS(IRMS):
    """
        NSE mandated RMS checks.
    """
    def __init__(self, broker:IBroker, ltp_threshold:float=50, ltp_pct:float=0.03, ltp_abs:float=1.5,
                 ltp_pct_fut:float=0.03, ltp_abs_fut:float=1.5, ltp_pct_opt:float=0.4,
                 ltp_abs_opt:float=20, limit_threshold:float=RMS_LIMIT_THRESHOLD, 
                 limit_threshold_floor:float=0.1, limit_scaling_factor:float=0.8):
        super().__init__(broker)
        self._price_precision = 2
        self._orders_tracker:dict[str,int] = {}
        self._pre_trade_func:Callable = broker.pre_trade_details
        self._logger = broker.logger
        self._ltp_threshold = ltp_threshold                 # set by NSE, subject to change
        self._ltp_pct = ltp_pct                             # set by NSE, subject to change
        self._ltp_abs = ltp_abs                             # set by NSE, subject to change
        self._ltp_pct_fut = ltp_pct_fut                     # set by NSE, subject to change
        self._ltp_abs_fut = ltp_abs_fut                     # set by NSE, subject to change
        self._ltp_pct_opt = ltp_pct_opt                     # set by NSE, subject to change
        self._ltp_abs_opt = ltp_abs_opt                     # set by NSE, subject to change
        self._limit_threshold = limit_threshold             # set by us, conservative approac
        self._limit_threshold_floor = limit_threshold_floor # set by us for retries
        self._limit_scaling_factor = limit_scaling_factor   # set by us for retries
        self._lock = TimeoutRLock(timeout=5)
        
        # show custom warning message
        pct = round(self._limit_threshold*100)
        msg = f'All market orders will automatically be converted to limit '
        msg += f'orders at {pct}% of the prescribed market price protection limit.'
        self._logger.warning(msg)
    
    @property
    def name(self) -> str:
        return "IND-RMS"
    
    def pretrade(self, order:Order, *args, **kwargs):
        """
            Enforce NSE market price protection by converting incoming 
            market order to limit order. See links below for more detais:
            https://archives.nseindia.com/content/circulars/CMTR54851.pdf
            https://archives.nseindia.com/content/circulars/CMTR55069.pdf
        """
        if order.order_type not in (OrderType.LIMIT, OrderType.MARKET):
            raise PreTradeCheckError(f'Only market or limit orders are supported.')
        
        logger = kwargs.pop('logger', self._logger)
        ticksize = order.asset.get_tick_size()
        ticksize = max(0.01, ticksize)
        
        if order.order_type == OrderType.LIMIT:
            try:
                original_price = order.price
                check = original_price/ticksize
                if check != int(check):
                    limit_price = order.price
                    
                    if order.side == OrderSide.BUY:
                        limit_price = round(math.floor(
                            limit_price/ticksize)*ticksize, self._price_precision)
                    else:
                        limit_price = round(math.ceil(
                            limit_price/ticksize)*ticksize, self._price_precision)
                        
                    order.convert_to_limit(limit_price)
                    msg = f'RMS converted user limit order {order.oid} placed at '
                    msg += f'{original_price} to limit price {limit_price} to '
                    msg += f'match with tick size {ticksize}.'
                    logger.info(msg)
            except Exception as e:
                msg = f'failed to check limit price is multiple of '
                msg += f'ticksize, will send original order:{str(e)}'
                logger.error(msg)
            
            return order
        
        count = 0
        if order.reference_id:
            try:
                with self._lock:
                    count = self._orders_tracker.get(order.reference_id, 0)
            except Exception:
                pass
        
        limit_threshold = self._limit_threshold*(self._limit_scaling_factor)**count
        limit_threshold = max(limit_threshold, self._limit_threshold_floor)
            
        try:
            ltp, upper, lower = self._pre_trade_func(order.asset, logger=logger)
            logger.info(f'computing pretrade for {order.asset} -> {ltp}, {upper}, {lower} {count}')
            
            if ltp is None or math.isnan(ltp) or ltp==0:
                ltp = float(order.price_at_entry)
            
            # assert ltp is not None, 'last traded price must be valid, got {ltp}.'
            # assert not math.isnan(ltp), 'last traded price must be valid, got {ltp}.'
            # assert ltp >= ticksize, 'last traded price is missing or below ticksize.'
            if ltp is None:
                raise ValueError(f'last traded price must be valid, got {ltp}.')
            if math.isnan(ltp):
                raise ValueError(f'last traded price must be valid, got {ltp}.')
            if ltp < ticksize:
                raise ValueError(f'last traded price {ltp} is below ticksize {ticksize}.')
        except Exception as e:
            raise PreTradeCheckError(f'pre-trade check failed:{str(e)}.')
        else:
            if isinstance(order.asset, EquityOption):
                pct_limit = ltp*self._ltp_pct_opt
                abs_limit = self._ltp_abs_opt
            elif isinstance(order.asset, EquityFutures):
                pct_limit = ltp*self._ltp_pct_fut
                abs_limit = self._ltp_abs_fut
            else:
                pct_limit = ltp*self._ltp_pct
                abs_limit = self._ltp_abs
                
            limit = max(abs(pct_limit), abs(abs_limit))
            limit = limit*limit_threshold
            
            if order.side == OrderSide.BUY:
                if upper is not None and upper > ltp:
                    dpr = (upper-ltp)*limit_threshold
                    limit = min(limit, dpr)
                    
                limit_price = ltp + limit
                limit_price = round(math.floor(
                        limit_price/ticksize)*ticksize, self._price_precision)
            else:
                if lower is not None and lower < ltp:
                    dpr = (ltp - lower)*limit_threshold
                    limit = min(limit, dpr)
                    
                limit_price = max(ticksize, ltp - limit)
                limit_price = round(math.ceil(
                        limit_price/ticksize)*ticksize, self._price_precision)
            
            try:
                #assert limit_price >= ticksize, 'computed limit price is below ticksize.'
                if limit_price < ticksize:
                    raise ValueError(f'computed limit price {limit_price} is below ticksize {ticksize}.')
            except Exception as e:
                raise PreTradeCheckError(f'pre-trade check failed:{str(e)}.')
                
            try:
                with self._lock:
                    if order.reference_id not in self._orders_tracker:
                        self._orders_tracker[order.reference_id] = 1
                    else:
                        self._orders_tracker[order.reference_id] += 1
            except Exception:
                pass
            
            order.convert_to_limit(limit_price)
            msg = f'RMS converted user market order {order.oid} to limit '
            msg += f'order placed at {limit_price}.'
            logger.info(msg)
            return order
    
    def posttrade(self, order:Order, *args, **kwargs):
        try:
            with self._lock:
                self._orders_tracker.pop(order.reference_id, None)
        except Exception:
            pass
    
    def monitor(self, position):
        pass
    
for exchange in ('NSE','BSE', 'NFO', 'BFO','IND'):
    register_rms(exchange, NSERMS)