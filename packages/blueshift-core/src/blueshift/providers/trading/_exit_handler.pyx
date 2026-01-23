# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool
cimport numpy as np
import numpy as np

from blueshift.interfaces.assets._assets cimport Asset
from blueshift.lib.trades._position cimport Position
from blueshift.lib.trades._accounts cimport BlotterAccount
from blueshift.lib.trades._order_types cimport PositionSide
from blueshift.interfaces.trading._exit_handler cimport IExitHandler, ExitMethod

from blueshift.lib.common.enums import AlgoMode
from blueshift.lib.common.functions import listlike
from blueshift.lib.common.sentinels import noop
from blueshift.lib.exceptions import NoContextError

cdef class ExitDef:
    """
        An exit definition (stoploss or take profit)
    """
    def __init__(self, context, type_, asset, method, target, 
                 trailing, entry,callback,do_exit,rolling,
                 trailing_fraction):
        self.context = context
        self.type = type_
        self.asset = asset
        self.method = method
        self.target = target
        self.trailing = trailing
        self.entry = entry
        self.callback = callback
        self.do_exit = do_exit
        self.rolling = rolling
        self.trailing_fraction = trailing_fraction


cdef class ExitHandler(IExitHandler):
    """
        A handler that implements stoploss and take profit. It also 
        tracks square-off exits and enforces order cool-off period 
        following any exit orders (stoploss, takeprofit or user trigger
        square-offs).
    """
    def __init__(self, algo, cooloff_period=30, **kwargs):
        self._algo = algo
        self._handlers = {}
        self._ctx_handlers = {}
        self._cooloff_period = cooloff_period
        # these are context sensitive
        self._entry_cooloff = {}
        # these apply globally, for all contexts
        # controlled by user to cool off the algo for a while
        self._ctx_entry_cooloff = {}
        # triggers automatically on squareoff of all if algo=True
        self._global_squareoff_cooloff = False
        # triggers automatically on squareoff of all in context
        self._ctx_squareoff_cooloff = {}
        # triggers automatically on squareoff of an asset
        self._squareoff_cooloff = {}
        
    cpdef set_cooloff_period(self, np.float64_t value):
        self._cooloff_period = value
        
    def get_cooloff_period(self):
        return self._cooloff_period
        
    cpdef reset(self, bool roll=True):
        handlers = self._handlers
        self._handlers = {}
        ctx_handlers = self._ctx_handlers
        self._ctx_handlers = {}
        self._entry_cooloff = {}
        self._ctx_entry_cooloff = {}
        self._global_squareoff_cooloff = False
        self._ctx_squareoff_cooloff = {}
        self._squareoff_cooloff = {}
        
        if not roll:
            return
        
        for ctx in handlers:
            # restore the exit def for live assets if rolling is True
            stoplosses = handlers[ctx].get('stoploss', {})
            for asset in stoplosses:
                obj = stoplosses[asset]
                if not obj.rolling or not self._algo.is_alive(asset):
                    continue
                self.add_stoploss(
                        ctx, asset, obj.method, obj.target, obj.callback,
                        obj.trailing_fraction, obj.do_exit, obj.entry, 
                        obj.rolling)
                
            takeprofits = handlers[ctx].get('takeprofit', {})
            for asset in takeprofits:
                obj = takeprofits[asset]
                if not obj.rolling or not self._algo.is_alive(asset):
                    continue
                self.add_takeprofit(
                        ctx, asset, obj.method, obj.target, obj.callback,
                        obj.do_exit, obj.entry, obj.rolling)
                
        for ctx in ctx_handlers:
            obj = ctx_handlers[ctx].get('stoploss', None)
            if obj and obj.rolling:
                self.add_stoploss(
                        ctx, None, obj.method, obj.target, obj.callback,
                        obj.trailing_fraction, obj.do_exit, obj.entry, 
                        obj.rolling)
            
            obj = ctx_handlers[ctx].get('takeprofit', None)
            if obj and obj.rolling:
                self.add_takeprofit(
                        ctx, None, obj.method, obj.target, obj.callback,
                        obj.do_exit, obj.entry, obj.rolling)
        
    cpdef reset_stoploss(self, object context, Asset asset):
        if asset is None:
            return self._reset_strategy_stoploss(context)
        
        if context not in self._handlers:
            return
        
        stoplosses = self._handlers[context].get('stoploss', {})
        if stoplosses and asset in stoplosses:
            stoplosses.pop(asset)
            
    def _reset_strategy_stoploss(self, object context):
        if context not in self._ctx_handlers:
            return
        
        self._ctx_handlers[context].pop('stoploss', None)
            
    cpdef reset_takeprofit(self, object context, Asset asset):
        if asset is None:
            return self._reset_strategy_takeprofit(context)
        
        if context not in self._handlers:
            return
        
        takeprofits = self._handlers[context].get('takeprofit', {})
        if takeprofits and asset in takeprofits:
            takeprofits.pop(asset)
            
    def _reset_strategy_takeprofit(self, object context):
        if context not in self._ctx_handlers:
            return
        
        self._ctx_handlers[context].pop('takeprofit', None)
            
    cpdef add_stoploss(self, object context, Asset asset, 
                       ExitMethod method, np.float64_t target, 
                       object callback=noop, np.float64_t trailing=0,
                       bool do_exit=True, np.float64_t entry_level=np.nan,
                       bool rolling=False):
        cdef np.float64_t entry = 0
        cdef bool is_trailing = False
        cdef Position pos
        
        if asset is None:
            return self._add_strategy_stoploss(
                    context, method, target, callback, trailing, do_exit,
                    entry_level, rolling)
        
        if not callback:
            callback=noop
        
        if not np.isnan(entry_level):
            entry = entry_level
        elif asset in context.portfolio.positions:
            pos = context.portfolio.positions[asset]
            if method != ExitMethod.AMOUNT:
                if pos.position_side == PositionSide.LONG:
                    entry = pos.buy_price
                else:
                    entry = pos.sell_price
            else:
                entry = pos.unrealized_pnl
        
        if trailing != 0:
            is_trailing = True
        
        obj = ExitDef(
                context, ExitType.STOPLOSS, asset, method, target, 
                is_trailing, entry, callback, do_exit, rolling, trailing)
        
        if context in self._handlers:
            stoplosses = self._handlers[context].get('stoploss',{})
            if not stoplosses:
                stoplosses = {asset:obj}
                self._handlers[context]['stoploss'] = stoplosses
            else:
                stoplosses[asset] = obj
        else:
            stoplosses = {asset:obj}
            self._handlers[context] = {'stoploss':stoplosses}
            
    def _add_strategy_stoploss(self, context, method, target, 
                              callback=noop, trailing=0, do_exit=True,
                              entry_level=0, rolling=False):
        if not callback:
            callback=noop
        is_trailing=False
        
        if not np.isnan(entry_level):
            entry = entry_level
        elif method not in (ExitMethod.AMOUNT, ExitMethod.PNL):
            entry = context.blotter.account.net
        elif method == ExitMethod.PNL:
            entry = context.blotter.account.pnls
        else:
            entry = context.blotter.account.mtm
            
        if trailing != 0:
            is_trailing = True
            
        obj = ExitDef(
                context, ExitType.STOPLOSS, None, method, target, 
                is_trailing, entry, callback, do_exit, rolling, trailing)
        
        if context in self._ctx_handlers:
            self._ctx_handlers[context]['stoploss'] = obj
        else:
            self._ctx_handlers[context] = {'stoploss':obj}
            
    cpdef add_takeprofit(self, object context, Asset asset, 
                       ExitMethod method, np.float64_t target, 
                       object callback=noop, bool do_exit=True, 
                       np.float64_t entry_level=np.nan, bool rolling=False):
        cdef np.float64_t entry = 0
        cdef Position pos
        
        if asset is None:
            return self._add_strategy_takeprofit(
                    context, method, target, callback, do_exit,
                    entry_level, rolling)
        
        if not callback:
            callback=noop
        
        if not np.isnan(entry_level):
            entry = entry_level
        elif asset in context.portfolio.positions:
            pos = context.portfolio.positions[asset]
            if method != ExitMethod.AMOUNT:
                if pos.position_side == PositionSide.LONG:
                    entry = pos.buy_price
                else:
                    entry = pos.sell_price
            else:
                entry = pos.unrealized_pnl
                
        obj = ExitDef(
                context, ExitType.TAKEPROFIT, asset, method, target, 
                False, entry, callback, do_exit, rolling, 0)
        
        if context in self._handlers:
            takeprofits = self._handlers[context].get('takeprofit',{})
            if not takeprofits:
                takeprofits = {asset:obj}
                self._handlers[context]['takeprofit'] = takeprofits
            else:
                takeprofits[asset] = obj
        else:
            takeprofits = {asset:obj}
            self._handlers[context] = {'takeprofit':takeprofits}
            
    def _add_strategy_takeprofit(self, context, method, target, 
                                callback=noop, do_exit=True, 
                                entry_level=0, rolling=False):
        if not callback:
            callback=noop
            
        if not np.isnan(entry_level):
            entry = entry_level
        elif method not in (ExitMethod.AMOUNT, ExitMethod.PNL):
            entry = context.blotter.account.net
        elif method == ExitMethod.PNL:
            entry = context.blotter.account.pnls
        else:
            entry = context.blotter.account.mtm
            
        obj = ExitDef(
                context, ExitType.TAKEPROFIT, None, method, target, 
                False, entry, callback, do_exit, rolling, 0)
        
        if context in self._ctx_handlers:
            self._ctx_handlers[context]['takeprofit'] = obj
        else:
            self._ctx_handlers[context] = {'takeprofit':obj}
            
    cpdef get_stoploss(self, object context, Asset asset):
        if asset is None:
            if context in self._ctx_handlers:
                return self._ctx_handlers[context].get('stoploss', None)
            else:
                return
            
        if context in self._handlers:
            stoploss = self._handlers[context].get('stoploss', {})
            if stoploss and asset in stoploss:
                return stoploss[asset]
            
    cpdef get_takeprofit(self, object context, Asset asset):
        if asset is None:
            if context in self._ctx_handlers:
                return self._ctx_handlers[context].get('takeprofit', None)
            else:
                return
            
        if context in self._handlers:
            takeprofit = self._handlers[context].get('takeprofit', {})
            if takeprofit and asset in takeprofit:
                return takeprofit[asset]
            
    cpdef copy_stoploss(self, Asset source, Asset target):
        for context in self._handlers:
            stoploss = self._handlers[context].get('stoploss', {})
            if stoploss and source in stoploss:
                obj = stoploss.pop(source)
                new_obj = ExitDef(
                        context, ExitType.STOPLOSS, target, obj.method, 
                        obj.target, obj.trailing, obj.entry, 
                        obj.callback, obj.do_exit, obj.rolling,
                        obj.trailing_fraction)
                stoploss[target] = new_obj
                
    cpdef copy_takeprofit(self, Asset source, Asset target):
        for context in self._handlers:
            takeprofit = self._handlers[context].get('takeprofit', {})
            if takeprofit and source in takeprofit:
                obj = takeprofit.pop(source)
                new_obj = ExitDef(
                        context, ExitType.TAKEPROFIT, target, obj.method, 
                        obj.target, obj.trailing, obj.entry, 
                        obj.callback, obj.do_exit, obj.rolling,
                        obj.trailing_fraction)
                takeprofit[target] = new_obj
            
    cpdef set_cooloff(self, object assets=None, bool squareoff=False, bool is_global=False):
        context = self._algo.current_context
        if not assets:
            self._ctx_entry_cooloff[context] = self._cooloff_period
            if squareoff:
                self._ctx_squareoff_cooloff[context] = True
                if is_global:
                    self._global_squareoff_cooloff = True
        else:
            if not listlike(assets):
                assets = [assets]
            for asset in assets:
                if context not in self._entry_cooloff:
                    self._entry_cooloff[context] = {}
                self._entry_cooloff[context][asset] = self._cooloff_period
                if squareoff:
                    if context not in self._squareoff_cooloff:
                        self._squareoff_cooloff[context] = set()
                    self._squareoff_cooloff[context].add(asset)
                    
    cpdef reset_cooloff(self, object context, Asset asset):
        self._global_squareoff_cooloff = False
        
        if context in self._ctx_squareoff_cooloff:
            self._ctx_squareoff_cooloff[context] = False
        
        if context in self._ctx_entry_cooloff:
            self._ctx_entry_cooloff[context] = 0
            
        if context in self._squareoff_cooloff:
            if not asset:
                self._squareoff_cooloff[context] = set()
            elif asset in self._squareoff_cooloff[context]:
                self._squareoff_cooloff[context].remove(asset)

        if context in self._entry_cooloff:
            if asset:
                self._entry_cooloff[context].pop(asset, None)
            else:
                self._entry_cooloff[context] = {}
                    
    def validate_cooloff(self, asset=None):
        if self._cooloff_period < 0:
            return True
        
        context = self._algo.current_context
        
        if self._ctx_entry_cooloff.get(context,0) > 0:
            return False
        
        if context in self._entry_cooloff:
            if asset and asset in self._entry_cooloff[context]:
                return False
        
        return self.validate_squareoff(asset)
    
    def validate_squareoff(self, asset=None):
        if self._global_squareoff_cooloff:
            return False
    
        context = self._algo.current_context        
        if not asset:
            return not self._ctx_squareoff_cooloff.get(context, False)
        elif context in self._squareoff_cooloff:
            return asset not in self._squareoff_cooloff[context]
        
        return True
    
    def remove_context(self, ctx):
        if ctx in self._handlers:
            self._handlers.pop(ctx)
        if ctx in self._ctx_handlers:
            self._ctx_handlers.pop(ctx)
        if ctx in self._ctx_entry_cooloff:
            self._ctx_entry_cooloff.pop(ctx)
        if ctx in self._ctx_squareoff_cooloff:
            self._ctx_squareoff_cooloff.pop(ctx)
        if ctx in self._squareoff_cooloff:
            self._squareoff_cooloff.pop(ctx)
            
    def _exit_asset(self, object context, Asset asset, 
                     object callback, object do_exit=True, remark=None):
        positions = context.portfolio.positions
        open_orders = context.open_orders
        
        ctx = context.name
        try:
            with self._algo.context.switch_context(ctx):
                try:
                    status = True
                    if do_exit:
                        status = self._algo.square_off(asset, algo=True, remark=remark)
                    callback(context, asset, success=status)
                except Exception as e:
                    self._algo.handle_error(e, ctx)
        except NoContextError:
            # the context is destroyed
            msg = f'Exit for asset {asset} failed, the context {ctx} is '
            msg += 'already destroyed.'
            self.remove_context(ctx)
            self._algo.log_error(msg)
            
    def _exit_strategy(self, context, callback, do_exit=True, remark=None):
        positions = context.portfolio.positions
        open_orders = context.open_orders
        
        ctx = context.name
        try:
            with self._algo.context.switch_context(ctx):
                try:
                    status = True
                    if do_exit:
                        status = self._algo.square_off(remark=remark)
                    callback(context, None, success=status)
                except Exception as e:
                    self._algo.handle_error(e, ctx)
        except NoContextError:
            # the context is destroyed
            msg = f'Exit for context failed, the context {ctx} is '
            msg += 'already destroyed.'
            self.remove_context(ctx)
            self._algo.log_error(msg)
        else:
            # remove any stoploss and takeprofit in this context
            # to avoid multiple square-offs
            if do_exit:
                self._handlers.pop(context, None)
        
    cpdef check_exits(self, bool reset=False):
        # called every minute in handle data with reset=True and 
        # also in run_idle with reset=False
        if reset:
            # reset squareoff cooloff
            self._global_squareoff_cooloff = False
            
            for context in self._ctx_squareoff_cooloff:
                self._ctx_squareoff_cooloff[context] = False
                
            for context in self._ctx_entry_cooloff:
                if self._ctx_entry_cooloff[context] > 0:
                    self._ctx_entry_cooloff[context] -= 1
                    
            for context in self._squareoff_cooloff:
                self._squareoff_cooloff[context] = set()
            
            for context in self._entry_cooloff:
                assets = list(self._entry_cooloff[context])
                for asset in assets:
                    if self._entry_cooloff[context][asset] > 0:
                        self._entry_cooloff[context][asset] -= 1
                    if self._entry_cooloff[context][asset] <= 0:
                        self._entry_cooloff[context].pop(asset, None)
        
        # first check the strategy level triggers only if global
        # square-off is possible
        contexts = list(self._ctx_handlers.keys())
        for context in contexts:
            self._check_strategy_exits_in_context(context)
        
        # check asset level exits
        for context in self._handlers:
            self._check_exits_in_context(context)
            
    cdef _update_trailing_target_strategy(self, ExitDef sl, BlotterAccount acct):
        cdef np.float64_t frac = sl.trailing_fraction
        cdef np.float64_t tgt = 0
        cdef np.float64_t new_tgt = 0
        
        if sl.method == ExitMethod.AMOUNT:
            if acct.mtm > sl.entry:
                sl.target = sl.target - (acct.mtm - sl.entry)*frac
                sl.entry = acct.mtm
        elif sl.method == ExitMethod.PNL:
            if acct.pnls > sl.entry:
                sl.target = sl.target - (acct.pnls - sl.entry)*frac
                sl.entry = acct.pnls
        elif acct.net > sl.entry:
            if sl.method == ExitMethod.MOVE:
                tgt = sl.entry - sl.target
                new_tgt = tgt + (acct.net-sl.entry)*frac
                sl.target = acct.net - new_tgt
            elif sl.method == ExitMethod.PERCENT:
                tgt = sl.entry*(1-sl.target)
                new_tgt = tgt + (acct.net-sl.entry)*frac
                sl.target = 1-new_tgt/acct.net
            sl.entry = acct.net
            
    cdef _check_strategy_exits_in_context(self, object context):
        if context not in self._ctx_handlers:
            # no handler registered for this context
            return
        
        cdef bool sl_hit = False
        cdef bool tp_hit = False
        cdef np.float64_t pct = 0
        cdef np.float64_t move = 0
        cdef ExitDef sl, tp
        cdef bool no_sl=False
        cdef bool no_tp=False
        cdef BlotterAccount acct
        
        cdef np.float64_t current = 0
        
        positions = context.portfolio.positions
        if not positions:
            # no positions in this context to check sl/tp
            return
        
        if 'stoploss' in self._ctx_handlers[context]:
            sl = self._ctx_handlers[context].get('stoploss')
        else:
            no_sl = True
        if 'takeprofit' in self._ctx_handlers[context]:
            tp = self._ctx_handlers[context].get('takeprofit')
        else:
            no_tp = True
        acct = context.blotter.account
        
        if not no_sl or not no_tp:
            if not self.validate_squareoff():
                msg = f'skipping exit checks for strategy as square-off cool-off is on.'
                self._algo.log_warning(msg)
                return
        
        if not no_sl:
            if sl.method == ExitMethod.PNL:
                if -acct.pnls > sl.target:
                    sl_hit = True
                    current = round(acct.pnls, 2)
            elif sl.method == ExitMethod.MOVE:
                move = acct.net - sl.entry
                if -move > sl.target:
                    sl_hit = True
                    current = round(move, 2)
            elif sl.method == ExitMethod.PERCENT and sl.entry != 0:
                pct = acct.net/sl.entry - 1
                if -pct > sl.target:
                    sl_hit = True
                    current = round(pct, 2)
            elif sl.method == ExitMethod.AMOUNT:
                if -acct.mtm > sl.target:
                    sl_hit = True
                    current = round(acct.mtm, 2)
                    
        if sl_hit:
            msg = f'Stoploss triggered for strategty context {context} at '
            msg += f'pnls {acct.pnls}, mtm {acct.mtm}, entry {sl.entry}, '
            msg += f'target {sl.target} and positions {positions}.'
            remark = f'SL@{current}/{round(sl.target)}'
            if self._algo.mode == AlgoMode.BACKTEST:
                self._algo.log_info(msg)
            else:
                self._algo.log_platform(msg)
            self._ctx_handlers.pop(context, None)
            self._exit_strategy(
                    context, sl.callback, do_exit=sl.do_exit,
                    remark=remark)
        elif not no_tp:
            # check tp
            if tp.method == ExitMethod.PNL:
                if acct.pnls > tp.target:
                    tp_hit = True
                    current = round(acct.pnls, 2)
            elif tp.method == ExitMethod.MOVE:
                move = acct.net - tp.entry
                if move > tp.target:
                    tp_hit = True
                    current = round(move, 2)
            elif tp.method == ExitMethod.PERCENT and tp.entry != 0:
                pct = acct.net/tp.entry - 1
                if pct > tp.target:
                    tp_hit = True
                    current = round(pct, 2)
            elif tp.method == ExitMethod.AMOUNT:
                if acct.mtm > tp.target:
                    tp_hit = True
                    current = round(acct.mtm, 2)
                    
            if tp_hit:
                msg = f'Takeprofit triggered for strategty context {context} '
                msg += f'at pnls {acct.pnls}, mtm {acct.mtm} entry {tp.entry}, '
                msg += f', target {tp.target} and positions {positions}.'
                remark = f'TP@{current}/{round(tp.target)}'
                if self._algo.mode == AlgoMode.BACKTEST:
                    self._algo.log_info(msg)
                else:
                    self._algo.log_platform(msg)
                self._ctx_handlers.pop(context, None)
                self._exit_strategy(
                        context, tp.callback, do_exit=tp.do_exit,
                        remark=remark)
        
        if not no_sl and not sl_hit and not tp_hit and sl.trailing:
            self._update_trailing_target_strategy(sl, acct)
                
            
    cdef _check_exits_in_context(self, object context):
        if context not in self._handlers:
            # no handler registered for this context
            return
        
        positions = context.portfolio.positions
        if not positions:
            # no positions in this context to check sl/tp
            return
        
        assets = set(positions.keys())
        stoplosses = self._handlers[context].get('stoploss', {})
        takeprofits = self._handlers[context].get('takeprofit', {})
        
        sl_assets = set(stoplosses.keys())
        tp_assets = set(takeprofits.keys()) - sl_assets
        sl_assets = sl_assets.intersection(assets)
        tp_assets = tp_assets.intersection(assets)
        
        for asset in sl_assets:
            if not self.validate_squareoff(asset=asset):
                # do not check if we cannot act on square-off
                msg = f'skipping SL check for {asset} as square-off cool-off is on.'
                self._algo.log_warning(msg)
                continue
            
            tp_hit = False
            sl = stoplosses.pop(asset)
            tp = takeprofits.pop(asset, None)
            sl_hit = self._check_sl(sl, positions[asset])
            
            if sl_hit:
                # first check if sl hit, exit the asset, ignore tp
                entry = sl.entry
                px = positions[asset].last_price
                msg = f'Stoploss triggered @{px} vs entry@{entry}: exiting position {positions[asset]} in {asset}'
                remark = f'SL@{round(px,2)}/{round(entry,2)}'
                if self._algo.mode == AlgoMode.BACKTEST:
                    self._algo.log_info(msg)
                else:
                    self._algo.log_platform(msg)
                self._exit_asset(
                        context, asset, sl.callback, sl.do_exit,
                        remark=remark)
            else:
                # if not sl, check tp
                if tp:
                    tp_hit = self._check_tp(tp, positions[asset])
                    
                if tp_hit:
                    entry = tp.entry
                    px = positions[asset].last_price
                    msg = f'Takeprofit triggered @{px} vs entry@{entry}: exiting position {positions[asset]} in {asset}'
                    remark = f'TP@{round(px,2)}/{round(entry,2)}'
                    if self._algo.mode == AlgoMode.BACKTEST:
                        self._algo.log_info(msg)
                    else:
                        self._algo.log_platform(msg)
                    self._exit_asset(
                            context, asset, tp.callback, tp.do_exit,
                            remark=remark)
                else:
                    # if none hit, restore the entries
                    stoplosses[asset] = sl
                    if tp:
                        takeprofits[asset] = tp
                    
        for asset in tp_assets:
            if not self.validate_squareoff(asset=asset):
                # skip check if we cannot square-off
                msg = f'skipping TP check for {asset} as square-off cool-off is on.'
                self._algo.log_warning(msg)
                continue
            
            tp = takeprofits.pop(asset)
            tp_hit = self._check_tp(tp, positions[asset])
            if tp_hit:
                entry = tp.entry
                px = positions[asset].last_price
                msg = f'Take-profit triggered @{px} vs entry@{entry}: exiting position in {asset}'
                remark = f'TP@{round(px,2)}/{round(entry,2)}'
                if self._algo.mode == AlgoMode.BACKTEST:
                    self._algo.log_info(msg)
                else:
                    self._algo.log_platform(msg)
                self._exit_asset(
                        context, asset, tp.callback, tp.do_exit,
                        remark=remark)
            else:
                # restore it back
                takeprofits[asset] = tp
                
    cdef _update_trailing_target_asset(self, ExitDef sl, Position pos):
        cdef np.float64_t current = pos.last_price
        cdef np.float64_t frac = sl.trailing_fraction
        cdef np.float64_t tgt = 0
        cdef np.float64_t new_tgt = 0
        
        if sl.method == ExitMethod.AMOUNT:
            #sl.target = -pos.unrealized_pnl
            if pos.unrealized_pnl > sl.entry:
                sl.target = sl.target - (pos.unrealized_pnl - sl.entry)*frac
                sl.entry = pos.unrealized_pnl
        else:
            if pos.position_side == PositionSide.LONG:
                if current > sl.entry:
                    if sl.method == ExitMethod.PRICE:
                        sl.target = sl.target + (current-sl.entry)*frac
                    elif sl.method == ExitMethod.MOVE:
                        tgt = sl.entry - sl.target
                        new_tgt = tgt + (current-sl.entry)*frac
                        sl.target = current - new_tgt
                    elif sl.method == ExitMethod.PERCENT:
                        tgt = sl.entry*(1-sl.target)
                        new_tgt = tgt + (current-sl.entry)*frac
                        sl.target = 1-new_tgt/current
                    sl.entry = current
            else:
                if current < sl.entry:
                    if sl.method == ExitMethod.PRICE:
                        sl.target = sl.target - (sl.entry-current)*frac
                    elif sl.method == ExitMethod.MOVE:
                        tgt = sl.entry + sl.target
                        new_tgt = tgt - (sl.entry-current)*frac
                        sl.target = new_tgt - current
                    elif sl.method == ExitMethod.PERCENT:
                        tgt = sl.entry*(1+sl.target)
                        new_tgt = tgt - (sl.entry-current)*frac
                        sl.target = new_tgt/current-1
                    sl.entry = current
                            
    cdef _check_sl(self, ExitDef sl, Position pos):
        cdef np.float64_t current = 0
        cdef np.float64_t move = 0
        cdef bool hit = False
        
        if sl.entry == 0 and sl.method != ExitMethod.AMOUNT:
            # for exit method the entry is not price, but unrealized pnl
            # which maybe 0
            if pos.position_side == PositionSide.LONG:
                sl.entry = pos.buy_price
            else:
                sl.entry = pos.sell_price
            
        current = pos.last_price
        if sl.method == ExitMethod.PRICE:
            if pos.position_side == PositionSide.LONG:
                hit = current < sl.target 
            else:
                hit = current > sl.target
        elif sl.method == ExitMethod.MOVE:
            if sl.entry == 0:
                return False
            move = current - sl.entry
            if pos.position_side == PositionSide.LONG:
                hit = -move > sl.target
            else:
                hit = move > sl.target
        elif sl.method == ExitMethod.PERCENT:
            if sl.entry == 0:
                return False
            move = current/sl.entry -1
            if pos.position_side == PositionSide.LONG:
                hit = -move > sl.target
            else:
                hit = move > sl.target
        elif sl.method == ExitMethod.AMOUNT:
            hit = -pos.unrealized_pnl > sl.target
        
        if sl.trailing and not hit:
            self._update_trailing_target_asset(sl, pos)
        
        return hit
    
    cdef _check_tp(self, ExitDef tp, Position pos):
        cdef np.float64_t current = 0
        cdef np.float64_t move = 0
        cdef bool hit = False
        
        if tp.entry == 0:
            if pos.position_side == PositionSide.LONG:
                tp.entry = pos.buy_price
            else:
                tp.entry = pos.sell_price
            
        current = pos.last_price
        if tp.method == ExitMethod.PRICE:
            if pos.position_side == PositionSide.LONG:
                hit = current > tp.target 
            else:
                hit = current < tp.target
        elif tp.method == ExitMethod.MOVE:
            move = current - tp.entry
            if pos.position_side == PositionSide.LONG:
                hit = move > tp.target
            else:
                hit = -move > tp.target
        elif tp.method == ExitMethod.PERCENT:
            if tp.entry == 0:
                return False
            move = current/tp.entry -1
            if pos.position_side == PositionSide.LONG:
                hit = move > tp.target
            else:
                hit = -move > tp.target
        elif tp.method == ExitMethod.AMOUNT:
            hit = pos.unrealized_pnl > tp.target
            
        return hit
    
    cdef bool _check_if_empty_context(self, context):
        if context not in self._handlers and \
            context not in self._ctx_handlers:
            return True
        
        if context in self._handlers:
            stoplosses = self._handlers[context].get('stoploss', {})
            takeprofits = self._handlers[context].get('takeprofit', {})
        
            if stoplosses or takeprofits:
                return False
            
        if context in self._ctx_handlers:
            stoplosses = self._ctx_handlers[context].get('stoploss', {})
            takeprofits = self._ctx_handlers[context].get('takeprofit', {})
        
            if stoplosses or takeprofits:
                return False
        
        return True
    
    cpdef bool is_empty(self, context=None):
        if context:
            return self._check_if_empty_context(context)
        
        contexts = set(list(
                self._handlers.keys()) + list(self._ctx_handlers.keys()))
        
        for context in contexts:
            if not self._check_if_empty_context(context):
                return False
            
        return True
    
