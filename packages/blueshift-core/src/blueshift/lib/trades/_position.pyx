# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython

cimport numpy as np
import numpy as np
from cpython cimport bool

from ._order_types import (
        ProductType,
        OrderFlag,
        OrderType,
        OrderValidity,
        OrderSide,
        PositionSide,
        OrderStatus,
        OrderUpdateType,
        GREEK)

from ._trade cimport Trade
from blueshift.interfaces.assets._assets cimport Asset, InstrumentType, OptionType
from ._order cimport Order

from blueshift.lib.common.constants import ALMOST_ZERO

cdef class Position:
    '''
        A position reflect the current economic interest in an 
        asset, including associated profit and losses accumulated 
        over the lifetime of such position. Positions are maintained 
        by the algo blotter (though a dedicated tracker) and are 
        updated on each trade or as and when prices change.
        
        A position object has the following attributes. All attributes
        are read-only from a strategy code.
        
        +-----------------+--------------------+-------------------------------+
        |Attribute        | Type               |Description                    |
        +=================+====================+===============================+
        |asset            | :ref:`Asset`       |Asset of the position          |
        +-----------------+--------------------+-------------------------------+
        |quantity         | ``float``          |Net quantity at present        |
        +-----------------+--------------------+-------------------------------+
        |buy_quantity     | ``float``          |Total buying quantity          |
        +-----------------+--------------------+-------------------------------+
        |buy_price        | ``float``          |Average buy price              |
        +-----------------+--------------------+-------------------------------+
        |sell_quantity    | ``float``          |Total sell quantity            |
        +-----------------+--------------------+-------------------------------+
        |sell_price       | ``float``          |Average selling price          |
        +-----------------+--------------------+-------------------------------+
        |pnl              | ``float``          |Total profit or loss           |
        +-----------------+--------------------+-------------------------------+
        |realized_pnl     | ``float``          |Realised part of pnl           |
        +-----------------+--------------------+-------------------------------+
        |unrealized_pnl   | ``float``          |Unrealized (MTM) part of pnl   |
        +-----------------+--------------------+-------------------------------+
        |last_price       | ``float``          |Last updated price             |
        +-----------------+--------------------+-------------------------------+
        |last_fx          | ``float``          |Last known FX conversion rate  |
        +-----------------+--------------------+-------------------------------+
        |underlying_price | ``float``          |Underlying price               |
        +-----------------+--------------------+-------------------------------+
        |timestamp        |``pandas.Timestamp``|Timestamp of last update       |
        +-----------------+--------------------+-------------------------------+
        |value            | ``float``          |Holding value of this position |
        +-----------------+--------------------+-------------------------------+
        |cost_basis       | ``float``          |Entry cost of this position    |
        +-----------------+--------------------+-------------------------------+
        |margin           | ``float``          |Margin posted (if available)   |
        +-----------------+--------------------+-------------------------------+
        |product_type     | :ref:`ProductType` |Product type                   |
        +-----------------+--------------------+-------------------------------+
        |position_side    | :ref:`PositionSide`|Position side (Long/Short)     |
        +-----------------+--------------------+-------------------------------+
        |fractional       |``bool``            |If a fractional order          |
        +-----------------+--------------------+-------------------------------+
            
            .. seealso::
                See :ref:`Asset` for details on asset object. See 
                :ref:`Orders` for details on order objects.
    '''

    def __init__(self,
                 Asset asset,
                 np.float64_t quantity,
                 int side,
                 object instrument_id,
                 int product_type,
                 np.float64_t average_price,
                 np.float64_t margin,
                 object timestamp,
                 object exchange_timestamp,
                 np.float64_t buy_quantity=0,
                 np.float64_t buy_price=0,
                 np.float64_t sell_quantity=0,
                 np.float64_t sell_price=0,
                 np.float64_t pnl=0,
                 np.float64_t realized_pnl=0,
                 np.float64_t unrealized_pnl=0,
                 np.float64_t last_price=0,
                 np.float64_t last_fx=1.0,
                 np.float64_t underlying_price=0,
                 bool fractional=False):
        '''
            The algo creates a position once a new trade is done and a
            matching position is not found. Matching is done on the underlying
            and once a position is closed, it is never re-used. A new one will
            be created instead.
        '''   
        import pandas as pd

        self.fractional = fractional
        self.asset = asset
        self.pid = hash(asset)
        self.instrument_id = instrument_id
        
        try:
            if isinstance(timestamp, str):
                timestamp = pd.Timestamp(timestamp)
        except Exception:
            pass
                
        try:
            if isinstance(exchange_timestamp, str):
                exchange_timestamp = pd.Timestamp(exchange_timestamp)
        except Exception:
            pass

        if side != -1:
            if side == OrderSide.BUY:
                self.quantity = quantity
                self.buy_price = average_price
                self.buy_quantity = quantity
                self.sell_quantity = 0
                self.sell_price = 0
                self.position_side = PositionSide.LONG
            else:
                self.quantity = -quantity
                self.sell_quantity = quantity
                self.sell_price = average_price
                self.buy_price = 0
                self.buy_quantity = 0
                self.position_side = PositionSide.SHORT
            self.pnl = 0
            self.realized_pnl = 0
            self.unrealized_pnl = 0
            self.last_price = average_price
            self.last_fx = last_fx
            self.underlying_price = underlying_price
            self.margin = margin
            self.timestamp = timestamp
            self.entry_time = exchange_timestamp
            self.product_type = product_type
            
            #if self.product_type==ProductType.DELIVERY and self.asset.is_funded():
            if self.is_funded() or self.asset.instrument_type==InstrumentType.OPT:
                self.value = self.quantity*average_price*last_fx
                self.cost_basis = self.value - self.unrealized_pnl
            else:
                self.value = 0
                self.cost_basis = 0
        else:
            self.quantity = quantity
            self.buy_price = buy_price
            self.buy_quantity = buy_quantity
            self.sell_quantity = sell_quantity
            self.sell_price = sell_price
            self.realized_pnl = realized_pnl
            self.unrealized_pnl = unrealized_pnl
            self.last_price = last_price
            self.last_fx = last_fx
            self.underlying_price = underlying_price
            self.margin = margin
            self.timestamp = timestamp
            self.entry_time = exchange_timestamp
            self.product_type = product_type
            
            if self.quantity > 0:
                self.position_side = PositionSide.LONG
            elif self.quantity < 0:
                self.position_side = PositionSide.SHORT

            self.compute_pnls()
            self.update_value()

    def __hash__(self):
        return self.pid

    def __float__(self):
        return self.quantity

    def __eq__(x,y):
        try:
            return hash(x) == hash(y)
        except (TypeError, AttributeError, OverflowError):
            raise TypeError

    def __str__(self):
        return 'Position[sym:%s,qty:%f,realized:%f, unrealized:%f]' %\
            (self.asset.exchange_ticker,self.quantity, self.realized_pnl,
             self.unrealized_pnl)

    def __repr__(self):
        return self.__str__()

    cpdef to_dict(self):
        return {'pid':self.pid,
                'instrument_id':self.instrument_id,
                'asset':self.asset,
                'quantity':self.quantity,
                'buy_quantity':self.buy_quantity,
                'buy_price':self.buy_price,
                'sell_quantity':self.sell_quantity,
                'sell_price':self.sell_price,
                'pnl':self.pnl,
                'realized_pnl':self.realized_pnl,
                'unrealized_pnl':self.unrealized_pnl,
                'last_price':self.last_price,
                'last_fx':self.last_fx,
                'underlying_price':self.underlying_price,
                'margin':self.margin,
                'timestamp':self.timestamp,
                'entry_time':self.entry_time,
                'value':self.value,
                'cost_basis':self.cost_basis,
                'product_type':self.product_type,
                'position_side':self.position_side,
                'fractional':self.fractional,
                }

    cpdef to_json(self):
        cdef int qprecision=0, pprecision=2
        if self.fractional:
            qprecision = 6
            pprecision = 6
            
        return {'pid':self.pid,
                'instrument_id':self.instrument_id,
                'asset':self.asset.exchange_ticker,
                'quantity':round(self.quantity,qprecision),
                'buy_quantity':round(self.buy_quantity,qprecision),
                'buy_price':round(self.buy_price,pprecision),
                'sell_quantity':round(self.sell_quantity,qprecision),
                'sell_price':round(self.sell_price,pprecision),
                'pnl':round(self.pnl,pprecision),
                'realized_pnl':round(self.realized_pnl,pprecision),
                'unrealized_pnl':round(self.unrealized_pnl,pprecision),
                'last_price':round(self.last_price,pprecision),
                'last_fx':round(self.last_fx,pprecision),
                'underlying_price':round(self.underlying_price,pprecision),
                'margin':round(self.margin,2),
                'timestamp':str(self.timestamp),
                'entry_time':str(self.entry_time),
                'value':round(self.value,2),
                'cost_basis':round(self.cost_basis,2),
                'product_type':ProductType(self.product_type).name,
                'position_side':PositionSide(self.position_side).name,
                'fractional':self.fractional,
                }

    def to_json_summary(self):
        import pandas as pd

        cdef int qprecision=0, pprecision=2
        if self.fractional:
            qprecision = 6
            pprecision = 6
            
        timestamp = str(self.timestamp) if not pd.isnull(self.timestamp) else None
            
        return {
                'asset':self.asset.exchange_ticker,
                'quantity':self.quantity,
                'pnl':round(self.pnl,pprecision),
                'realized_pnl':round(self.realized_pnl,pprecision),
                'unrealized_pnl':round(self.unrealized_pnl,pprecision),
                'last_price':round(self.last_price,pprecision),
                'timestamp':timestamp,
                'position_side':PositionSide(self.position_side).name
                }

    cpdef __reduce__(self):
        return(self.__class__,( self.pid,
                                self.instrument_id,
                                self.asset,
                                self.quantity,
                                self.buy_quantity,
                                self.buy_price,
                                self.sell_quantity,
                                self.sell_price,
                                self.pnl,
                                self.realized_pnl,
                                self.unrealized_pnl,
                                self.last_price,
                                self.last_fx,
                                self.underlying_price,
                                self.margin,
                                self.timestamp,
                                self.entry_time,
                                self.value,
                                self.cost_basis,
                                self.product_type,
                                self.position_side,
                                self.fractional
                                ))

    @classmethod
    def from_dict(cls, dict data):
        """
            we are building from an existing positions. Side, price etc
            are not relevant.
        """
        import pandas as pd

        data['side'] = -1
        data['average_price'] = 0
        data['last_fx'] = data.pop('last_fx', 1.0)
        data['underlying_price'] = data.pop('underlying_price', 0.0)
        
        if 'margin' not in data:
            data['margin'] = 0
        
        if 'exchange_timestamp' in data:
            data['exchange_timestamp'] = data['exchange_timestamp']
        elif 'entry_time' in data:
            data['exchange_timestamp'] = data.pop('entry_time')
        else:
            data['exchange_timestamp'] = None
            
        if 'timestamp' in data and isinstance(data['timestamp'],str):
            try:
                data['timestamp'] = pd.Timestamp(data['timestamp'])
            except Exception:
                pass
        if 'exchange_timestamp' in data and isinstance(data['exchange_timestamp'],str):
            try:
                data['exchange_timestamp'] = pd.Timestamp(data['exchange_timestamp'])
            except Exception:
                pass
        
        data.pop('pid',None)
        data.pop('value',None)
        data.pop('cost_basis',None)
        data.pop('position_side',None)
        
        # for improved json
        if 'product_type' in data and isinstance(data['product_type'],str):
            data['product_type'] = ProductType[data['product_type']]
        
        return cls(**data)

    def update_from_dict(self, data):
        import pandas as pd

        if "asset" in data:
            self.asset = data["asset"]
        if "quantity" in data:
            self.quantity = data["quantity"]
        if "buy_quantity" in data:
            self.buy_quantity = data["buy_quantity"]
        if "buy_price" in data:
            self.buy_price = data["buy_price"]
        if "sell_quantity" in data:
            self.sell_quantity = data["sell_quantity"]
        if "sell_price" in data:
            self.sell_price = data["sell_price"]
        if "pnl" in data:
            self.pnl = data["pnl"]
        if "realized_pnl" in data:
            self.realized_pnl = data["realized_pnl"]
        if "unrealized_pnl" in data:
            self.unrealized_pnl = data["unrealized_pnl"]
        if "last_price" in data:
            self.last_price = data["last_price"]
        if "last_fx" in data:
            self.last_fx = data["last_fx"]
        if "underlying_price" in data:
            self.underlying_price = data["underlying_price"]
        if "timestamp" in data:
            try:
                if isinstance(data["timestamp"], str):
                    data["timestamp"] = pd.Timestamp(data["timestamp"])
            except Exception:
                pass
            self.timestamp = data["timestamp"]
        if "entry_time" in data:
            try:
                if isinstance(data["entry_time"], str):
                    data["entry_time"] = pd.Timestamp(data["entry_time"])
            except Exception:
                pass
            self.entry_time = data["entry_time"]
        if "value" in data:
            self.value = data["value"]
        if "cost_basis" in data:
            self.cost_basis = data["cost_basis"]
        if "margin" in data:
            self.margin = data["margin"]
        if "product_type" in data:
            self.product_type = int(data["product_type"])
            
        if self.quantity > 0:
            self.position_side = PositionSide.LONG
        elif self.quantity < 0:
            self.position_side = PositionSide.SHORT

    @classmethod
    def from_trade(cls,Trade t):
        """
          Create new position object from trade
        """
        cdef Position p
        p = Position(t.asset, t.quantity,
                     t.side, t.instrument_id, t.product_type,
                     t.average_price, t.margin, t.timestamp,
                     t.exchange_timestamp, last_fx=t.last_fx,
                     underlying_price=t.underlying_price,
                     fractional=t.fractional)
        return p

    @classmethod
    def from_order(cls, Order o, np.float64_t margin=0,
                   np.float64_t last_fx=1.0, 
                   np.float64_t underlying_price=0):
        """
          Update / create position object from order
        """
        cdef Position p
        p = Position(o.asset, o.filled,
                     o.side, o.asset.sid, o.product_type,
                     o.average_price, margin, o.timestamp,
                     o.exchange_timestamp, last_fx = last_fx,
                     fractional=o.fractional)
        return p
    
    def copy(self):
        return Position.from_dict(self.to_dict())

    cdef compute_pnls(self, bool realized=True):
        if self.buy_quantity > self.sell_quantity:
            if realized:
                self.realized_pnl = self.sell_quantity*\
                                        (self.sell_price - self.buy_price)
            self.unrealized_pnl = (self.buy_quantity - self.sell_quantity)*\
                                    (self.last_price - self.buy_price)
        else:
            if realized:
                self.realized_pnl = self.buy_quantity*\
                                        (self.sell_price - self.buy_price)
            self.unrealized_pnl = (self.sell_quantity - self.buy_quantity)*\
                                    (self.sell_price - self.last_price)
        if realized:
            self.realized_pnl = self.realized_pnl*self.last_fx
        self.unrealized_pnl = self.unrealized_pnl*self.last_fx
        self.pnl = self.unrealized_pnl + self.realized_pnl

    cdef update_value(self):
        #if self.product_type==ProductType.DELIVERY and self.asset.is_funded():
        if self.is_funded() or self.asset.instrument_type==InstrumentType.OPT:
            self.value = self.quantity*self.last_price*self.last_fx
            self.cost_basis = self.value - self.unrealized_pnl
        else:
            self.value = 0
            self.cost_basis = 0

    cpdef update_price(self, np.float64_t last_price,
                       np.float64_t last_fx=1.0, 
                       np.float64_t underlying_price=0):
        if np.isnan(last_price) or np.isnan(last_fx) or np.isnan(underlying_price):
            return
        
        self.last_price = last_price
        self.last_fx = last_fx
        self.underlying_price = underlying_price

        self.compute_pnls()
        self.update_value()

    cpdef add_to_position(self, Position pos):
        cdef bool realized = False
        
        if abs(self.quantity+pos.quantity) < abs(self.quantity):
            realized = True
        
        if pos.buy_quantity > 0:
            self.buy_price = self.buy_quantity*self.buy_price + \
                                pos.buy_quantity*pos.buy_price
            self.buy_quantity = self.buy_quantity + pos.buy_quantity
            self.buy_price = self.buy_price / self.buy_quantity

        if pos.sell_quantity > 0:
            self.sell_price = self.sell_quantity*self.sell_price + \
                                pos.sell_quantity*pos.sell_price
            self.sell_quantity = self.sell_quantity + pos.sell_quantity
            self.sell_price = self.sell_price / self.sell_quantity

        self.quantity = self.quantity + pos.quantity
        self.last_price = pos.last_price
        self.last_fx = pos.last_fx
        self.underlying_price = pos.underlying_price

        if self.entry_time > pos.entry_time:
            self.entry_time = pos.entry_time
        if pos.timestamp and self.timestamp and self.timestamp < pos.timestamp:
            self.timestamp = pos.timestamp
            
        if self.quantity > 0:
            self.position_side = PositionSide.LONG
        elif self.quantity < 0:
            self.position_side = PositionSide.SHORT

        self.margin = self.margin + pos.margin
        self.compute_pnls()
        self.update_value()

    cpdef update(self, Trade trade):
        cdef np.float64_t last_qty
        cdef bool realized = False
        
        last_qty = self.quantity
        if trade.side == OrderSide.BUY:
            self.buy_price = self.buy_quantity*self.buy_price + \
                                trade.average_price*trade.quantity
            self.buy_quantity = self.buy_quantity + trade.quantity
            self.buy_price = self.buy_price / self.buy_quantity
            self.quantity = self.quantity + trade.quantity
        else:
            self.sell_price = self.sell_quantity*self.sell_price + \
                                trade.average_price*trade.quantity
            self.sell_quantity = self.sell_quantity + trade.quantity
            self.sell_price = self.sell_price / self.sell_quantity
            self.quantity = self.quantity - trade.quantity
        
        if abs(last_qty) < abs(self.quantity):
            realized = True
            
        self.last_price = trade.average_price
        self.last_fx = trade.last_fx
        self.underlying_price = trade.underlying_price
        
        if self.quantity > 0:
            self.position_side = PositionSide.LONG
        elif self.quantity < 0:
            self.position_side = PositionSide.SHORT
        
        self.timestamp = trade.timestamp
        self.margin = self.margin + trade.margin
        self.compute_pnls()
        self.update_value()

    cpdef bool if_closed(self):
        """ If this position is closed out. """
        if self.quantity == 0:
            return True
        if self.fractional and abs(self.quantity) < ALMOST_ZERO:
            return True
        return False
            

    cpdef np.float64_t apply_split(self, np.float64_t ratio,
                                   np.float64_t last_fx=1.0):
        cdef np.float64_t cash, amount, remains

        cash = amount = remains = 0

        amount = self.quantity*ratio
        self.quantity = self.quantity*ratio
        self.last_fx = last_fx

        remains = amount - self.quantity
        self.buy_quantity = self.buy_quantity*ratio
        self.sell_quantity = self.sell_quantity*ratio
        self.buy_price = self.buy_price/ratio
        self.sell_price = self.sell_price/ratio
        self.last_price = self.last_price/ratio
        cash = remains*self.last_price*self.last_fx

        self.compute_pnls()
        self.update_value()

        return cash

    cpdef np.float64_t apply_merger(self, Asset acquirer,
                                    np.float64_t ratio,
                                    np.float64_t cash_pct,
                                    np.float64_t last_fx=1.0):
        cdef np.float64_t dirty_qty, cash1, cash2

        dirty_qty = cash1 = cash2 = 0

        dirty_qty = self.quantity*cash_pct
        
        if not self.fractional:
            self.quantity = np.floor(dirty_qty)
        else:
            self.quantity = dirty_qty
        self.last_fx = last_fx

        if not self.fractional:
            cash1 = (dirty_qty - self.quantity)*self.last_price*self.last_fx
        else:
            cash1 = 0
        cash2 = self.apply_split(ratio, last_fx)

        self.compute_pnls()
        self.update_value()

        return cash1+cash2
    
    cpdef get_mtm_settlement(self, np.float64_t quantity, np.float64_t last_price,
                              np.float64_t last_fx=1.0):
        """ get settlement cashflow from realized profit given a trade. """
        cdef np.float64_t mtm, sell_price, buy_price, realized = 0
        cdef np.float64_t unwind_qty, buy_qty, sell_qty = 0
        
        # no settlement cashflow for funded positions
        #if self.product_type==ProductType.DELIVERY and self.asset.is_funded():
        if self.is_funded() or self.asset.instrument_type==InstrumentType.OPT:
            return 0
        
        if quantity==0:
            return 0
        elif abs(quantity) < ALMOST_ZERO:
            return 0
        
        if abs(self.quantity) < abs(quantity):
            # maybe position flip, mtm settlement will only on the
            # unwind part
            unwind_qty = np.copysign(self.quantity, quantity)
        else:
            unwind_qty = quantity
        
        sell_price = self.sell_price
        buy_price = self.buy_price
        buy_qty = self.buy_quantity
        sell_qty = self.sell_quantity
        
        if unwind_qty < 0:
            sell_price = -last_price*unwind_qty + self.sell_price*self.sell_quantity
            sell_price = sell_price/(-unwind_qty+self.sell_quantity)
            sell_qty = -unwind_qty + self.sell_quantity
        else:
            buy_price = last_price*unwind_qty + self.buy_price*self.buy_quantity
            buy_price = buy_price/(unwind_qty+self.buy_quantity)
            buy_qty = unwind_qty + self.buy_quantity
            
        if sell_qty > buy_qty:
            realized = buy_qty*(sell_price - buy_price)
        else:
            realized = sell_qty*(sell_price - buy_price)
            
        mtm = realized - self.realized_pnl
            
        return -mtm*last_fx
    
    cpdef get_unwind_order(self, object timestamp, np.float64_t quantity=0, 
                           int product_type=ProductType.DELIVERY, 
                           int order_flag=OrderFlag.NORMAL, 
                           int order_type=OrderType.MARKET, 
                           int order_validity=OrderValidity.DAY,
                           object remark=''):
        cdef int side
        cdef np.float64_t qty
        cdef Order order
        
        side = OrderSide.BUY if self.quantity < 0 else OrderSide.SELL
        
        if abs(quantity) < ALMOST_ZERO:
            quantity = 0
        
        if quantity==0:
            qty = abs(self.quantity)
        else:
            qty = max(abs(quantity), abs(self.quantity))
            
        order = Order(qty, side, self.asset, 
                      product_type=product_type, order_flag=order_flag,
                      order_type=order_type, order_validity=order_validity,
                      timestamp=timestamp,
                      fractional=self.fractional,
                      remark=remark)
        
        return order

    cpdef double get_exposure(self):
        cdef np.float64_t delta = 1
        cdef np.float64_t ref_price = 1
        
        if self.asset.instrument_type == InstrumentType.STRATEGY:
            raise NotImplementedError
            
        if self.asset.instrument_type == InstrumentType.OPT:
            if self.asset.option_type == OptionType.PUT:
                delta = -delta
                
            # avoid zero exposure when underlying price is not updated
            if self.underlying_price==0:
                ref_price = self.last_price
            else:
                ref_price = self.underlying_price
            return self.quantity*ref_price*self.last_fx*delta
        
        return self.quantity*self.last_price*self.last_fx
            
    cpdef double get_value(self):
        if self.is_funded() or self.asset.instrument_type==InstrumentType.OPT:
            return self.quantity*self.last_price*self.last_fx
        else:
            return 0
        
    cpdef double upfront_cash(self, np.float64_t pct_margin=0.15):
        cdef int pprecision=2
        if self.fractional:
            pprecision = 6
            
        # upfront cash requirement for a new position
        cdef double margin = abs(self.get_exposure()*pct_margin)
        cdef double value = self.get_value()
        
        if self.asset.instrument_type == InstrumentType.OPT and self.quantity>0:
            # for long options margin is 0
            margin = 0
        elif self.is_funded() and self.quantity>0:
            # for long funded trade, margin is 0
            margin = 0
        elif self.is_funded() and self.quantity<0:
            # for short funded trade, it is actually a margin product
            value = 0
        
        return round(margin + value, pprecision) 
        
    cpdef bool is_funded(self):
        if self.product_type != ProductType.DELIVERY:
            return False
        
        return self.asset.is_funded()

    cpdef double get_greeks(self, int greek):
        raise NotImplementedError