# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython
from cpython cimport bool

cimport numpy as np
import numpy as np

from ._order_types import (
        ProductType,
        OrderFlag,
        OrderType,
        OrderValidity,
        OrderSide,
        OrderStatus,
        OrderUpdateType)

from ._trade cimport Trade
from ._position cimport Position
from blueshift.interfaces.assets._assets cimport Asset, InstrumentType

status_dict = {0:'complete',1:'open',2:'rejected',3:'cancelled'}
side_dict = {0:'BUY',1:'SELL'}

cdef class Order:
    '''
        Order object encapsulates all details about orders sent 
        by the algo to the broker. It also tracks the order fill.
        Orders are automatically tracked by internal trackers. 
        Strategy code can query its attributes to check various 
        details and fill status.
        
        An order object has the following attributes. All attributes
        are read-only from a strategy code.
        
        +-----------------+----------------------+-------------------------------+
        |Attribute        | Type                 |Description                    |
        +=================+======================+===============================+
        |oid              | ``str``              |Order ID                       |
        +-----------------+----------------------+-------------------------------+
        |asset            | :ref:`Asset`         |Asset of the position          |
        +-----------------+----------------------+-------------------------------+
        |quantity         | ``float``            |Net quantity at present        |
        +-----------------+----------------------+-------------------------------+
        |product_type     | :ref:`ProductType`   |Product type                   |
        +-----------------+----------------------+-------------------------------+
        |order_type       | :ref:`OrderType`     |Order type                     |
        +-----------------+----------------------+-------------------------------+
        |order_validity   | :ref:`OrderValidity` |Order validity                 |
        +-----------------+----------------------+-------------------------------+
        |disclosed        | ``float``            |Total amount disclosed         |
        +-----------------+----------------------+-------------------------------+
        |price            | ``float``            |Limit price                    |
        +-----------------+----------------------+-------------------------------+
        |trigger_price    | ``float``            |Stoploss trigger price         |
        +-----------------+----------------------+-------------------------------+
        |stoploss_price   | ``float``            |Limit for stop-limit order     |
        +-----------------+----------------------+-------------------------------+
        |filled           | ``float``            |Amount filled so far           |
        +-----------------+----------------------+-------------------------------+
        |pending          | ``float``            |Pending (quantity-filled)      |
        +-----------------+----------------------+-------------------------------+
        |average_price    | ``float``            |Average price of fill          |
        +-----------------+----------------------+-------------------------------+
        |side             | :ref:`OrderSide`     |Order side (buy/sell)          |
        +-----------------+----------------------+-------------------------------+
        |status           | :ref:`OrderStatus`   |Order status                   |
        +-----------------+----------------------+-------------------------------+
        |timestamp        |``pandas.Timestamp``  |Timestamp of last update       |
        +-----------------+----------------------+-------------------------------+
        |fractional       |``bool``              |If a fractional order          |
        +-----------------+----------------------+-------------------------------+
        |price_at_entry   |``float``             |Mid price at entry time        |
        +-----------------+----------------------+-------------------------------+
        |create_latency   |``float``             |Latency (milli) to create      |
        +-----------------+----------------------+-------------------------------+
        |exec_latency     |``float``             |Latency (milli) to place       |
        +-----------------+----------------------+-------------------------------+

        .. note::
            The ``oid`` is the field through which the platform 
            tracks an order (which can be different from the broker 
            or the exchange IDs).
    '''

    def __init__(self,
                 np.float64_t quantity,     # required
                 int side,                  # required
                 Asset asset,               # required
                 int product_type=ProductType.DELIVERY,
                 int order_flag=OrderFlag.NORMAL,
                 int order_type=OrderType.MARKET,
                 int order_validity = OrderValidity.DAY,
                 int disclosed=0,
                 np.float64_t price=0,             # for limit prices
                 np.float64_t trigger_price=0,     # for stoplosses
                 np.float64_t stoploss_price=0,     # for stoplosses
                 object user='',
                 object placed_by='',   # algo ID
                 object tag='blueshift',
                 object oid=None,
                 object reference_id=None,
                 object broker_order_id=None,
                 object exchange_order_id=None,
                 object parent_order_id=None,
                 np.float64_t filled=0,
                 np.float64_t pending=0,
                 np.float64_t average_price=0,
                 int status=OrderStatus.OPEN,
                 object status_message=None,
                 object exchange_timestamp=None,
                 object timestamp=None,
                 bool fractional=False,
                 np.float64_t price_at_entry=0,
                 np.float64_t create_latency=0,
                 np.float64_t exec_latency=0,
                 object remark=''       # a remark by the user
                 ):
        '''
            The only agent who can create (or delete) an order is
            the user trading agent. The broker cannot create an order.
            At the time of creation, user algo can only know about
            order details known at user level. So things like filled,
            pending, status etc are set accordingly and are not part
            of the init arguments.
        '''
        import uuid
        import pandas as pd

        cdef int qprecision=0, pprecision=2
            
        self.fractional=fractional
        if self.fractional:
            qprecision = 6
            pprecision = 6
            
        self.asset = asset
        self.side = side
        self.quantity = round(quantity, qprecision)
        self.price_at_entry = price_at_entry

        self.oid = uuid.uuid4().hex if oid is None else oid
        self.reference_id = self.oid if reference_id is None else reference_id
        self.hashed_oid = int(hash(self.oid))
        self.broker_order_id=broker_order_id
        self.exchange_order_id = exchange_order_id
        self.parent_order_id = parent_order_id

        self.user=user
        self.placed_by=placed_by
        self.product_type=product_type
        self.order_flag=order_flag
        self.order_type=order_type
        self.order_validity=order_validity
        
        self.filled=round(filled, qprecision)
        self.pending=pending if pending != 0 else (quantity-filled)
        self.pending=round(self.pending, qprecision)
        self.disclosed=disclosed
        self.price=round(price, pprecision)
        self.average_price=average_price
        self.trigger_price=round(trigger_price, pprecision)
        self.stoploss_price = round(stoploss_price, pprecision)

        self.status=status
        self.status_message=status_message
        
        try:
            if isinstance(timestamp, str):
                timestamp = pd.Timestamp(timestamp)
        except Exception:
            pass
                
        try:
            exchange_timestamp = pd.Timestamp(exchange_timestamp)
        except Exception:
            pass
        
        self.exchange_timestamp=exchange_timestamp
        self.timestamp=timestamp
        self.create_latency = create_latency
        self.exec_latency = exec_latency
        self.tag=tag
        self.remark = remark

        if self.status == OrderStatus.OPEN and self.pending == 0:
            self.status = OrderStatus.COMPLETE
            

    def __hash__(self):
        return self.hashed_oid

    def __eq__(x,y):
        try:
            return hash(x) == hash(y)
        except (TypeError, AttributeError, OverflowError):
            raise TypeError

    def __str__(self):
        return 'Order[id:%s, sym:%s, qty:%f, side:%s, filled:%f, at:%f, status:%s, timestamp:%s]' % \
                (self.oid, self.asset.exchange_ticker,self.quantity,
                 side_dict[self.side], self.filled,self.average_price,
                 status_dict[self.status], self.timestamp)

    def __repr__(self):
        return self.__str__()

    cpdef to_dict(self):
        return {
                'oid':self.oid,
                'reference_id':self.reference_id,
                'hashed_oid':self.hashed_oid,
                'broker_order_id':self.broker_order_id,
                'exchange_order_id':self.exchange_order_id,
                'parent_order_id':self.parent_order_id,
                'asset':self.asset,
                'user':self.user,
                'placed_by':self.placed_by,
                'product_type':self.product_type,
                'order_flag':self.order_flag,
                'order_type':self.order_type,
                'order_validity':self.order_validity,
                'quantity':self.quantity,
                'filled':self.filled,
                'pending':self.pending,
                'disclosed':self.disclosed,
                'price':self.price,
                'average_price':self.average_price,
                'trigger_price':self.trigger_price,
                'stoploss_price':self.stoploss_price,
                'side':self.side,
                'status':self.status,
                'status_message':self.status_message,
                'exchange_timestamp':self.exchange_timestamp,
                'timestamp':self.timestamp,
                'tag':self.tag,
                'fractional':self.fractional,
                'price_at_entry':self.price_at_entry,
                'create_latency':self.create_latency,
                'exec_latency':self.exec_latency,
                'remark':self.remark}

    cpdef to_json(self):
        cdef int qprecision=0, pprecision=2
        
        if self.fractional:
            qprecision = 6
            pprecision = 6
            
        exchange_timestamp = str(self.exchange_timestamp) if \
            self.exchange_timestamp is not None else None
        timestamp = str(self.timestamp) if self.timestamp is not None else None
            
        return {
                'oid':self.oid,
                'reference_id':self.reference_id,
                'hashed_oid':self.hashed_oid,
                'broker_order_id':self.broker_order_id,
                'exchange_order_id':self.exchange_order_id,
                'parent_order_id':self.parent_order_id,
                'asset':self.asset.exchange_ticker,
                'user':self.user,
                'placed_by':self.placed_by,
                'product_type':ProductType(self.product_type).name,
                'order_flag':OrderFlag(self.order_flag).name,
                'order_type':OrderType(self.order_type).name,
                'order_validity':OrderValidity(self.order_validity).name,
                'quantity':round(self.quantity,qprecision),
                'filled':round(self.filled,qprecision),
                'pending':round(self.pending,qprecision),
                'disclosed':self.disclosed,
                'price':round(self.price, pprecision),
                'average_price':round(self.average_price,pprecision),
                'trigger_price':round(self.trigger_price,pprecision),
                'stoploss_price':round(self.stoploss_price,pprecision),
                'side':OrderSide(self.side).name,
                'status':OrderStatus(self.status).name,
                'status_message':self.status_message,
                'exchange_timestamp':exchange_timestamp,
                'timestamp':timestamp,
                'tag':self.tag,
                'fractional':self.fractional,
                'price_at_entry':round(self.price_at_entry,pprecision),
                'create_latency':round(self.create_latency,3),
                'exec_latency':round(self.exec_latency,3),
                'remark':str(self.remark)}

    def to_json_summary(self):
        import pandas as pd
        cdef int qprecision=0, pprecision=2
        
        if self.fractional:
            qprecision = 6
            pprecision = 6
            
        exchange_timestamp = str(self.exchange_timestamp) if \
            not pd.isnull(self.exchange_timestamp) else None
        timestamp = str(self.timestamp) if not pd.isnull(self.timestamp) else None
        
        return {
                'oid':self.oid,
                'asset':self.asset.exchange_ticker,
                'quantity':round(self.quantity,qprecision),
                'filled':round(self.filled,qprecision),
                'average_price':round(self.average_price,pprecision),
                'side':OrderSide(self.side).name,
                'status':OrderStatus(self.status).name,
                'exchange_timestamp':exchange_timestamp,
                'timestamp':timestamp,
                'remark':str(self.remark)}

    cpdef __reduce__(self):
        return(self.__class__,( self.oid,
                                self.reference_id,
                                self.hashed_oid,
                                self.broker_order_id,
                                self.exchange_order_id,
                                self.parent_order_id,
                                self.asset,
                                self.user,
                                self.placed_by,
                                self.product_type,
                                self.order_flag,
                                self.order_type,
                                self.order_validity,
                                self.quantity,
                                self.filled,
                                self.pending,
                                self.disclosed,
                                self.price,
                                self.average_price,
                                self.trigger_price,
                                self.stoploss_price,
                                self.side,
                                self.status,
                                self.status_message,
                                self.exchange_timestamp,
                                self.timestamp,
                                self.tag,
                                self.fractional,
                                self.price_at_entry,
                                self.create_latency,
                                self.exec_latency,
                                self.remark))
    
    @classmethod
    def from_dict(cls, data):
        """ from an existing order, remove the unnecessary fields"""
        import pandas as pd

        data.pop('hashed_oid', None)
        
        if isinstance(data['side'], str):
            data['side'] = OrderSide[data['side'].upper()]
        if 'product_type' in data and isinstance(data['product_type'],str):
            data['product_type'] = ProductType[data['product_type'].upper()]
        if 'order_flag' in data and isinstance(data['order_flag'],str):
            data['order_flag'] = OrderFlag[data['order_flag'].upper()]
        if 'order_type' in data and isinstance(data['order_type'],str):
            data['order_type'] = OrderType[data['order_type'].upper()]
        if 'order_validity' in data and isinstance(data['order_validity'],str):
            data['order_validity'] = OrderValidity[data['order_validity'].upper()]
        if 'status' in data and isinstance(data['status'],str):
            data['status'] = OrderStatus[data['status'].upper()]
        if 'timestamp' in data and data['timestamp'] not in ('None', None):
            try:
                data['timestamp'] = pd.Timestamp(data['timestamp'])
            except Exception:
                pass
        if 'exchange_timestamp' in data and data['exchange_timestamp'] not in ('None', None):
            try:
                data['exchange_timestamp'] = pd.Timestamp(data['exchange_timestamp'])
            except Exception:
                pass
        
        return cls(**data)
    
    def copy_trackers_from_order(self, order):
        data = {
                'fractional':order.fractional,
                'tag':order.tag,
                'remark':order.remark,
                'reference_id':order.reference_id,
                'parent_order_id':order.parent_order_id,
                'placed_by':order.placed_by,
                'user':order.user,
                'price_at_entry':order.price_at_entry,
                'create_latency':order.create_latency,
                'exec_latency':order.exec_latency}
        
        self.update_from_dict(data, preserve_trackers=False)

    def update_from_dict(self, data, preserve_trackers=False):
        import pandas as pd

        cdef int pprecision=2
        
        if preserve_trackers:
            keep = ['fractional', 'tag', 'placed_by', 'user',
                    'price_at_entry','create_latency','exec_latency',
                    'reference_id','parent_order_id','remark']
            
            for k in keep:
                data.pop(k, None)
            
        if "fractional" in data:
            self.fractional = data["fractional"]
        if self.fractional:
            pprecision = 6
            
        if "asset" in data:
            # this is used for rolling asset to dated asset update
            self.asset = data["asset"]
        if "order_type" in data:
            self.order_type = int(data["order_type"])
        if "order_validity" in data:
            self.order_validity = int(data["order_validity"])
        if "exchange_order_id" in data and data["exchange_order_id"]:
            # this is used for late update of exchange order id
            self.exchange_order_id = data["exchange_order_id"]
        if "broker_order_id" in data and data["broker_order_id"]:
            # this is used for late update of broker order id
            self.broker_order_id = data["broker_order_id"]
        if "product_type" in data:
            self.product_type = int(data["product_type"])
        if "quantity" in data:
            self.quantity = data["quantity"]
        if "pending" in data:
            self.pending = data["pending"]
            self.filled = self.quantity - self.pending
        if "filled" in data:
            self.filled = data["filled"]
            self.pending = self.quantity - self.filled
        if "disclosed" in data:
            if self.fractional:
                self.disclosed = data["disclosed"]
            else:
                self.disclosed = int(data["disclosed"])
        if "price" in data:
            self.price = round(data["price"], pprecision)
        if "average_price" in data:
            self.average_price = data["average_price"]
        if "trigger_price" in data:
            self.trigger_price = round(data["trigger_price"], pprecision)
        if "stoploss_price" in data:
            self.stoploss_price = round(data["stoploss_price"], pprecision)
        if "status" in data:
            self.status = int(data["status"])
        
        if "status_message" in data:
            self.status_message = data["status_message"]
        elif "reason" in data:
            self.status_message = data["reason"]
        
        if "timestamp" in data and data['timestamp'] not in ('None', None):
            # do not update the timestamp
            #self.timestamp = data["timestamp"]
            try:
                data["timestamp"] = pd.Timestamp(data["timestamp"])
            except Exception:
                pass
            self.exchange_timestamp = data["timestamp"]
        if "exchange_timestamp" in data and data['exchange_timestamp'] not in ('None', None):
            try:
                data["exchange_timestamp"] = pd.Timestamp(data["exchange_timestamp"])
            except Exception:
                pass
            self.exchange_timestamp = data["exchange_timestamp"]
        
        if "tag" in data:
            self.tag = data["tag"]
            
        if "remark" in data:
            self.remark = data["remark"]
            
        if "reference_id" in data:
            self.reference_id = data["reference_id"]
        
        if "price_at_entry" in data:
            self.price_at_entry = data["price_at_entry"]
            
        if "create_latency" in data and data["create_latency"]>0:
            self.create_latency = data["create_latency"]
        if "exec_latency" in data and data["exec_latency"]>0:
            self.exec_latency = data["exec_latency"]
        if "placed_by" in data:
            self.placed_by = data["placed_by"]
        if "parent_order_id" in data:
            self.parent_order_id = data["parent_order_id"]
        if "user" in data:
            self.user = data["user"]

        if self.pending < 1 and self.status == OrderStatus.OPEN:
            self.status = OrderStatus.COMPLETE
            self.status_message = 'complete'

    cpdef cash_outlay(self, object to_ccy, object data_portal):
        """
            This method calculate cash outlay of the order based on the
            status as well as the asset instrument type and trade price.

            Args:
                ``to_ccy (str or CCY)``: output currency for conversion

                ``data_portal (object)``: Data portal to query fx rate.
        """
        cdef int side
        
        if self.product_type==ProductType.DELIVERY and self.asset.is_funded():
            side = 1 if self.side == OrderSide.BUY else -1
            last_fx = self.asset.fx_rate(to_ccy, data_portal, self.timestamp)
            return self.filled*self.average_price*side*last_fx
        elif self.asset.instrument_type == InstrumentType.OPT:
            # options are special in the sense they both have margins
            # and cashflows associated
            side = 1 if self.side == OrderSide.BUY else -1
            last_fx = self.asset.fx_rate(to_ccy, data_portal, self.timestamp)
            return self.filled*self.average_price*side*last_fx
        
        return 0
    
    cpdef convert_to_limit(self, float price):
        cdef int pprecision=2
        
        if self.fractional:
            pprecision = 6
            
        self.order_type = OrderType.LIMIT
        self.price = round(price, pprecision)
        self.trigger_price = 0
        self.stoploss_price = 0
    
    cpdef set_order_id(self, object order_id):
        self.oid = order_id
        
    cpdef set_reference_id(self, object reference_id):
        self.reference_id = reference_id
        
    cpdef set_user(self, object user):
        self.user = user
        
    cpdef set_placed_by(self, object placed_by):
        self.placed_by = placed_by
        
    cpdef set_tag(self, object tag):
        self.tag = tag
        
    cpdef set_remark(self, object remark):
        self.remark = remark

    cpdef set_timestamp(self, object timestamp, bool exchange=False):
        self.timestamp = timestamp

        if exchange:
            self.exchange_timestamp = timestamp
            
    cpdef set_exchange_timestamp(self, object timestamp):
        self.exchange_timestamp = timestamp
        
    cpdef set_latency(self, np.float64_t create_latency=0, 
                      np.float64_t exec_latency=0):
        if create_latency > 0:
            self.create_latency = create_latency
            
        if exec_latency > 0:
            self.exec_latency = exec_latency

    cpdef update(self, int update_type, object kwargs):
        '''
            This method is called by the execution platform, based on the
            type of updates to be done appropriate arguments must be passed.
            No validation done here.
        '''
        if update_type == OrderUpdateType.EXECUTION:
            self.partial_execution(kwargs)
        elif update_type == OrderUpdateType.MODIFICATION:
            self.user_update(kwargs)
        elif update_type == OrderUpdateType.CANCEL:
            self.partial_cancel('Order status updated to cancel')
        else:
            self.reject(kwargs)

    cpdef execute(self, float price, np.float64_t volume, 
                  object timestamp):
        if self.status !=  OrderStatus.OPEN:
            return
        
        self.average_price = (self.filled*self.average_price + \
                volume*price)
        self.filled = self.filled + volume
        self.average_price = self.average_price/self.filled
        self.pending = self.quantity - self.filled

        if self.pending > 0:
            self.status = OrderStatus.OPEN
            self.status_message = 'open'
        else:
            self.status = OrderStatus.COMPLETE
            self.status_message = 'complete'

        self.exchange_timestamp = timestamp

    cpdef partial_execution(self, Trade trade):
        """
            Pass on a Trade object to update a full or partial execution
        """
        if self.status !=  OrderStatus.OPEN:
            return
        
        if self.order_validity == OrderValidity.FOK and self.pending > trade.quantity:
            return self.partial_cancel('FOK cancel')
            
        self.average_price = (self.filled*self.average_price + \
                trade.quantity*trade.average_price)
        self.filled = self.filled + trade.quantity
        self.average_price = self.average_price/self.filled
        self.pending = self.quantity - self.filled

        if self.pending > 0:
            self.status = OrderStatus.OPEN
            self.status_message = 'open'
        else:
            self.status = OrderStatus.COMPLETE
            self.status_message = 'complete'

        if self.broker_order_id is None:
            self.broker_order_id = trade.broker_order_id
            self.exchange_order_id = trade.exchange_order_id

        self.exchange_timestamp = trade.exchange_timestamp
        
        if self.order_validity == OrderValidity.IOC and self.pending > 0:
            self.partial_cancel('IOC cancel')

    cpdef partial_cancel(self, object reason='cancel'):
        '''
            This cancels the remaining part of the order. The order is marked
            cancelled. The exeucted part should modify the corresponding
            positions already
        '''
        if self.status !=  OrderStatus.OPEN:
            return
        
        self.status = OrderStatus.CANCELLED
        self.status_message = str(reason)

    cpdef reject(self, object reason):
        '''
            The case of reject. A reject can never be partial. the argument
            passed is a string explaining the reason to reject.
        '''
        if self.status !=  OrderStatus.OPEN:
            return
        
        self.status = OrderStatus.REJECTED
        self.status_message = reason


    cpdef user_update(self, object kwargs):
        '''
            Fields to be updated on user request. All fields may
            not be present.
        '''
        cdef int pprecision=2
        
        if self.status !=  OrderStatus.OPEN:
            return
        
        if self.fractional:
            pprecision = 6
        
        if 'price' in kwargs:
            self.price = round(kwargs['price'], pprecision)
        if 'quantity' in kwargs:
            self.quantity = kwargs['quantity']
            self.pending = self.quantity - self.filled

        if 'trigger_price' in kwargs:
            self.trigger_price = round(kwargs['trigger_price'], pprecision)
        if 'stoploss_price' in kwargs:
            self.stoploss_price = round(kwargs['stoploss_price'], pprecision)
        if 'order_type' in kwargs:
            self.order_type = kwargs['order_type']
        if 'order_validity' in kwargs:
            self.order_validity = kwargs['order_validity']

        if 'tag' in kwargs:
            self.tag = kwargs['tag']
        if 'remark' in kwargs:
            self.remark = kwargs['remark']
        if 'disclosed' in kwargs:
            self.disclosed = kwargs['disclosed']


    cpdef update_from_pos(self, Position pos, np.float64_t price):
        '''
            Pass on a position object to update a full or partial execution
        '''
        if self.status !=  OrderStatus.OPEN:
            return
        
        self.average_price = price
        self.filled = abs(pos.quantity)
        self.pending = self.quantity - self.filled

        if self.pending > 0:
            self.status = OrderStatus.OPEN
            self.status_message = 'open'
        else:
            self.status = OrderStatus.COMPLETE
            self.status_message = 'complete'

        self.exchange_timestamp = pos.timestamp
        self.timestamp = pos.timestamp
        
    cpdef bool is_inactive(self):
        if self.is_final():
            return False
        
        return self.status != OrderStatus.OPEN

    cpdef bool is_open(self):
        """ Is the order still open. """
        return self.status == OrderStatus.OPEN
    
    cpdef bool is_done(self):
        return self.status == OrderStatus.COMPLETE
    
    cpdef bool is_cancelled(self):
        return self.status == OrderStatus.CANCELLED
    
    cpdef bool is_rejected(self):
        return self.status == OrderStatus.REJECTED
    
    cpdef bool has_failed(self):
        return self.status in (
                OrderStatus.REJECTED, 
                OrderStatus.CANCELLED,
                )
    
    cpdef bool is_final(self):
        return self.status in (
                OrderStatus.COMPLETE, 
                OrderStatus.REJECTED, 
                OrderStatus.CANCELLED,
                )

    cpdef bool is_buy(self):
        """ Is the order a buy order. """
        return self.side == OrderSide.BUY
    
    cpdef bool is_triggered(self, np.float64_t last_price):
        cdef bool triggered=False
        
        if self.order_type not in (OrderType.STOPLOSS, OrderType.STOPLOSS_MARKET):
            return True
        
        if self.side == OrderSide.SELL and last_price < self.trigger_price:
            triggered = True
        elif self.side == OrderSide.BUY and last_price > self.trigger_price:
            triggered = True
            
        if triggered:
            if self.order_type==OrderType.STOPLOSS_MARKET:
                self.order_type = OrderType.MARKET
            else:
                self.order_type = OrderType.LIMIT
                self.price = self.stoploss_price
                
        return triggered

    def signature(self):
        return self.asset.exchange_ticker + str(self.quantity) + str(self.side)