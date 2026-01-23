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
        OrderStatus,
        OrderUpdateType,
        GREEK)

from blueshift.interfaces.assets._assets cimport Asset, InstrumentType

cdef class Trade:
    '''
        Trade object definition. A trade belongs to an order that
        generated the trade(s)
    '''

    def __init__(self,
                 int tid,
                 Asset asset,
                 np.float64_t quantity,
                 np.float64_t average_price,
                 int side,
                 object oid,
                 object broker_order_id,
                 object exchange_order_id,
                 object instrument_id,
                 int product_type,
                 np.float64_t cash_flow,
                 np.float64_t margin,
                 np.float64_t commission,
                 np.float64_t charges,
                 object exchange_timestamp,
                 object timestamp,
                 np.float64_t last_fx=1.0,
                 np.float64_t underlying_price=0,
                 bool fractional=False):
        '''
            The only agent who can create a trade is the execution
            platform. All fields are required.
        '''
        self.fractional = fractional
        self.asset = asset
        self.tid = tid
        self.hashed_tid = int(hash(tid))
        self.quantity = quantity
        self.side = side
        self.oid = oid
        self.broker_order_id = broker_order_id
        self.exchange_order_id = exchange_order_id
        self.instrument_id = instrument_id
        self.product_type = product_type
        self.average_price = average_price
        self.cash_flow = cash_flow
        self.margin = margin
        self.commission = commission
        self.charges = charges
        self.exchange_timestamp = exchange_timestamp
        self.timestamp = timestamp
        self.last_fx = last_fx
        self.underlying_price = underlying_price

    def __hash__(self):
        return self.hashed_tid

    def __eq__(x,y):
        try:
            return hash(x) == hash(y)
        except (TypeError, AttributeError, OverflowError):
            raise TypeError

    def __str__(self):
        return 'Trade[sym:%s,qty:%f,average price:%f]' % \
                    (self.asset.exchange_ticker,self.quantity,
                     self.average_price)

    def __repr__(self):
        return self.__str__()

    cpdef to_dict(self):
        return {'tid':self.tid,
                'hashed_tid':self.hashed_tid,
                'oid':self.oid,
                'broker_order_id':self.broker_order_id,
                'exchange_order_id':self.exchange_order_id,
                'instrument_id':self.instrument_id,
                'asset':self.asset,
                'side':self.side,
                'product_type':self.product_type,
                'average_price':self.average_price,
                'cash_flow':self.cash_flow,
                'margin':self.margin,
                'commission':self.commission,
                'charges':self.charges,
                'exchange_timestamp':self.exchange_timestamp,
                'timestamp':self.timestamp,
                'quantity':self.quantity,
                'last_fx':self.last_fx,
                'underlying_price':self.underlying_price,
                'fractional':self.fractional}

    cpdef to_json(self):
        return {'tid':self.tid,
                'hashed_tid':self.hashed_tid,
                'oid':self.oid,
                'broker_order_id':self.broker_order_id,
                'exchange_order_id':self.exchange_order_id,
                'instrument_id':self.instrument_id,
                'asset':self.asset.exchange_ticker,
                'side':self.side,
                'product_type':self.product_type,
                'average_price':self.average_price,
                'cash_flow':self.cash_flow,
                'margin':self.margin,
                'commission':self.commission,
                'charges':self.charges,
                'exchange_timestamp':str(self.exchange_timestamp),
                'timestamp':str(self.timestamp),
                'quantity':self.quantity,
                'last_fx':self.last_fx,
                'underlying_price':self.underlying_price,
                'fractional':self.fractional}

    def to_json_summary(self):
        return {'tid':self.tid,
                'hashed_tid':self.hashed_tid,
                'oid':self.oid,
                'broker_order_id':self.broker_order_id,
                'exchange_order_id':self.exchange_order_id,
                'instrument_id':self.instrument_id,
                'asset':self.asset.exchange_ticker,
                'side':OrderSide(self.side).name,
                'product_type':ProductType(self.product_type).name,
                'average_price':self.average_price,
                'cash_flow':self.cash_flow,
                'margin':self.margin,
                'commission':self.commission,
                'charges':self.charges,
                'exchange_timestamp':str(self.exchange_timestamp),
                'timestamp':str(self.timestamp),
                'quantity':self.quantity,
                'last_fx':self.last_fx,
                'underlying_price':self.underlying_price,
                'fractional':self.fractional}

    cpdef __reduce__(self):
        return(self.__class__,( self.tid,
                                self.hashed_tid,
                                self.oid,
                                self.broker_order_id,
                                self.exchange_order_id,
                                self.instrument_id,
                                self.asset,
                                self.side,
                                self.product_type,
                                self.average_price,
                                self.cash_flow,
                                self.margin,
                                self.commission,
                                self.charges,
                                self.quantity,
                                self.exchange_timestamp,
                                self.timestamp,
                                self.last_fx,
                                self.underlying_price,
                                self.fractional))

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def update_from_dict(self, data):
        if "product_type" in data:
            self.product_type = int(data["product_type"])
        if "average_price" in data:
            self.average_price = data["average_price"]
        if "cash_flow" in data:
            self.cash_flow = data["cash_flow"]
        if "margin" in data:
            self.margin = data["margin"]
        if "commission" in data:
            self.commission = data["commission"]
        if "charges" in data:
            self.charges = data["charges"]
        if "timestamp" in data:
            self.timestamp = data["timestamp"]
            self.exchange_timestamp = data["exchange_timestamp"]
        if "exchange_timestamp" in data:
            self.exchange_timestamp = data["exchange_timestamp"]
        if "quantity" in data:
            if self.fractional:
                self.quantity = data["quantity"]
            else:
                self.quantity = int(data["quantity"])
        if "last_fx" in data:
            self.last_fx = float(data["last_fx"])
        if "underlying_price" in data:
            self.underlying_price = float(data["underlying_price"])
            
    cpdef get_cash_outlay(self):
        cdef np.float64_t outflow = 0
        outflow = self.cash_flow + self.margin + self.charges + \
                    self.commission
        return outflow

    cpdef get_exposure(self):
        cdef np.float64_t side = 0
        
        if self.asset.instrument_== InstrumentType.STRATEGY:
            raise NotImplementedError

        if self.side == OrderSide.BUY:
            side = 1
        else:
            side = -1
            
        if self.asset.instrument_type == InstrumentType.OPT:
            return side*self.quantity*self.underlying_price*self.last_fx
        
        return side*self.quantity*self.average_price*self.last_fx

    cpdef get_value(self):
        if self.product_type==ProductType.DELIVERY and self.asset.is_funded():
            return self.get_exposure()
        elif self.asset.instrument_type == InstrumentType.OPT:
            return self.get_exposure()
        else:
            return 0

    cpdef get_greeks(self, int greek):
        raise NotImplementedError
