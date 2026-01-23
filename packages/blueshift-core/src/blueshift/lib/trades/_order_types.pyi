from enum import Enum

class ProductType(Enum):
    INTRADAY:ProductType
    DELIVERY:ProductType
    MARGIN:ProductType
    ALGO:ProductType

class OrderFlag(Enum):
    NORMAL:OrderFlag
    AMO:OrderFlag

class OrderType(Enum):
    MARKET:OrderType
    LIMIT:OrderType

class OrderValidity(Enum):
    DAY:OrderValidity
    IOC:OrderValidity
    FOK:OrderValidity

class OrderSide(Enum):
    BUY:OrderSide
    SELL:OrderSide
    
class PositionSide(Enum):
    LONG:PositionSide
    SHORT:PositionSide

class OrderStatus(Enum):
    COMPLETE:OrderStatus
    OPEN :OrderStatus
    REJECTED:OrderStatus
    CANCELLED:OrderStatus
    INACTIVE:OrderStatus

class OrderUpdateType(Enum):
    EXECUTION:OrderUpdateType       # full or partial execution
    CANCEL:OrderUpdateType          # cancelled - full or remaining - by user
    MODIFICATION:OrderUpdateType    # user posted modification request
    REJECT:OrderUpdateType          # rejected by the execution platform

class GREEK(Enum):
    DELTA:GREEK
    GAMMA:GREEK
    VEGA:GREEK
    THETA:GREEK