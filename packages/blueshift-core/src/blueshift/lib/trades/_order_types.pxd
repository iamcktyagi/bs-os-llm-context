# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
cimport cython

cpdef enum ProductType:
    INTRADAY = 0,       # margin product with intraday auto-squareoff
    DELIVERY = 1        # normal delivery product
    MARGIN = 2          # margin product
    ALGO = 3            # Algo product/ smart orders

cpdef enum OrderFlag:
    NORMAL = 0,
    AMO = 1             # after markets order, schedule next day

cpdef enum OrderType:
    MARKET = 0,         # market order
    LIMIT = 1,          # limit order
    STOPLOSS = 2,       # stop-limit order
    STOPLOSS_MARKET = 3 # stoploss order
    SMART = 4           # smart algo

cpdef enum OrderValidity:
    DAY = 0,
    IOC = 1,            # Immedeate or Cancel
    FOK = 2,            # Fill or Kill - on partial fill order cancelled
    AON = 3,            # All or None - on partila fill, order retained
    GTC = 4,            # Good till cencelled
    OPG = 5,            # submit “market on open” (MOO) and “limit on open” (LOO) orders. This order is eligible to execute only in the market opening auction
    CLS = 6             # order type to submit “market on close” (MOC) and “limit on close” (LOC) orders. This order is eligible to execute only in the market closing auction

cpdef enum OrderSide:
    BUY = 0,
    SELL = 1
    
cpdef enum PositionSide:
    LONG = 0,
    SHORT = 1

cpdef enum OrderStatus:
    COMPLETE = 0,
    OPEN = 1,
    REJECTED = 2,
    CANCELLED = 3,
    INACTIVE = 4

cpdef enum OrderUpdateType:
    EXECUTION = 0       # full or partial execution
    CANCEL = 1          # cancelled - full or remaining - by user
    MODIFICATION = 2    # user posted modification request
    REJECT = 3          # rejected by the execution platform

cpdef enum GREEK:
    DELTA=0
    GAMMA=1
    VEGA=2
    THETA=3