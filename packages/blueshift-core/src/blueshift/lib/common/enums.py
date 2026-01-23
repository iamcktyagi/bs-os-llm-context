from enum import Enum, unique

class AlgoCallBack(Enum):
    ''' algo callback type enums. '''
    TRADE = 'TRADE'
    ACCOUNT = 'ACCOUNT'
    POSITION = 'POSITION'
    DATA = 'DATA'
    NEWS = 'NEWS'
    INFO = 'INFO'
    ERROR = 'ERROR'
    IDLE = 'IDLE'
    ONECLICK = 'ONECLICK'
    EXTERNAL = 'EXTERNAL'
    CONNECT = 'CONNECT'
    DISCONNECT = 'DISCONNECT'
    RECONCILIATION = 'RECONCILIATION'
    
class AlgoTimeoutEvent(Enum):
    ''' algo timeout event enum. '''
    TIMEOUT = 'timeout'
    
class AlgoTerminateEvent(Enum):
    ''' algo termination event enum. '''
    CANCEL = 'cancel'
    STOP = 'stop'
    KILL = 'kill'
    REJECT = 'reject'
    DONE = 'done'
    
@unique
class AlgoMode(Enum):
    """
        Track the current running mode of algo - live or backtest.
        ``BACKTEST`` for backtesting mode and ``LIVE`` for live mode.
    """
    BACKTEST = 'BACKTEST'   # algo is running in backtest mode.
    LIVE = 'LIVE'           # algo is running in live trading mode.
    PAPER = 'PAPER'         # algo is running in paper trading simulation
    EXECUTION = 'EXECUTION' # algo is running in smart execution mode.
    
REALTIME_MODES = set([AlgoMode.LIVE, AlgoMode.PAPER, AlgoMode.EXECUTION,])
BACKTEST_MODES = set([AlgoMode.BACKTEST,])
LIVE_MODES = set([AlgoMode.LIVE, AlgoMode.EXECUTION,])
SIMULATION_MODES = set([AlgoMode.BACKTEST, AlgoMode.PAPER,])
    
@unique
class ExecutionMode(Enum):
    """
        Track the current execution mode of a live algo. ``AUTO``  
        stands for automatic sending of orders to the broker. 
        ``ONECLICK`` means the user must confirm the order before 
        it is send to the broker.
    """
    AUTO = 'AUTO'
    ONECLICK = 'ONECLICK'

@unique
class AlgoState(Enum):
    """
        Track the current state of the algo state machine.
    """
    # these enums must be in integer values and in order as these 
    # are comapred to know the current state of an Algorithm/Strategy.
    STARTUP = 0
    INITIALIZED = 1
    BEFORE_TRADING_START = 2
    TRADING_BAR = 3
    AFTER_TRADING_HOURS = 4
    HEARTBEAT = 5
    PAUSED = 6
    STOPPED = 7
    DORMANT = 8
    
class AlgoStatus(Enum):
    CREATED='created'
    RUNNING='running'
    PAUSED='paused'
    CANCELLED='cancelled'
    STOPPED='stopped'
    KILLED='killed'
    ERRORED='errored'
    DONE='done'
    OPEN = 'open' # only for smart orders
    REJECTED = 'rejected' # only for smart orders
    
TerminalAlgoStatuses = set([AlgoStatus.CANCELLED, AlgoStatus.STOPPED, 
                            AlgoStatus.ERRORED, AlgoStatus.DONE,
                            AlgoStatus.REJECTED, AlgoStatus.KILLED])

class BlotterType(Enum):
    """
        Type of blotter - virtual (multiple algos sharing same
        account) or exclusive (one-to-one algo and account)
    """
    VIRTUAL='VIRTUAL'
    EXCLUSIVE='EXCLUSIVE'

class BrokerType(Enum):
    """
        Types of brokers. TWS broker is specific to IB only.
    """
    BACKTESTER = 'BACKTESTER'
    PAPERTRADER = 'PAPERTRADER'
    RESTBROKER = 'RESTBROKER'
    TWSBROKER = 'TWSBROKER'
    MULTIUSER = 'multiuser'

class AlgoPlatform(Enum):
    """ platform types """
    CONSOLE = 'cli'
    WHITELABLE = 'blueshift'
    API = 'api'
    WEB = 'web'
    DESKTOP = 'desktop'
    MOBILE = 'mobile'
    
class AccountEventType(Enum):
    """ account events and corporate actions. """
    RollOver = 0
    CapitalChange = 1
    StockDividend = 2
    StockSplit = 3
    StockMerger = 4
    
class AlgoMessageType(Enum):
    """ published messages types """
    INIT = 'INIT'
    COMPLETE = 'COMPLETE'    # emit after event loop exit
    BAR = 'BAR'
    DAILY = 'DAILY'
    SOB = 'SOB'         # before trading start
    PAUSED = 'PAUSED'      # paused state
    RESUMED = 'RESUMED'
    HEARTBEAT = 'HEARTBEAT'
    ALGO_END = 'ALGO_END'  # emit after all processing
    HEALTH_HEARTBEAT = 'HEALTH_HEARTBEAT'
    STDOUT = 'STDOUT'
    TRADES = 'TRADES'
    TRANSACTIONS = 'TRANSACTIONS'
    POSITIONS = 'POSITIONS'
    VALUATION = 'VALUATION'
    LOG = 'LOG'
    COMMAND = 'COMMAND'
    PLATFORM = 'PLATFORM'
    ONECLICK = 'ONECLICK'
    DEBUG = 'DEBUG'
    STATUS_MSG = 'STATUS_MSG'
    SMART_ORDER = 'SMART_ORDER'
    NOTIFY = 'NOTIFY' # notify the front end to flash the message
    CALLBACK = 'CALLBACK' # callback
    
class OneclickMethod(Enum):
    """ Methods accepted under one-click confirmation flow """
    PLACE_ORDER = 'PLACE_ORDER'
    CANCEL_ORDER = 'CANCEL_ORDER'
    
class SmartOrderAction(Enum):
    """ Methods accepted under one-click confirmation flow """
    CREATE = 'create'
    CANCEL = 'cancel'

class BlueshiftCallbackAction(Enum):
    EOD_UPLOAD = 'eod-upload'
    BROKER_AUTH = 'broker-auth'
    ALGO_STATUS = 'algo-status'
    SMART_ORDER_STATUS = 'smart-order-status'
    SUB_STRATEGY_UPDATE = 'sub-strategy-update'
    SERVICE_STATUS = 'service-status'
    EXTERNAL_EVENT = 'external-event'

class AlgoOrderStatus(Enum):
    COMPLETE = 'complete',
    OPEN = 'open',
    REJECTED = 'rejected',
    CANCELLED = 'cancelled'
    CREATED = 'created'
    ERRORED = 'errored'

class ConnectionStatus(Enum):
    """ Connection status for brokers supporting streaming. """
    PENDING="pending"
    ABORTED="aborted"
    UNSET="unset"
    ESTABLISHED="established"
    DISCONNECTED="disconnected"

class BrokerTables(Enum):
    """ Broker tables list for thread-safe manipulations. """
    OFFERS=0
    OPEN_POSITIONS=1
    CLOSED_POSITIONS=2
    ORDERS=3
    SUMMARY=4
    ACCOUNTS=5
    PRICE=6

class OneClickStatus(Enum):
    """Oneclick confirmation object processing status. """
    PROCESSED = "processed"
    PROCESSING = "processing"
    
class OneClickState(Enum):
    """Oneclick user confirmation state. """
    ACTIONED = "actioned"
    ACKNOWLEDGED = "acknowledged"
    EXPIRED = "expired"
    ERRORED = "errored"
    COMPLETED = "completed"
    PENDING = "pending"
    APPROVED = 'approved'
    DISCARDED = 'discarded'

class ExpiryType(Enum):
    """ option expiry types."""
    DAILY='DAILY'
    WEEKLY='WEEKLY'
    MONTHLY='MONTHLY'
    EOM='EOM'
    QUARTERLY='QUARTERLY'
    IMM='IMM'
    LEAPS='LEAPS'
    OCC='OCC' # dated expiry

class ChunkerType(Enum):
    DATE='date'
    EXPIRY='expiry'

class CallBackResult(Enum):
    SUCCESS='success' # success
    FAILED='failed' # failed to post, not fatal
    ERROR='error' # fatal error
    UNREACHABLE='unreachable' # callback server not reachable