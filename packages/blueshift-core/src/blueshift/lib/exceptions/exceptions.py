from __future__ import annotations
from typing import Type, Callable
import sys
import builtins
from enum import Enum

class BlueshiftWarning(UserWarning):
    pass

class ExceptionHandling(Enum):
    IGNORE = 0      # continue execution silently
    LOG = 1         # continue execution, log message
    WARN = 2        # continue execution, log warning
    RECOVER = 3     # log error, save context, re-start with context
    TERMINATE = 4   # terminate the current context
    CRITICAL = 5   # terminate all contexts
    
def add_info_to_error_msg(msg:str, module:str="<unknown>", function:str="<unknown>") -> str:
    return f"In module {module}, function {function}, error:{msg}"
    
def raise_exception(e:Exception|None, msg:str, cls:Type[BlueshiftException], module:str|None=None, 
                    func:str|None=None, handling:ExceptionHandling|None=None, url:str|None=None):
    """
        Re-raise caught exception with handling suggestions.

        Args:
            ``e  (Exception)``: Exception caught.

            ``msg (str)``: Error message to add.

            ``cls (BlueshiftException)``: Exception to raise.

            ``module (str)``: Module information.

            ``func (str)``: Function/ method information.

            ``handling (ExceptionHandling)``: Handling suggestion.
    """
    if e:
        msg = f"{msg}:{str(e)}"

    if module is not None and func is not None:
        msg = add_info_to_error_msg(msg, module, func)
        
    if not url and hasattr(e,'url'):
        url = e.url # type: ignore

    if handling:
        raise cls(msg=msg, handling=handling, url=url)
    else:
        raise cls(msg=msg, url=url)

class BlueshiftException(Exception):
    """ base class for all blueshift exceptions."""
    msg = 'Blueshift exception'
    handling = ExceptionHandling.TERMINATE
    url = None
    sep = '->>>'
    
    def __init__(self, *args, **kwargs):
        str_msg = self.msg
        keep_default = kwargs.pop('keep_default', True)
        
        if 'msg' in kwargs:
            str_msg = kwargs.pop('msg')
            if keep_default and not str_msg.startswith(self.msg+': '):
                str_msg = self.msg + f'{self.sep} ' + str_msg
        elif len(args)>0 and args[0] is not None:
            str_msg = str(args[0])
            if keep_default and not str_msg.startswith(self.msg+': '):
                str_msg = self.msg + f'{self.sep} ' + str_msg
            
        if not str_msg.endswith('.'):
            str_msg += '.'
            
        if 'url' in kwargs:
            self.url = kwargs['url']
        if self.url:
            str_msg += f' For more information on this error, see {self.url}.'
            
        if 'handling' in kwargs:
            handling = kwargs.pop('handling')
            if isinstance(handling, ExceptionHandling):
                self.handling = handling
            
        super(BlueshiftException, self).__init__(str_msg)
        
    def to_dict(self):
        return {'class': self.__class__.__name__,
                'error':str(self),
                'handling':self.handling.name}
        
    @classmethod
    def from_dict(cls, data):
        if not isinstance(data, dict):
            return cls(str(data))
            
        klass = None
        class_name = data.get('class')
        
        if class_name:
            try:
                klass = getattr(
                        sys.modules[__name__], class_name, None)
            except Exception:
                pass
            
            if not klass:
                klass = getattr(builtins, class_name, None)
        
        message = data.get('error','something went wrong.')
        if klass:
            if issubclass(klass, BlueshiftException):
                try:
                    handling = ExceptionHandling[data['handling']]
                except Exception:
                    handling = None
                if handling:
                    e = klass(message, handling=handling)
                else:
                    e = klass(message)
            else:
                e = klass(message)
        else:
            e = cls(message)
            
        return e
    
def jsonify_exception(e):
    if isinstance(e, BlueshiftException):
        return e.to_dict()
    
    return {'class':e.__class__.__name__, 'error':str(e)}

def get_clean_err_msg(msg:str, sep=BlueshiftException.sep) -> str:
    parts = msg.split(sep)
    if len(parts) > 2:
        msg = parts[0] + f'{sep} ' + parts[-1]
    
    return msg
    
################ strategy and related error ########################

class StrategyError(BlueshiftException):
    pass

class NotValidStrategy(StrategyError):
    """ Illegal strategy type. """
    msg = "Not a valid strategy"
    handling = ExceptionHandling.TERMINATE
    
class NoCodeForStrategy(StrategyError):
    """ Strategy has no associated code. """
    msg = "Strategy has no associated code"
    handling = ExceptionHandling.TERMINATE
    
class UserDefinedException(StrategyError):
    msg = "Error in strategy"
    
class ScheduleFunctionError(StrategyError):
    '''
        Not a valid scheduler inputs or related issues.
    '''
    msg = "Schedule function error"
    handling = ExceptionHandling.TERMINATE
    
class NoSuchPipeline(StrategyError):
    """ Pipeline requested is not registered. See 
        :ref:`Pipeline APIs<Pipeline APIs>`.
    """
    pass
    
class TradingControlError(StrategyError):
    '''
        A trading risk control check failed. See 
        :ref:`Risk Management APIs<Risk Management APIs>`.
    '''
    msg = "Trading control error"
    handling = ExceptionHandling.TERMINATE
    
class RecordVarError(StrategyError):
    '''
        Not a valid record var value.
    '''
    msg = "Record variable error"
    handling = ExceptionHandling.TERMINATE
    
class AlgoOrderError(StrategyError):
    """ Error in Blueshift algo order. """
    msg = "Error in algo order"
    handling = ExceptionHandling.TERMINATE
    
class AlgoStateError(StrategyError):
    """ Error raised from utility AlgoStateMachine. """
    msg = "Error in algo state"
    handling = ExceptionHandling.TERMINATE
    
################ algo input errors ##############################
    
class InputError(BlueshiftException):
    pass
    
class InitializationError(InputError):
    """malformed request object."""
    msg = "Initialization error"
    handling = ExceptionHandling.TERMINATE
    
class AlgoStartError(InputError):
    """ Error in Blueshift algo order. """
    msg = "Error in algo order"
    handling = ExceptionHandling.TERMINATE
    
class ValidationError(InputError):
    """ Validation failed. Raise this exception to flag invalid inputs 
        or parameters.
    """
    msg = 'Validation error'
    handling = ExceptionHandling.TERMINATE
    
class ConfigurationError(InputError):
    '''
        Unexpected error about config object.
    '''
    msg = "Unexpected Error in config"
    handling = ExceptionHandling.TERMINATE
    
########### broker, orders, apis related errors ###################

class BrokerError(BlueshiftException):
    """ Something went wrong while connecting to the broker or fetching 
        data over broker API.
    """
    msg = "Broker error"
    handling = ExceptionHandling.WARN
    
class BrokerNotFound(BrokerError):
    """ broker not implemented."""
    msg = "Broker error"
    handling = ExceptionHandling.TERMINATE

class BrokerInitializationError(BrokerError):
    """ broker not initialized."""
    msg = "Broker error"
    handling = ExceptionHandling.TERMINATE
    
class ConnectivityError(BrokerError):
    """Failed to connect, something wrong with the network."""
    msg = "error in connections"
    
class AuthenticationError(BrokerError):
    """ Failed in broker authentication. """
    msg = "Authentication Error"
    handling = ExceptionHandling.CRITICAL

class BrokerConnectionError(ConnectivityError):
    """ Error in connecting to broker servers."""
    msg = "Broker connection error"
    
class APIError(BrokerError):
    """ Broker server sent an error response, either because of invalid 
        or illegal input parameters, or the server failed to respond 
        temporarily.
    """
    msg = "API error"
    handling = ExceptionHandling.WARN

class APIException(BrokerError):
    """ Broker API exception."""
    msg = "API failed."
    handling = ExceptionHandling.TERMINATE
    
class DataAPIError(APIError):
    """ API error from broker during a data fetch request.
    """
    msg = "data fetch error"
    handling = ExceptionHandling.WARN
    
class TooManyRequests(APIError):
    """ breached request limit for the operation. """
    msg = 'Too many requests'
    handling = ExceptionHandling.WARN

class ServerError(APIError):
    """Received an error response from broker data or trade API."""
    msg = 'Server error'
    handling = ExceptionHandling.WARN

class BadDataError(APIError):
    """ Received malformed data from broker server."""
    msg = 'Malformed data'
    handling = ExceptionHandling.WARN

class IllegalRequest(APIError):
    """Illegal parameters for API request."""
    msg = 'Illegal request'
    handling = ExceptionHandling.WARN

class APIRetryError(APIError):
    """refused at api end-point, but we will retry."""
    msg = "API retry error"
    handling = ExceptionHandling.WARN
    
class PositionNotFound(BrokerError):
    """ missing position. """
    msg = 'No such position'
    
class OrderError(BrokerError):
    """Error while placing an order."""
    msg = "Error in placing order"
    handling = ExceptionHandling.WARN
    
class SquareoffError(BrokerError):
    """Error while placing an order."""
    msg = "Error in square-off"
    handling = ExceptionHandling.WARN
    
class ReconciliationError(OrderError):
    """fatal order reconciliation failure."""
    msg = "Reconciliation error"
    handling = ExceptionHandling.TERMINATE
    
class PreTradeCheckError(OrderError):
    """check before placing order failed. """
    msg = "pre-trade check failed"
    handling = ExceptionHandling.WARN

class InsufficientFund(OrderError):
    """Insufficient fund in account. Could not complete the transaction. """
    msg = "Insufficient fund"
    
class PriceOutOfRange(OrderError):
    """Limit price out of range. Could not complete the transaction. """
    msg = "Limit price out of range"

class TradingBlockError(OrderError):
    """Trading blocked error."""
    msg = "Trading blocked"
    
class OrderNotFound(OrderError):
    """ missing order. """
    msg = 'No such order'
    
class OrderNotReady(OrderError):
    """ order not ready. """
    msg = 'order not ready - try again later.'
    
class OrderAlreadyProcessed(OrderError):
    """Order cannot be modified or cancel, as it is already processed."""
    msg = "Order already processed"
    handling = ExceptionHandling.WARN
    
################# calendar and sessions related errors ##############

class BlueshiftCalendarException(BlueshiftException):
    """Exception base class."""
    pass

class NotValidCalendar(BlueshiftCalendarException):
    """ Not a valid calendar """
    msg = "Invalid calendar"

class SessionOutofRange(BlueshiftCalendarException):
    """ Not a valid calendar, calendar dispatch failed. """
    def __init__(self, message='', *args, **kwargs):
        if message == '':
            message = 'session out of range:'
            
        if 'dt' in kwargs:
            message += str(kwargs['dt'])
            
        super(SessionOutofRange, self).__init__(msg=message)

class InvalidDate(BlueshiftCalendarException):
    """ Not a valid calendar, calendar dispatch failed. """
    def __init__(self, message='', *args, **kwargs):
        if message == '':
            message = 'invalid date:'
            
        if 'dt' in kwargs:
            message += str(kwargs['dt'])
            
        super(InvalidDate, self).__init__(msg=message)

############### data and assets related exceptions ####################

class BlueshiftDataException(BlueshiftException):
    """Exception base class."""
    pass

class MissingDataError(BlueshiftDataException):
    """ Missing data error. """
    msg = "Missing data"

class AssetsException(BlueshiftDataException):
    """asset definition exceptions."""
    pass

class AssetsDBException(AssetsException):
    """ asset database related exceptions. """
    pass

class AssetNotFound(AssetsException):
    """ data store exceptions. """
    pass

class SymbolNotFound(AssetNotFound):
    """ Symbol requested does not exist. Usually raised when 
        :ref:`symbol<Assets Fetching APIs>` function fails.
    """
    msg = "Asset not found"
    handling = ExceptionHandling.TERMINATE

class DataStoreException(BlueshiftDataException):
    """ data store exceptions. """
    pass

class DataReadException(DataStoreException):
    '''
        Unexpected error about reading data from disk.
    '''
    msg = "Failed to read data from disk"
    handling = ExceptionHandling.TERMINATE

class HistoryWindowStartsBeforeData(DataStoreException):
    """ Data query in pipelines extended beyond available start date. 
        See :ref:`Pipeline APIs<Pipeline APIs>`.
    """
    handling = ExceptionHandling.TERMINATE
    
class NoDataForAsset(DataStoreException):
    """ Data query extended beyond available start date. See 
        :ref:`data.history<Querying Historical Data>`.
    """
    handling = ExceptionHandling.TERMINATE

class DataSourceException(BlueshiftDataException):
    """ data source exceptions. """
    pass

class IngestionException(BlueshiftDataException):
    """ ingestion exceptions. """
    msg = "Ingestion error"
    
class BundleException(IngestionException):
    """ data set exceptions. """
    pass

class DataSetException(IngestionException):
    """ data set exceptions. """
    pass
    
class DataWriteException(IngestionException):
    '''
        Unexpected error about saving data to disk.
    '''
    msg = "Failed to write data to disk"
    handling = ExceptionHandling.TERMINATE
    
########## algo control related exceptions ######################
    
class ControlException(BlueshiftException):
    msg = "Control error"
    handling = ExceptionHandling.TERMINATE
    
# Control Exception
class CommandShutdownException(ControlException):
    '''
        Raised when a shutdown command received.
    '''
    msg = "Shutdown command received"
    handling = ExceptionHandling.TERMINATE
    
class ExecutionTimeout(ControlException):
    msg = "Run-time timeout error"
    
class TerminationError(ControlException):
    msg = "Algo run terminated"
    
class SubContextTerminate(ControlException):
    '''
        Raised for subcontext termination.
    '''
    msg = "Sub-strategy will terminate"
    handling = ExceptionHandling.WARN
    
class UserStopError(TerminationError):
    msg = "Stop requested, Blueshift will shutdown"
    
############## run time and internal exceptions ########################
    
class BlueshiftRuntimeError(BlueshiftException):
    pass

class ContextException(BlueshiftRuntimeError):
    pass

class ContextError(ContextException):
    msg = 'Context error'

class NoContextError(ContextException):
    msg = 'No context found'
    
class DataException(BlueshiftRuntimeError):
    '''
        Unexpected error about internal data.
    '''
    msg = "Unexpected error"
    handling = ExceptionHandling.TERMINATE
    
class RetryError(BlueshiftRuntimeError):
    """ retry of the operation did not succeed. """
    msg = 'Retry permanently failed'

class InternalError(BlueshiftRuntimeError):
    msg = "Internal error"

class StateError(InternalError):
    '''
        A function called in illegal state.
    '''
    msg = "Illegal state"
    handling = ExceptionHandling.TERMINATE
    
class StateMachineError(StateError):
    '''
        Illegal state transition attempted in a state machine.
    '''
    msg = "Error in attempted state change"
    handling = ExceptionHandling.TERMINATE
    
class ClockError(InternalError):
    '''
        Unexpected termination of clock.
    '''
    msg = "Unexpected Error in Clock"
    handling = ExceptionHandling.TERMINATE
    
class AlertManagerError(InternalError):
    '''
        Unexpected error about alert manager.
    '''
    msg = "Unexpected Error in alert manager"
    handling = ExceptionHandling.TERMINATE
    
class BlueshiftPathException(InternalError):
    '''
        Unexpected error about path handling.
    '''
    msg = "Unexpected Error in Blueshift"
    handling = ExceptionHandling.TERMINATE
    
class SmartOrderException(InternalError):
    '''
        Raised if for execution mode errors.
    '''
    msg = "Smart order error"
    handling = ExceptionHandling.WARN
    
class OneClickException(InternalError):
    '''
        Raised for errors related to one-click notifications.
    '''
    msg = "Already processed or not exists"
    handling = ExceptionHandling.WARN

class AlreadyProcessedException(OneClickException):
    '''
        Raised if one-click notification already processed.
    '''
    msg = "Already processed or not exists"
    handling = ExceptionHandling.WARN

class ExpiredNotificationException(OneClickException):
    '''
        Raised if one-click notification is expired.
    '''
    msg = "Notification expired"
    handling = ExceptionHandling.WARN
    
class ExceptionSource:
    """ base data structure for source of exception """
    def __init__(self, user=None, algo=None, filename=None, lineno=None):
        self._user = user
        self._algo = algo
        self._filename = filename
        self._lineno = lineno
    
class CodeRedException(Exception):
    """ Code red base exception class """
    msg = "{msg}"

    def __init__(self, *args, **kwargs):
        user = kwargs.pop("user", None)
        algo = kwargs.pop("algo", None)
        filename = kwargs.pop("filename", None)
        lineno = kwargs.pop("lineno", None)
        self.source = ExceptionSource(user, algo, filename, lineno)
        self._error_msg = self.create_message(**kwargs)
        self.kwargs = kwargs

    def create_message(self, **kwargs):
        msg = kwargs.pop("msg", self.msg)
        if self.source._filename and self.source._lineno:
            msg = "Error in line {}, file <{}>:".format(
                    self.source._lineno, self.source._filename) + msg
        return msg

    def message(self):
        return self.__str__()

    def __str__(self):
        return self._error_msg

    def __repr__(self):
        return self.__str__()
    
class MissingConfigError(CodeRedException):
    msg = "config or config file missing."

class StrException(CodeRedException):
    msg = "String conversion on this object is not supported."

class TypeException(CodeRedException):
    msg = "Use of type function on this object is not supported."

class ImportException(CodeRedException):
    pass

class UnsafeCodeError(CodeRedException):
    msg = "illegal code."


CodeRedExceptionMapping = {
        UnsafeCodeError: SyntaxError,
        TypeException: RuntimeError,
        StrException: RuntimeError,
        ImportException: ImportError,
        }

class CodeGenError(Exception):
    # exception thrown during Python code generation
    def __init__(self, msg, block_id=None):
        super(CodeGenError, self).__init__(msg)
        self.id = block_id

class BlueshiftLibraryException(UserDefinedException):
    """ Error in Blueshift Library operation. """
    msg = 'error in blueshift library function'

class TimeSeriesException(BlueshiftLibraryException):
    """ Error in Blueshift Library timeseries operation. """
    msg = 'timeseries handling exception'
    
class TechnicalsException(BlueshiftLibraryException):
    """ Error in Blueshift Library technicals analysis operation. """
    msg = 'technical indicators exception'
    
class StatsException(BlueshiftLibraryException):
    """ Error in Blueshift Library statistical analysis operation. """
    msg = 'Error in stats function'
    
class OHLCVException(TimeSeriesException):
    """ Invalid data for Open-High-Low-Close-Volume input. """
    msg = "Error in OHLCV input"
    
class ResampleError(TimeSeriesException):
    """ Error in input data resampling operation. """
    msg = "Error in resampling"
    
class RollingWindowError(TimeSeriesException):
    """ Error in input data rolling window stats computation. """
    msg = "Error in rolling window"
    
class ExpandingWindowError(TimeSeriesException):
    """ Error in input data cumulative window stats computation. """
    msg = "Error in expanding window"
    
class ModelError(TimeSeriesException):
    """ Error in Blueshift Library model. """
    msg = "Error in estimating model"
