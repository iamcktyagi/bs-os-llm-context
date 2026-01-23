
from transitions import Machine

from blueshift.lib.common.enums import AlgoState as STATE, AlgoMode, ExecutionMode


class _AlgoStateMachine:
    """ 
        An implementation of state machine rules for an algorithm. States
        changes are triggered by two sets of events. One is the clock tick.
        The second is any command from channel (i.e. user interaction, 
        only for live mode).
        
        Note:
            Clock transitions: ignoring state altering commands, any 
            backtest can move like dormant -> startup -> before trading 
            start -> trading bar -> after trading hours -> dromant. 
            For a live algo, if started on a trading hour, it will be 
            dormant -> startup -> before trading start -> trading bar -> 
            after trading hours -> heartbeat -> before trading -> trading 
            bar -> after trading hours -> heartbeat and so on. If started 
            on a non-trading hour, it can jump from initialize to heartbeat. 
            So dormant -> initialize -> heartbeat -> before start -> heart 
            beat -> trading bars -> after trading -> heartbeat and so on. 
            On stop from any signal it goes to `stopped` state.
            
        Args:
            ``name (str)``: A name for the state machine.
            
            ``mode (int)``: Mode of the algo run.
            
            ``execution_mode (int)``: Mode of the execution.
    """
    
    states:list[STATE] = [s for s, v in STATE.__members__.items()] # type: ignore
    """ complete set of possible machine states. """
    
    def __init__(self, name:str, mode:AlgoMode, execution_mode:ExecutionMode):
        self.name = name
        self.mode = mode
        self.execution_mode = execution_mode
        self._paused:bool = False

        transitions = [
        {'trigger':'fsm_initialize','source':'STARTUP','dest':'INITIALIZED'},
        {'trigger':'fsm_before_trading_start','source':'HEARTBEAT','dest':'BEFORE_TRADING_START'},
        {'trigger':'fsm_before_trading_start','source':'INITIALIZED','dest':'BEFORE_TRADING_START'},
        {'trigger':'fsm_before_trading_start','source':'AFTER_TRADING_HOURS','dest':'BEFORE_TRADING_START'},
        {'trigger':'fsm_handle_data','source':'BEFORE_TRADING_START','dest':'TRADING_BAR'},
        {'trigger':'fsm_handle_data','source':'HEARTBEAT','dest':'TRADING_BAR'},
        {'trigger':'fsm_handle_data','source':'TRADING_BAR','dest':'TRADING_BAR'},
        {'trigger':'fsm_after_trading_hours','source':'TRADING_BAR','dest':'AFTER_TRADING_HOURS'},
        {'trigger':'fsm_after_trading_hours','source':'HEARTBEAT','dest':'AFTER_TRADING_HOURS'},
        {'trigger':'fsm_after_trading_hours','source':'INITIALIZED','dest':'AFTER_TRADING_HOURS'},
        {'trigger':'fsm_heartbeat','source':'AFTER_TRADING_HOURS','dest':'HEARTBEAT'},
        {'trigger':'fsm_heartbeat','source':'BEFORE_TRADING_START','dest':'HEARTBEAT'},
        {'trigger':'fsm_heartbeat','source':'INITIALIZED','dest':'HEARTBEAT'},
        {'trigger':'fsm_heartbeat','source':'HEARTBEAT','dest':'HEARTBEAT'},
        {'trigger':'fsm_heartbeat','source':'TRADING_BAR','dest':'HEARTBEAT'},
        {'trigger':'fsm_analyze','source':'*','dest':'STOPPED'},
        ]
        
        self.machine = Machine(model=self,
                               states=_AlgoStateMachine.states,
                               transitions=transitions,
                               initial="STARTUP")
        self.machine.add_transition('fsm_stop','*','STOPPED')
        
    
    def is_running(self) -> bool:
        """ returns True if we are in a running state """
        return not (self._paused or self.is_STOPPED()) # type: ignore
    
    def is_paused(self) -> bool:
        return self._paused
    
    def set_pause(self):
        """ set the machine state to pause """
        self._paused = True
        
    def reset_pause(self):
        """ un-pause the state of the machine """
        self._paused = False
            
    def reset_state(self):
        self.state = 'STARTUP'
        self._paused = False