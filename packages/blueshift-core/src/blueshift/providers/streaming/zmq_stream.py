from typing import Callable, Any
import gzip
import json
import zmq
import datetime

from blueshift.lib.common.functions import listlike
from blueshift.lib.common.types import Command
from blueshift.lib.common.ctx_mgrs import TimeoutRLock
from blueshift.lib.common.constants import CONNECTION_TIMEOUT
from blueshift.lib.exceptions import TerminationError, InitializationError
from blueshift.interfaces.streaming import (
    StreamingPublisher, StreamingSubscriber, CmdReqClient, CmdRepServer)

from blueshift.lib.serialize.json import (
        convert_nan, BlueshiftJSONEncoder)

class ZeroMQPublisher(StreamingPublisher):
    """
        |0MQ| publisher class. This is used to publish messages on a
        given ip/ port for streaming updates from the algorithm.

        Note:
            The messages are ``multi-part`` and encoded as UTF-8. The
            default end-of-transmission string is "EOM".

            see also :ref:`alerts configuration <zmqconf>`.

        Args:
            ``host (str)``: ip address.

            ``port (number)``: port to publish at.

            ``topic (str)``: topic of the message channel.

        .. |0MQ| raw:: html

           <a href="http://zeromq.org/" target="_blank">0MQ</a>
    """
    def __init__(self, host, port, topic, protocol="tcp", EOM = "EOM",
                 encoding = "utf-8", compression=0, bind=True):
        self._protocol = protocol
        self._addr = host
        self._port = port
        self._topic = topic
        self._EOM = EOM
        self._encoding = encoding
        self._encoded_topic = bytes(self._topic, self._encoding)
        self._encoded_EOM = bytes(self._EOM, self._encoding)

        self._socket = None
        self._context = None

        # compression option
        self._compression = compression
        
        # connect or bind flag
        self._bind_flag = bind

        # to support re-entrant context managers
        self._ref_count = 0
        # for thread safety
        self._socket_lock = TimeoutRLock(timeout=CONNECTION_TIMEOUT)
        
    def __str__(self):
        return f"ZMQPub[{self._addr}:{self._port}]"
    
    def __repr__(self):
        return self.__str__()

    def connect(self):
        """ bind to specified socket with re-entrant capability. """
        if self._ref_count > 0:
            self._ref_count += 1
            return
        
        try:
            with self._socket_lock:
                self._context = zmq.Context()
                self._socket = self._context.socket(zmq.PUB)
                conn_string = "%s://%s:%s" % (self._protocol,
                                              self._addr,
                                              self._port)
        
                self._socket.setsockopt(zmq.SNDHWM, 10)
                #self._socket.setsockopt(zmq.SNDBUF, 50*2048)
                
                if self._bind_flag:
                    self._socket.bind(conn_string)
                else:
                    self._socket.connect(conn_string)
        except zmq.ZMQError as e:
            msg = f'Publisher failed to bind: {str(e)}'
            raise InitializationError(msg)
        except TimeoutError:
            msg = f'Publisher failed to bind: timed out'
            raise InitializationError(msg)

        self._ref_count = self._ref_count + 1
        
    @property
    def socket(self):
        return self._socket
    
    @property
    def is_connected(self):
        # zmq connected concept is absent, we just check if we 
        # have a socket open
        return self._socket is not None

    def send(self, msg, ignore_nan=True, **kwargs):
        """ publish to specified socket. """
        if not self.is_connected:
            self.connect()
        
        if not isinstance(msg, str):
            try:
                if ignore_nan:
                    convert_nan(msg)
                msg = json.dumps(msg, cls=BlueshiftJSONEncoder)
            except Exception:
                msg = str(msg)

        msg = bytes(msg, self._encoding)

        if self._compression > 0:
            msg = gzip.compress(msg, level=max(9,self._compression)) # type: ignore
        
        with self._socket_lock:
            if self._socket:
                self._socket.send_multipart([self._encoded_topic, msg])
        
    def send_to_topic(self, topic, msg, ignore_nan=True, **kwargs):
        """ publish to specified topic. """
        if not self.is_connected:
            self.connect()

        try:
            if ignore_nan:
                convert_nan(msg)
            msg = json.dumps(msg, cls=BlueshiftJSONEncoder)
        except Exception:
            msg = str(msg)

        msg = bytes(msg, self._encoding)

        if self._compression > 0:
            msg = gzip.compress(msg, level=max(9,self._compression)) # type: ignore
            
        encoded_topic = bytes(topic, self._encoding)
        
        with self._socket_lock:
            if self._socket:
                self._socket.send_multipart([encoded_topic, msg])

    def close(self, forced=False):
        """ close connection and re-set 0MQ context. """
        self._ref_count = self._ref_count - 1

        if self._ref_count > 0 and not forced:
            return
        
        with self._socket_lock:
            if self._socket:
                self._socket.send_multipart([self._encoded_topic,
                                             self._encoded_EOM])
                self._socket.close()
            if self._context:
                self._context.term()
            self._context = self._socket = None

class ZeroMQSubscriber(StreamingSubscriber):
    """
        |0MQ| subscriber class. This is used to subscribe to messages on a
        given ip/ port for streaming updates from the algorithm. A program
        outside the current running algo can instantiate this class and
        capture the updates. Over-write the handle_msg method to implement
        custom handling.

        Note:
            The messages are ``multi-part`` and encoded as UTF-8. The
            default end-of-transmission string is "EOM".

        Args:
            ``host (str)``: ip address.

            ``port (number)``: port to publish at.

            ``topic (str)``: topic of the message channel.

    """
    def __init__(self, host, port, topic, protocol="tcp", EOM = "EOM",
                 no_block=False, encoding="utf-8", compression=0,
                 bind=False, handler:Callable[..., tuple[str, Any]]|None=None, timeout=None):
        if handler and not callable(handler):
            msg = f'Subscriber handler must be a callable, got {handler}.'
            raise InitializationError(msg)
        self._handler = handler
            
        self._protocol = protocol
        self._addr = host
        self._port = port
        self._EOM = EOM
        self._no_block = no_block
        self._encoding = encoding
        self._encoded_EOM = bytes(self._EOM, self._encoding)
        
        if not listlike(topic):
            topic = [topic]
        self._encoded_topic = set([])
        for t in topic:
            self._encoded_topic.add(bytes(t, self._encoding))
            
        self._timeout = None
        if timeout:
            self._timeout = int(timeout*1000) # in milliseconds

        self._compression = compression
        self._bind_flag = bind
        self._socket = None
        self._context = None
        
    def __str__(self):
        return f"ZMQPub[{self._addr}:{self._port}]"
    
    def __repr__(self):
        return self.__str__()
        
    @property
    def socket(self):
        return self._socket
    
    @property
    def is_connected(self):
        # zmq connected concept is absent, we just check if we 
        # have a socket open
        return self._socket is not None

    def connect(self):
        """ connect to specified socket. """
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.SUB)
        conn_string = "%s://%s:%s" % (self._protocol,
                                      self._addr,
                                      self._port)
        
        try:
            if self._bind_flag:
                self._socket.bind(conn_string)
            else:
                self._socket.connect(conn_string)
        except zmq.ZMQError as e:
            msg = f'Subscriber failed to connect: {str(e)}'
            raise InitializationError(msg)
            
        for t in self._encoded_topic:
            self._socket.setsockopt(zmq.SUBSCRIBE, t)
            
    def subscribe(self, topic):
        t = bytes(topic, self._encoding)
        self._encoded_topic.add(t)
        if self._socket:
            self._socket.setsockopt(zmq.SUBSCRIBE, t)
            
    def unsubscribe(self, topic):
        t = bytes(topic, self._encoding)
        if t in self._encoded_topic:
            self._encoded_topic.remove(t)
        if self._socket:
            self._socket.setsockopt(zmq.UNSUBSCRIBE, t)

    def recv(self, *args, **kwargs):
        """ receive next message from the specified socket and exit. """
        if not self._socket:
            return None, None
            
        if self._no_block:
            packet = self._socket.recv_multipart(zmq.NOBLOCK)
            #[topic, msg] = self._socket.recv_multipart(zmq.NOBLOCK)
        elif self._timeout is not None:
            events = self._socket.poll(self._timeout, zmq.POLLIN)
            if events:
                #[topic, msg] = self._socket.recv_multipart()
                packet = self._socket.recv_multipart()
            else:
                return None, None
        else:
            packet = self._socket.recv_multipart()
            
        try:
            topic, msg = packet
        except Exception:
            if len(packet) > 1:
                topic = packet[0]
                msg = packet[-1]
            else:
                return None, None
            
        topic = topic.decode(self._encoding)

        if self._compression > 0:
            msg = gzip.decompress(msg)
        msg = msg.decode(self._encoding)

        if self._handler:
            return self._handler(topic, msg, *args, **kwargs)

        return str(topic), msg

    def recv_all(self, *args, **kwargs):
        """ receive messages from the specified socket in a loop. """
        if not self._socket:
            return
        
        while True:
            if self._no_block:
                [topic, msg] = self._socket.recv_multipart(
                        zmq.NOBLOCK)
            else:
                [topic, msg] = self._socket.recv_multipart()

            topic = topic.decode(self._encoding)

            if self._compression > 0:
                msg = gzip.decompress(msg)
            msg = msg.decode(self._encoding)

            if msg == self._EOM:
                self.close()
                break
            else:
                self.handle_msg(topic, msg, *args, **kwargs)

    def close(self):
        """ close and clean up the connection. """
        if self._socket:
            self._socket.close()

        if self._context:
            self._context.term()

        self._context = self._socket = None

    def handle_msg(self, topic, msg, *args, **kwargs):
        if self._handler:
            return self._handler(topic, msg, *args, **kwargs)

class ZeroMQCmdRepServer(CmdRepServer):
    '''
        |0MQ| Reply socket server for receiving on the command channel
        and processing + forwarding the input. This should go in the
        algo class that receives and executes a command. Commands are
        interpreted as jasonified strings cast in to the Command type.

        Note:
            The command listener waits on the socket to receive commands
            with a timeout and ``DONTWAIT`` flag on. If no commands is
            received within that time window, an exception is thrown, and
            caught to continue on the next waiting cycle.

            see also :ref:`alerts configuration <zmqconf>`.

        Args:
            ``host (str)``: ip address.

            ``port (number)``: port to publish at.
            
            ``handler (callable)``: Callable to handle input request.

            ``no_block (bool)``: ignored.
    '''
    def __init__(self, host, port, protocol="tcp", handler=None, 
                 no_block=True, bind=True, timeout=10, EOM = "EOM"):
        self._protocol = protocol
        self._addr = host
        self._port = port
        self._handler = handler or self._default_handler
        self._no_block = no_block
        self._bind_flag = bind
        self._timeout = timeout*1000
        self._EOM = EOM
        
    def __str__(self):
        return f"ZMQRep[{self._addr}:{self._port}]"
    
    def __repr__(self):
        return self.__str__()

    def connect(self, timeout=None):
        """ create and bind to the socket. """
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.REP)
        conn_string = "%s://%s:%s" % (self._protocol,
                                      self._addr,
                                      self._port)
        
        timeout = timeout or self._timeout
        if timeout:
            try:
                timeout = int(timeout)
                self._timeout = timeout
                self._socket.setsockopt(zmq.LINGER, 0)
                #self._socket.setsockopt(zmq.RCVTIMEO, timeout)
                self._socket.setsockopt(zmq.SNDTIMEO, timeout)
            except Exception:
                pass
            
        try:
            if self._bind_flag:
                self._socket.bind(conn_string)
            else:
                self._socket.connect(conn_string)
        except zmq.ZMQError as e:
            msg = f'Rep server failed to bind: {str(e)}'
            raise InitializationError(msg)

    def _validate_auth(self, *args, **kwargs):
        """ validation method to be implemented here. """
        return True
    
    def _default_handler(self, cmd):
        if not self._validate_auth(cmd):
            msg = {"status":"Error","results":str(cmd)}
        else:
            msg = {"status":"Ok","results":str(cmd)}

        return msg

    def process_command(self):
        """ waits on the socket and receive the next command. this is non-blocking. """
        if not self._socket:
            return self._EOM
        
        try:
            if self._no_block:
                cmd = self._socket.recv_string(flags=zmq.NOBLOCK)
            else:
                cmd = self._socket.recv_string()
            
            msg = self._handler(cmd)
            if isinstance(msg, dict):
                msg = json.dumps(msg)
            elif not isinstance(msg, str):
                msg = str(msg)
            
            if self._no_block:
                self._socket.send_string(str(msg),flags=zmq.DONTWAIT)
            else:
                self._socket.send_string(str(msg))
                
            # if end of message received, return it to the caller to 
            # signify the socket should be closed now.
            if cmd == self._EOM:
                return self._EOM
        except zmq.error.Again:
            pass
        except Exception as e:
            if isinstance(e, TerminationError):
                raise
            # illegal json, commandas etc. We do not want to fail algo.
            msg = {"status":"Error","results": str(e)}
            msg = json.dumps(msg)
            
            try:
                if self._no_block:
                    self._socket.send_string(str(msg),flags=zmq.DONTWAIT)
                else:
                    self._socket.send_string(str(msg))
            except Exception:
                pass

    def close(self):
        """ close and clean up the connection. """
        if self._socket:
            self._socket.close()

        if self._context:
            self._context.term()

        self._context = self._socket = None

class ZeroMQCmdReqClient(CmdReqClient):
    '''
        |0MQ| Request socket client for sending commands on the command
        channel. Commands are of Command type, sent as jasonified
        strings. This should be part of the system controlling the
        running algo, locally or from a remote machine.

        Args:
            ``host (str)``: ip address.

            ``port (number)``: port to publish at.

            ``no_block (bool)``: ignored.
    '''
    def __init__(self, host, port, protocol="tcp", no_block=False,
                 bind=False):
        self._protocol = protocol
        self._addr = host
        self._port = port
        self._no_block = no_block
        self._timeout = None
        self._bind_flag = bind
        self._socket = None
        self._context = None
        self._socket_lock = TimeoutRLock(timeout=CONNECTION_TIMEOUT)
        
    def __str__(self):
        return f"ZMQReq[{self._addr}:{self._port}]"
    
    def __repr__(self):
        return self.__str__()

    def connect(self, timeout=None):
        """ connect to the socket. """
        with self._socket_lock:
            self._context = zmq.Context()
            self._socket = self._context.socket(zmq.REQ)

            if timeout:
                try:
                    timeout = int(timeout)
                    self._timeout = timeout
                    self._socket.setsockopt(zmq.LINGER, 1)
                    self._socket.setsockopt(zmq.RCVTIMEO, timeout)
                    self._socket.setsockopt(zmq.SNDTIMEO, timeout)
                except Exception:
                    pass

            conn_string = "%s://%s:%s" % (self._protocol,
                                        self._addr,
                                        self._port)
            try:
                if self._bind_flag:
                    self._socket.bind(conn_string)
                else:
                    self._socket.connect(conn_string)
            except zmq.ZMQError as e:
                msg = f'Req client failed to connect: {str(e)}'
                raise InitializationError(msg)

    def send_command(self, strcmd=None):
        """ send a command. current reads from the input device (stdin).
            command name is a name of a function that the algorithm can
            run (part of command API). Args a comma-separated list of
            arguments. Kwargs is a list of comma-separated keyword=value
            pairs.
        """
        with self._socket_lock:
            if self._socket is None:
                # trigger a auto-connect on send
                self.connect(timeout=self._timeout)
                
            if strcmd is None:
                cmd = str(input("enter a command:") or "continue") #nosec
                args = json.dumps(input("enter arguments list (json):")) #nosec
                kwargs = json.dumps(input("enter keyword arguments list(json):")) #nosec
                cmd = Command(cmd,args,kwargs)
                strcmd = json.dumps(cmd._asdict())

            return self.dispatch(strcmd)

    def dispatch(self, msg):
        if not self._socket:
            return
        
        self._socket.send_string(msg, zmq.NOBLOCK)
        poller = zmq.Poller()
        poller.register(self._socket, zmq.POLLIN)
        if poller.poll(self._timeout):
            msg = self._socket.recv()
            msg = msg.decode("utf-8")
            return msg

        # we timed out, make sure the socket is in correct state on next send
        # on next send, we automatically "connect" if required.
        self.close()
        return None

    def close(self):
        """ close and clean up the connection. """
        if self._socket:
            self._socket.close()

        if self._context:
            self._context.term()

        self._context = self._socket = None

