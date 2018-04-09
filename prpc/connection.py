#
# coding: utf-8
# Copyright (c) 2018 DATADVANCE
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

"""
Connection implementation.
"""

import asyncio
import logging
import uuid

from . import calls, exceptions, method_locator, platform, rpc_response, utils
from .protocol import constants, messages


class Connection(object):
    """Single point-to-point RPC connection.

    Implements a transport independent protocol.

    For server usage, see :func:`~prpc.connection.Connection.accept`.

    For client usage, see :func:`~prpc.connection.Connection.connect`.

    Args:
        methods: Methods to expose through RPC; may be
            :class:`~prpc.method_locator.AbstractMethodLocator` instance,
            a valid input for :class:`~prpc.method_locator.TreeMethodLocator`
            or None.
        loop: asyncio event loop to use (asyncio.get_eventloop() by default).
        logger: Logger instance.
        serve_traceback: Toggle sending full tracebacks on RPC exceptions.
        attached_data: Custom data (dict or list of pairs) to store
            in connection 'data' property.
        debug: Adds some excessive logging and checking.

    Note:
        Debug flag enables per-call messages. Without it, Connection will still
        write out some 'DEBUG' level messages, but only for event that
        are independent on the total number of calls.
    """
    DEFAULT_LOG_NAME = "prpc.Connection"

    def __init__(self, methods=None, loop=None,
                 logger=None, serve_traceback=True,
                 attached_data=None, debug=False):
        """Initialize new connection instance."""
        self._loop = loop or asyncio.get_event_loop()
        self._log = logger or logging.getLogger(self.DEFAULT_LOG_NAME)
        self._listen_task = None
        self._accept_timeout_handle = None
        self._on_close = utils.Signal()
        self._attached_data = {}

        if attached_data is not None:
            self._attached_data.update(attached_data)

        self._socket = None
        self._socket_factory = None
        self._mode = constants.ConnectionMode.NEW
        self._state = constants.ConnectionState.NEW
        self._id = None

        self._current_req_id = 0
        self._outgoing_calls = {}
        self._incoming_calls = {}
        self._send_lock = asyncio.Lock(loop=loop)

        if (isinstance(methods, method_locator.AbstractMethodLocator) or
            methods is None):
            self._method_locator = methods
        else:
            self._method_locator = method_locator.TreeMethodLocator(methods)

        self._connect_callback = None
        self._handshake_data = None
        self._remote_handshake = None

        self._serve_traceback = serve_traceback
        self._debug = debug

        self._handlers = {
            constants.MessageType.REQUEST_HANDSHAKE:
                self._handle_request_handshake,
            constants.MessageType.REQUEST_CALL_START:
                self._handle_request_call_start,
            constants.MessageType.REQUEST_CALL_CANCEL:
                self._handle_request_call_cancel,
            constants.MessageType.REQUEST_STREAM_MESSAGE:
                self._handle_request_stream_message,
            constants.MessageType.REQUEST_STREAM_CLOSE:
                self._handle_request_stream_close,
            constants.MessageType.RESPONSE_HANDSHAKE:
                self._handle_response_handshake,
            constants.MessageType.RESPONSE_RESULT:
                self._handle_response_result,
            constants.MessageType.RESPONSE_STREAM_MESSAGE:
                self._handle_response_stream_message,
            constants.MessageType.RESPONSE_STREAM_CLOSE:
                self._handle_response_stream_close
        }

        self._exception_by_status = {
            constants.ResponseStatus.SUCCESS: None,
            constants.ResponseStatus.METHOD_ERROR: exceptions.RpcMethodError,
            constants.ResponseStatus.METHOD_NOT_FOUND:
                exceptions.RpcMethodNotFoundError,
            constants.ResponseStatus.SERVER_ERROR: exceptions.RpcServerError
        }

        self._call_type_classes = {
            constants.CallType.HANDSHAKE: calls.Handshake,
            constants.CallType.UNARY: calls.UnaryCall,
            constants.CallType.ISTREAM: calls.IStreamCall,
            constants.CallType.OSTREAM: calls.OStreamCall,
            constants.CallType.BISTREAM: calls.BiStreamCall
        }

    @property
    def state(self):
        """Get connection current state."""
        return self._state

    @property
    def connected(self):
        """Check if connection is operational."""
        return self._state == constants.ConnectionState.CONNECTED

    @property
    def active(self):
        """Check if connection has any active calls."""
        if not self.connected:
            return False
        return bool(self._incoming_calls) or bool(self._outgoing_calls)

    @property
    def mode(self):
        """Get connection mode."""
        return self._mode

    @property
    def id(self):
        """Get connection id."""
        return self._id

    @property
    def handshake_data(self):
        """Get handshake data received from peer."""
        return self._remote_handshake.user_data

    @property
    def on_close(self):
        """Signal emitted on close."""
        return self._on_close

    @property
    def locator(self):
        """Get method locator instance. May be None."""
        return self._method_locator

    @property
    def loop(self):
        """Get the event loop instance used by this connection."""
        return self._loop

    @property
    def data(self):
        """Custom dict to store attached data."""
        return self._attached_data

    async def connect(self, socket_factory, handshake_data=None,
                      connect_callback=None, timeout=None):
        """Attempt to connect to peer in client mode.

        Args:
            socket_factory: Async callable returning socket
                            compatible with platform.MessageSocket interface.
            handshake_data: Custom data to embed into handshake message.
            connect_callback: Sync or async callable to validate
                incoming handshake.
            timeout: Connection timeout (number or None).

        Note:
            If 'timeout' is None, connection uses a default value
            (see protocol.constants). To disable timeout altogether,
            pass a negative timeout value.

        Note:
            *.connect(...)* does not support async context manager
            interface for now. Amount of hacks required to allow both
            'await connection.connect' and 'async with connection.connect'
            is unpleasant and imposes restrictions on
            :mod:`~prpc.platform` modules.
        """
        assert self._state in (constants.ConnectionState.NEW,
                               constants.ConnectionState.CLOSED)
        self._socket_factory = socket_factory
        self._mode = constants.ConnectionMode.CLIENT

        self._socket = None
        self._connect_callback = connect_callback
        self._handshake_data = handshake_data

        self._socket = await self._socket_factory()
        self._listen_task = self._loop.create_task(self._listen())
        self._state = constants.ConnectionState.PENDING
        try:
            # Reuse timeout code from 'public API'.
            #
            # It's not too convenient, but moreorless clear
            # (so that mumbo-jumbo with 'timing' out so that no 'unread' futures
            #  is concentrated in the _outgoing_call machinery)
            start_handshake = self._outgoing_call_factory(
                constants.CallType.HANDSHAKE,
                None,
                messages.HandshakeData(
                    constants.PROTOCOL_VERSION, None, self._handshake_data
                ),
                None,
                timeout=self._get_timeout_value(
                    timeout, constants.HANDSHAKE_TIMEOUT
                )
            )
            started = self._loop.create_future()
            finished = self._loop.create_future()

            self._loop.create_task(start_handshake(started, finished))
            await started

            remote_handshake = None
            try:
                remote_handshake = await finished
            except exceptions.RpcCallTimeoutError:
                raise exceptions.RpcConnectionTimeoutError("connect timed out")

            # Store connection id before calling connect_callback.
            self._id = remote_handshake.id
            # Motivation: rejecting outside of connect is 'too late'
            # as untrusted server could have send some call already
            # (between 'await connect' and handshake validation code).
            #
            # Ofc it's not an actual security measure (TLS should do a
            # better job), but more of a consistency guard (so that mistaken
            # but not malicious peers won't do unexpected things).
            if self._connect_callback:
                try:
                    # Run a user accept callback,
                    # terminating the connection if it raises.
                    await utils.asyncio.call_sync_async(
                        self._connect_callback,
                        self, remote_handshake.user_data
                    )
                except Exception as ex:
                    raise exceptions.RpcConnectionRejectedError(
                        "connection rejected by connect callback"
                    ) from ex

            self._remote_handshake = remote_handshake
            self._state = constants.ConnectionState.CONNECTED
            self._log.info("Connection established (client mode)")
        except Exception:
            self._id = None
            await self._close()
            raise

    async def accept(self, socket, handshake_data=None,
                     connect_callback=None, timeout=None):
        """Accept connection in a server mode.

        Args:
            socket: Async socket implementing
                with platform.MessageSocket interface.
            handshake_data: Custom data to embed into handshake message.
            connect_callback: Sync or async callable
                to validate incoming handshake.
            timeout: Connection timeout (non-negative number or None).

        Note:
            If 'timeout' is None, connection uses a default value
            (see protocol.constants).
            To disable timeout altogether, pass a negative timeout value.
        """
        self._socket = socket
        self._mode = constants.ConnectionMode.SERVER
        self._state = constants.ConnectionState.PENDING
        self._id = uuid.uuid4().hex

        self._connect_callback = connect_callback
        self._handshake_data = handshake_data

        self._listen_task = self._loop.create_task(self._listen())
        timeout = self._get_timeout_value(timeout, constants.HANDSHAKE_TIMEOUT)
        if timeout is not None:
            # loop.call_later cannot be used to schedule coroutines.
            async def scheduled_close():
                await asyncio.sleep(timeout)
                await self._close(False)
            self._accept_timeout_handle = self._loop.create_task(
                scheduled_close()
            )
        return await self._listen_task

    async def close(self):
        """Close the connection and interrupt any active calls."""
        await self._close(True)

    async def call_simple(self, method, *args, **kwargs):
        """Run unary RPC call with simplified signature.

        Note:
            For advanced APIs like call cancellation use call_unary()

        Args:
            method: Remote method name.
            *args: Positional arguments (expanded).
            **kwargs: Keyword arguments (expanded).

        Returns:
            Call return value.
        """
        async with self.call_unary(method, args, kwargs) as call:
            result = await call.result
        return result

    def call_unary(self, method, args=None, kwargs=None, timeout=None):
        """Run unary RPC call.

        Args:
            method: Remote method name.
            args: Positional arguments as a list/tuple.
            kwargs: Keyword arguments as a dict.
            timeout: Optional timeout value in seconds (int or float).

        Returns:
            :class:`~prpc.rpc_response.RpcResponse` instance.
        """
        return self._outgoing_call(
            constants.CallType.UNARY, method, args, kwargs, timeout
        )

    def call_istream(self, method, args=None, kwargs=None, timeout=None):
        """Run istream RPC call (e.g. file download).

        Args:
            method: Remote method name.
            args: Positional arguments as a list/tuple.
            kwargs: Keyword arguments as a dict.
            timeout: Optional timeout value in seconds (int or float).

        Returns:
            :class:`~prpc.rpc_response.RpcResponse` instance.
        """
        return self._outgoing_call(
            constants.CallType.ISTREAM, method, args, kwargs, timeout
        )

    def call_ostream(self, method, args=None, kwargs=None, timeout=None):
        """Run ostream RPC call (e.g. file upload).

        Args:
            method: Remote method name.
            args: Positional arguments as a list/tuple.
            kwargs: Keyword arguments as a dict.
            timeout: Optional timeout value in seconds (int or float).

        Returns:
            :class:`~prpc.rpc_response.RpcResponse` instance.
        """
        return self._outgoing_call(
            constants.CallType.OSTREAM, method, args, kwargs, timeout
        )

    def call_bistream(self, method, args=None, kwargs=None, timeout=None):
        """Run bistream RPC call (e.g. subprotocol).

        Args:
            method: Remote method name.
            args: Positional arguments as a list/tuple.
            kwargs: Keyword arguments as a dict.
            timeout: Optional timeout value in seconds (int or float).

        Returns:
            :class:`~prpc.rpc_response.RpcResponse` instance.
        """
        return self._outgoing_call(
            constants.CallType.BISTREAM, method, args, kwargs, timeout
        )

    async def _listen(self):
        """Listen for incoming messages.

        Note:
            Should be executed as a separate task in most cases.
        """
        # 'Exit status' checks are very rudimental for now.
        # Will get important if reconnect implementation is desired.
        closed_cleanly = False
        # Store socket instance, as close() operation can reset it to None.
        socket = self._socket
        try:
            async for msg_type, payload in socket:
                if msg_type == platform.MessageType.ERROR:
                    raise exceptions.RpcLocalError('transport error')
                if msg_type != platform.MessageType.BINARY:
                    raise exceptions.RpcLocalError(
                        'invalid message payload type, '
                        'only binary messages are expected'
                    )

                decoded = messages.ProtocolMessage.from_bytes(payload)
                if not self._is_acceptable_message(decoded.MESSAGE_TYPE):
                    self._log.warning(
                        'unexpected message dropped '
                        '(connection status: %s, message: %s)',
                        self._state.name, decoded.MESSAGE_TYPE.name
                    )
                    continue
                await self._handlers[decoded.MESSAGE_TYPE](decoded)
            closed_cleanly = socket.close_code == platform.CLOSE_CODE_SUCCESS
        except Exception:
            self._log.exception('Listen loop interrupted by exception')
        self._log.debug(
            'Exiting listen loop. Socket status: %s, close code: %s',
            'closed' if socket.closed else 'open',
            socket.close_code
        )
        await self._close(closed_cleanly)
        return closed_cleanly

    def _outgoing_call(self, call_type, method, args, kwargs, timeout):
        """Initiate an outgoing call.

        Args:
            call_type: Call type (constants.CallType).
            method: Remote method name.
            args: Positional args as a list/tuple.
            kwargs: Keyword args as a dict.
            timeout: Optional timeout value in seconds.

        Returns:
            :class:`~prpc.rpc_response.RpcResponse` instance.
        """
        self._debug_log(
            'Creating %s call \'%s\'' % (call_type.name.lower(), method,)
        )
        call_impl = self._outgoing_call_factory(
            call_type, method, args, kwargs, timeout
        )
        return rpc_response.RpcResponse(
            call_impl, self._send_message, self._loop
        )

    def _outgoing_call_factory(self, call_type, method, args, kwargs, timeout):
        """Create an async function to run a call.

        Args:
            call_type: Call type (constants.CallType).
            method: Remote method name.
            args: Positional args as a list/tuple.
            kwargs: Keyword args as a dict.
            timeout: Optional timeout value in seconds.

        Returns:
            Async callable, signature
            (started: asyncio.Future, finished: asyncio.Future) -> NoneType.
        """
        assert call_type in constants.CallType
        if call_type != constants.CallType.HANDSHAKE:
            assert isinstance(method, str)
        timeout = self._get_timeout_value(timeout)
        args = args or []
        kwargs = kwargs or {}

        async def outgoing_call(started, finished):
            if call_type != constants.CallType.HANDSHAKE:
                self._require_state(constants.ConnectionState.CONNECTED)
            else:
                self._require_state(constants.ConnectionState.PENDING)
            try:
                call = self._create_outgoing_call(
                    call_type, method, args, kwargs)
                with call:
                    await call.send(self._send_message)
                    # Notify API that call is started (see RpcResponse).
                    started.set_result(call)
                    # Use wait instead of wait_for so we don't actually
                    # retrieve the result (as it can raise).
                    _, pending = await asyncio.wait(
                        [call.result],
                        timeout=timeout,
                        loop=self._loop
                    )
                    # Can only happen when timeout is not None.
                    if pending:
                        assert timeout is not None
                        await call.cancel(
                            self._send_message,
                            exceptions.RpcCallTimeoutError(
                                'call %s (\'%s\') timed out (timeout=%fs)' %
                                (call.id, method, timeout,)
                            )
                        )
                        # .result should resolve immediately on cancel()
                        assert call.result.done()
                    # This future is visible to client and may be unexpectedly
                    # cancelled (e.g. if the task performing this outgoing call
                    # is cancelled).
                    if not finished.cancelled():
                        # Forward the result to the 'public API'.
                        if call.result.exception():
                            finished.set_exception(call.result.exception())
                        else:
                            finished.set_result(call.result.result())
                    else:
                        # Ignore 'result not retrieved' error.
                        call.result.exception()
            finally:
                # Catch-all. Shoudn't get there, but sometimes we just do,
                # so we need to avoid deadlocks.
                if not started.done():
                    started.set_exception(
                        exceptions.finally_error(
                            'failed to initialize outgoing call'
                        )
                    )
                elif not finished.done():
                    finished.set_exception(
                        exceptions.finally_error(
                            'failed to finalize outgoing call'
                        )
                    )
                # Don't raise the error (if any),
                # it's already forwarded to caller.
                return
        return outgoing_call

    async def _send_message(self, msg):
        """Serialize a ProtocolMessage and pass it to the trasport layer."""
        # Encode message before obtaining a write lock
        # (so the lock duration is shorter).
        dgram = msg.to_bytes()
        async with self._send_lock:
            await self._socket.send(dgram)

    async def _incoming_call(self, msg, started):
        """Incoming call handler (accepts all call types except HANDSHAKE).

        Note:
            The 'started' solution may look ugly, but it solves a hard sync
            problem with a small amount of code. SeemsGood.

        Args:
            msg: messages.ProtocolMessage instance.
            started: asyncio.Future that will be resolved
                when it is safe to unblock listen loop.
        """
        result = None
        status = constants.ResponseStatus.SERVER_ERROR
        exception = None
        try:
            # Handshake call shoudn't get there.
            assert msg.call_type != constants.CallType.HANDSHAKE
            with self._create_incoming_call(
                msg.call_type, msg.id, msg.method, msg.args, msg.kwargs
            ) as call:
                self._debug_log('Incoming call: %s', call)
                await call.accept(self._send_message)
                started.set_result(call)
                handler = self._get_rpc_method(msg.call_type, msg.method)
                result = await call.invoke(handler, self, self._method_locator)
                # Pack the return value immediately to ensure that it is
                # serializable.
                try:
                    result = messages.pack(result)
                except TypeError as ex:
                    result = None
                    raise exceptions.RpcMethodError.wrap_exception(
                        'method return value cannot be serialized', ex
                    )
                status = constants.ResponseStatus.SUCCESS
        except exceptions.RpcConnectionClosedError:
            # Connection is already closed, response is impossible.
            #
            # Not a self._debug_log as this should happen only on connection
            # close, and does not 'scale' with total number of requests.
            self._log.debug(
                'Call %s (\'%s\') interrupted (connection closed)',
                msg.id, msg.method
            )
            return
        except exceptions.RpcCancelledError:
            # Client doesn't expect response there.
            self._debug_log(
                'Call %s (\'%s\') cancelled by remote peer', msg.id, msg.method
            )
            return
        except exceptions.RpcMethodError as ex:
            self._debug_log(
                'Call %s (\'%s\') raised an exception',
                msg.id, msg.method, exc_info=True
            )
            exception = ex
            status = constants.ResponseStatus.METHOD_ERROR
        except exceptions.RpcMethodNotFoundError as ex:
            exception = ex
            status = constants.ResponseStatus.METHOD_NOT_FOUND
        except Exception as ex:
            self._debug_log(
                'Unexpected exception while handling call %s (\'%s\')',
                msg.id, msg.method, exc_info=True
            )
            exception = ex
            status = constants.ResponseStatus.SERVER_ERROR
        finally:
            # Catch-all failure branch, ugly, but required for unexpected errors
            # (e.g. in _create_incoming_call in case of duplicate request id).
            #
            # Failing is always (?) better than hanging.
            if not started.done():
                started.set_exception(
                    exceptions.finally_error('failed to accept call')
                )
                # Don't raise the error (if any),
                # it's already forwarded to caller.
                return

        self._debug_log(
            'Call %s (\'%s\') finished, status: %s',
            msg.id, msg.method, status.name
        )
        response = messages.ResponseResult(
            msg.id, result, status, self._error_tuple(exception)
        )
        await self._send_message(response)

    def _create_outgoing_call(self, call_type, method, args, kwargs):
        """Create an outgoing call instance. See 'calls' module for details.

        Note:
          Automatically generates a new call id.
        """
        cls = self._call_type_classes[call_type]
        assert issubclass(cls, calls.Call)
        request_id = self._next_request_id()
        return cls(
            request_id, self._loop,
            method, args, kwargs,
            self._outgoing_calls
        )

    def _create_incoming_call(self, call_type, request_id,
                              method, args, kwargs):
        """Create an incoming call instance. See 'calls' module for details."""
        cls = self._call_type_classes[call_type]
        assert issubclass(cls, calls.Call)
        return cls(
            request_id, self._loop,
            method, args, kwargs,
            self._incoming_calls
        )

    def _get_rpc_method(self, call_type, method):
        """Find and return an RPC method to call."""
        if self._method_locator is None:
            raise exceptions.RpcMethodNotFoundError(
                'connection has no available methods'
            )
        try:
            handler = self._method_locator.resolve(method, call_type, self)
        except Exception as ex:
            raise exceptions.RpcMethodNotFoundError.wrap_exception(
                'method \'%s\' is not found' % (method,), ex
            ) from ex
        if not callable(handler):
            raise exceptions.RpcServerError(
                'method \'%s\' is not callable' % (method,)
            )
        return handler

    async def _close(self, success=False):
        """Close the connection and cleanup all ongoing calls."""
        # Repeated .close should be ignored.
        if (self._state != constants.ConnectionState.PENDING and
            self._state != constants.ConnectionState.CONNECTED):
            return

        if success:
            close_code = platform.CLOSE_CODE_SUCCESS
        else:
            close_code = platform.CLOSE_CODE_ERROR
        self._state = constants.ConnectionState.CLOSING
        for call in self._outgoing_calls.values():
            call.close(
                exceptions.RpcConnectionClosedError('connection closed')
            )
        for call in self._incoming_calls.values():
            call.close(
                exceptions.RpcConnectionClosedError('connection closed')
            )
        if self._socket and not self._socket.closed:
            await self._socket.close(code=close_code)
        self._state = constants.ConnectionState.CLOSED
        self._listen_task = None
        if self._accept_timeout_handle:
            self._accept_timeout_handle.cancel()
            self._accept_timeout_handle = None
        self._socket = None
        self._socket_factory = None
        self._connect_callback = None
        try:
            await self._on_close.send(self)
        except Exception:
            self._log.exception('on_close callback raised an exception')

    def _require_state(self, state):
        """Ensure proper connection state.

        Args:
          state: desired connection state
        """
        assert self._state in constants.ConnectionState
        assert state in constants.ConnectionState
        if self._state != state:
            raise exceptions.RpcConnectionClosedError(
                'invalid connection status: expected %s, current %s'
                % (state.name, self._state.name)
            )

    def _is_acceptable_message(self, message_type):
        """Validate message against current connection state.

        Args:
          message_type: message

        Returns:
          True if message can be accepted
        """
        # Connection mode should be already known.
        assert self._mode != constants.ConnectionMode.NEW
        # Incorrect messages shoudn't even be deserialized.
        assert message_type in constants.MessageType
        if self._state == constants.ConnectionState.PENDING:
            if self._mode == constants.ConnectionMode.SERVER:
                return message_type == constants.MessageType.REQUEST_HANDSHAKE
            else:
                return message_type == constants.MessageType.RESPONSE_HANDSHAKE
        if self._state == constants.ConnectionState.CONNECTED:
            return (message_type != constants.MessageType.RESPONSE_HANDSHAKE and
                    message_type != constants.MessageType.REQUEST_HANDSHAKE)
        return False

    def _error_tuple(self, exception):
        """Encode exception to message format."""
        if exception is None:
            return None
        try:
            error_message = str(exception)
        except Exception:
            error_message = (
                '<unserializable error %s>' % (type(exception).__name__,)
            )
        if isinstance(exception, exceptions.RpcRemoteError):
            cause_type = exception.cause_type
            cause_message = exception.cause_message
        else:
            cause_type = None
            cause_message = None
        error_trace = None
        if self._serve_traceback:
            error_trace = exceptions.format_traceback_string(exception)
        return error_message, cause_type, cause_message, error_trace

    def _extract_error(self, msg):
        """Extract an exception from protocol message."""
        try:
            cls = self._exception_by_status.get(
                msg.status, exceptions.RpcUnknownError)
            if cls is None:
                return None
            assert issubclass(cls, exceptions.RpcRemoteError)
            error_message = msg.error.message or '<no error message>'
            cause_type = msg.error.cause_type or ''
            cause_message = msg.error.cause_message or ''
            exception = cls(error_message, cause_type,
                            cause_message, msg.error.trace)
        except Exception as ex:
            exception = exceptions.RpcLocalError(
                'unable to decode remote exception'
            ).with_traceback(ex.__traceback__)
        return exception

    def _next_request_id(self):
        """Get the next outgoing request id."""
        # For now, let's use simple conuter.
        #
        # Naturally, that requires keeping incoming/outgoing calls strictly
        # separated. We should never expect ids to be unique across
        # both collections.
        #
        # This way we can ignore both any kind of 'counter' syncronization and
        # ugly GUID strings.
        self._current_req_id += 1
        return self._current_req_id

    def _get_timeout_value(self, value, default=None):
        """Get timeout value with default.

        Args:
          value: user-defined timeout value
          default: value to use if 'value' is None

        Returns:
          effective timeout value
        """
        assert isinstance(value, (int, float, type(None)))
        if value is None:
            return default
        if value < 0:
            return None
        return value

    def _debug_log(self, *args, **kwargs):
        """Conditional logging for debug-only messages."""
        if self._debug:
            self._log.debug(*args, **kwargs)

    async def _handle_request_handshake(self, message):
        """Message handler for REQUEST_HANDSHAKE messages."""
        self._debug_log('Handling incoming handshake')
        self._require_state(constants.ConnectionState.PENDING)
        remote_handshake = message.args
        # Check protocol version.
        # Theoretically, we've already did using WS subprotocols,
        # but it's done outside of the protocol implementation.
        # (connection establishment is delegated to the client for now)
        #
        # Also, as a development, WS protocols can be used
        # to mark incompatible versions, while in-protocol
        # handshake can identify some minor features.
        if constants.PROTOCOL_VERSION != remote_handshake.protocol_version:
            raise exceptions.RpcLocalError(
                'incompatible protocol version (server: %s, sender: %s)' % (
                    constants.PROTOCOL_VERSION,
                    remote_handshake.protocol_version
                )
            )
        try:
            # Run a user accept callback, terminating
            # the connection if it raises.
            if self._connect_callback:
                await utils.asyncio.call_sync_async(
                    self._connect_callback,
                    self, remote_handshake.user_data
                )
            self._remote_handshake = remote_handshake
        except Exception as ex:
            raise exceptions.RpcConnectionRejectedError(
                'connection rejected by connect callback'
            ) from ex
        # TODO: test a case with a lengthy accept callback
        # so that client timeouts/disconnects by this point
        self._state = constants.ConnectionState.CONNECTED
        self._log.info('Connection established (server mode)')
        if self._accept_timeout_handle is not None:
            self._accept_timeout_handle.cancel()
            self._accept_timeout_handle = None
        response = messages.ResponseHandshake(
            message.id, messages.HandshakeData(
                constants.PROTOCOL_VERSION, self._id, self._handshake_data
            ),
            constants.ResponseStatus.SUCCESS, self._error_tuple(None)
        )
        await self._send_message(response)

    async def _handle_request_call_start(self, msg):
        """Message handler for REQUEST_CALL_START messages."""
        # We must block the listen loop until the call is registered
        # so we can dispatch 'stream_message' events properly.
        call_started = self._loop.create_future()
        self._loop.create_task(self._incoming_call(msg, call_started))
        await call_started

    async def _handle_request_call_cancel(self, msg):
        """Message handler for REQUEST_CALL_CANCEL messages"""
        call = self._incoming_calls.get(msg.id)
        if call is None:
            self._debug_log(
                'Dropped cancel message for unregistered incoming call %s',
                msg.id
            )
            return
        call.cancelled()

    async def _handle_request_stream_message(self, msg):
        """Message handler for REQUEST_STREAM_MESSAGE messages."""
        call = self._incoming_calls.get(msg.id)
        if call is None:
            self._debug_log(
                'Dropped stream message for unregistered incoming call %s',
                msg.id
            )
            return
        if call.stream.is_closed:
            self._debug_log(
                'Dropped stream message for closed incoming call %s', msg.id
            )
            return
        # Ensure that that the call type is right.
        # It's checked with nice exceptions in the send code, so there it's
        # just an assert.
        assert call.stream is not None
        assert call.stream.is_readable
        if not await call.stream.feed(
            msg.data, timeout=constants.STREAM_READ_TIMEOUT
        ):
            # Will be received by caller as ServerError. Seems fine,
            # adding separate response status for this 'kill switch'
            # looks excessive.
            call.cancelled(
                exceptions.RpcStreamTimeoutError(
                    'callee stream buffer is full'
                )
            )

    async def _handle_request_stream_close(self, msg):
        """Message handler for REQUEST_STREAM_CLOSE messages."""
        call = self._incoming_calls.get(msg.id)
        if call is None:
            self._debug_log(
                'Dropped stream close for unregistered incoming call %s', msg.id
            )
            return
        assert call.stream is not None
        call.stream.close_sync()

    async def _handle_response_handshake(self, msg):
        """Message handler for RESPONSE_HANDSHAKE messages."""
        call = self._outgoing_calls.get(msg.id)
        assert call is not None
        error = self._extract_error(msg)
        if error:
            call.set_exception(error)
        else:
            call.set_result(msg.result)

    async def _handle_response_result(self, msg):
        """Message handler for RESPONSE_RESULT messages."""
        call = self._outgoing_calls.get(msg.id)
        if call is None:
            self._debug_log(
                'Dropped result for unregistred outgoing call %s', msg.id)
            return
        error = self._extract_error(msg)
        if error:
            call.set_exception(error)
        else:
            # Call responses are double-packed.
            result = messages.unpack(msg.result)
            call.set_result(result)

    async def _handle_response_stream_message(self, msg):
        """Message handler for RESPONSE_STREAM_MESSAGE messages."""
        call = self._outgoing_calls.get(msg.id)
        if call is None:
            self._debug_log(
                'Dropped stream message for unregistred outgoing call %s',
                msg.id
            )
            return
        if call.stream.is_closed:
            self._debug_log(
                'Dropped stream message for closed outgoing call %s',
                msg.id
            )
            return
        # Ensure that that the call type is right.
        # It's checked with nice exceptions in the send code, so there it's
        # just an assert.
        assert call.stream is not None
        assert call.stream.is_readable
        if not await call.stream.feed(
            msg.data, timeout=constants.STREAM_READ_TIMEOUT
        ):
            await call.cancel(
                self._send_message,
                exceptions.RpcStreamTimeoutError(
                    'caller stream buffer is full'
                )
            )

    async def _handle_response_stream_close(self, msg):
        """Message handler for RESPONSE_STREAM_CLOSE messages."""
        call = self._outgoing_calls.get(msg.id)
        if call is None:
            self._debug_log(
                'Dropped stream close for unregistred outgoing call %s',
                msg.id
            )
            return
        assert call.stream is not None
        call.stream.close_sync()
