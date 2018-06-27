#
# coding: utf-8
# Copyright (c) 2018 DATADVANCE
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
Defines classes representing current state of each RPC call.

Call classes are tightly bound to the Connection and may be considered
it's implementation detail.
"""

from . import call_context, exceptions, stream, utils
from .protocol import constants, messages


class Call(object):
    """Base class for call state representation.

    The 'Call' hierarchy has 4 main goals:

    * Simplify protocol code by abstracting the call type out
      (e.g. unary and stream calls pass different arguments to the
      target callable)

    * Maintain the separate FSM for the call.

    * Store cohesive state of the call at any point of time
      to simplify all kinds of debugging. That leads to some memory
      overhead (e.g. outgoing calls don't really need to store 'args'),
      but it should be negligible.

    * Improve internal exception tolerance when used as context manager


    Note:
      Can be refactored into separate hierarchies of "incoming"
      and "outgoing" calls (~12 classes in total). It should be done,
      if it continues to grow. Time for full-Java madness has not come yet.
    """

    CALL_TYPE = None

    def __init__(self, request_id, loop, method, args, kwargs, registry):
        """Initialize new call object.

        Args:
            request_id: Unique request id.
            loop: Current event loop.
            method: Method name.
            args: Method positional args.
            kwargs: Method keyword args.
            registry: Call registry to store this call.
        """
        self._id = request_id
        self._loop = loop
        self._method = method
        self._args = args or ()
        self._kwargs = kwargs or {}
        self._registry = registry

        self._state = constants.CallState.NEW
        # initialized in 'send'/'invoke'
        self._result = None
        self._stream = None
        self._method_task = None

    @property
    def id(self):
        """Get call's request id."""
        return self._id

    @property
    def method(self):
        """Get taget method name."""
        return self._method

    @property
    def args(self):
        """Get call arguments."""
        return self._args

    @property
    def kwargs(self):
        """Get call keyword arguments."""
        return self._kwargs

    @property
    def result(self):
        """Get the result future."""
        return self._result

    @property
    def stream(self):
        """Get the stream instance."""
        return self._stream

    @property
    def state(self):
        """Get the current call state."""
        return self._state

    async def send(self, on_message):
        """Emit a call message and change call state.

        Args:
            on_message: Async callable accepting
                a messages.ProtocolMessage instance.
        """
        assert self._state == constants.CallState.NEW
        self._state = constants.CallState.SENDING
        self._result = self._loop.create_future()
        await self._send(on_message)

    async def cancel(self, on_message, exception=None):
        """Cancel the outgoing call and notify the peer.

        Args:
            on_message: Async callable accepting
                a messages.ProtocolMessage instance.
            exception: Exception object to set as call result (optional).
        """
        if self._state in (constants.CallState.FINISHED,
                           constants.CallState.CANCELLED):
            return False
        if self._state != constants.CallState.SENDING:
            raise exceptions.InvalidCallState(
                'only active outgoing calls can '
                'be cancelled (current status: %s)' % (self._state.name,)
            )
        self._state = constants.CallState.CANCELLED
        self._result.set_exception(
            exception or exceptions.RpcCancelledError(
                'call is cancelled by client'
            )
        )
        await on_message(messages.RequestCallCancel(self.id))
        return True

    def set_result(self, result):
        """Publish result recieved from peer and finalize the call.

        Args:
            result: Call result, will be forwarded directly to caller.

        Returns:
            True if result is successfully set.
        """
        if self._state in (constants.CallState.FINISHED,
                           constants.CallState.CANCELLED):
            return False
        if self._state != constants.CallState.SENDING:
            raise exceptions.InvalidCallState(
                'only active outgoing calls can '
                'receive results (current status: %s)' % (self._state.name,)
            )
        self._state = constants.CallState.FINISHED
        self._result.set_result(result)
        return True

    def set_exception(self, exception):
        """Publish exception recieved from peer and finalize the call.

        Args:
            exception: Any exception object, will be
                forwarded directly to caller.

        Returns:
            True if exception is successfully set.
        """
        if self._state in (constants.CallState.FINISHED,
                           constants.CallState.CANCELLED):
            return False
        if self._state != constants.CallState.SENDING:
            raise exceptions.InvalidCallState(
                'only active outgoing calls can '
                'receive results (current status: %s)' % (self._state.name,)
            )
        self._state = constants.CallState.FINISHED
        self._result.set_exception(exception)
        return True

    async def accept(self, on_message):
        """Prepare to accept the call.

        Args:
            on_message: Async callable accepting
                a messages.ProtocolMessage instance.
        """
        assert self._state == constants.CallState.NEW
        self._state = constants.CallState.ACCEPTING
        await self._accept(on_message)

    async def invoke(self, method, connection, locator):
        """Invoke the method. Must be called after 'accept'.

        Args:
            method: Async callable accepting a CallContext instance.
            connection: Connection instance to be passed to as a context.
            locator: Locator instance to be passed to as a context.

        Returns:
            Method return value.
        """
        assert self._state == constants.CallState.ACCEPTING
        self._state = constants.CallState.ACCEPTED
        self._result = self._loop.create_future()
        ctx = call_context.CallContext(self, connection, locator)
        return await self._call_method(method, ctx)

    def cancelled(self, exception=None):
        """Cancel the method execution.

        Can be called on accepted calls only. For finished calls it is a noop.

        Args:
            exception: Optional exception to set as a call result.

        Warning:
            Exception type is actually important. Using RpcCancelledError there
            has a special meaning, as it suppresses sending response
            to the peer.
        """
        # nothing to cancel
        if self._state in (constants.CallState.FINISHED,
                           constants.CallState.CANCELLED):
            return
        if self._state != constants.CallState.ACCEPTED:
            raise exceptions.InvalidCallState(
                'only active accepted calls can '
                'be cancelled (current status: %s)' % (self._state.name,)
            )
        self._state = constants.CallState.CANCELLED
        if self._method_task is not None:
            self._method_task.cancel()
        self._result.set_exception(
            exception or exceptions.RpcCancelledError(
                'call cancelled by client'
            )
        )
        # 'Terminal' exceptions can be ignored.
        self._result.exception()

    def close(self, exception=None):
        """Finish the call unconditionally.

        Args:
           exception: Optional exception to set as a call result.
        """
        if self._state == constants.CallState.SENDING:
            self._result.set_exception(
                exception or exceptions.RpcLocalError('call closed')
            )
            # 'Terminal' exceptions can be ignored.
            self._result.exception()
            self._state = constants.CallState.FINISHED
        elif self._state == constants.CallState.ACCEPTED:
            self.cancelled(exception)

    async def _send(self, on_message):
        """Interface for subclasses.

        Should setup the local state and then send request to the peer.

        Args:
            on_message: Async callable accepting a
                messages.ProtocolMessage instance.
        """
        raise NotImplementedError()

    async def _accept(self, on_message):
        """Interface for subclasses.

        Should setup the local state before method invocation.

        Args:
            on_message: Async callable accepting
                a messages.ProtocolMessage instance.
        """
        raise NotImplementedError()

    def _make_call_request_message(self, call_type):
        """Helper - create REQUEST_CALL_START message to send."""
        assert call_type in constants.CallType
        return messages.RequestCallStart(
            self._id, call_type, self._method, self._args, self._kwargs
        )

    async def _call_method(self, method, context):
        """Helper - wrap method invocation in a task."""
        self._method_task = self._loop.create_task(
            self._forward_task_result(method(context))
        )
        try:
            return await self._result
        finally:
            self._method_task = None

    async def _forward_task_result(self, method_coro):
        """Helper - execute as a task and forward method result."""
        # Note: it's important to change state right here, otherwise
        # we can get race condition between `invoke` `cancelled` because
        # of intermediate awaits.
        try:
            result = await method_coro
            # Can be done if it's cancelled.
            if not self._state == constants.CallState.CANCELLED:
                self._state = constants.CallState.FINISHED
                self._result.set_result(result)
        # Even the infamous sys.exit shouldn't leak to through RPC, so let's
        # catch BaseException.
        except BaseException as ex:
            # Result may be already resolved by cancel().
            if not self._state == constants.CallState.CANCELLED:
                self._state = constants.CallState.FINISHED
                self._result.set_exception(
                    exceptions.RpcMethodError.wrap_exception(
                        'call %s (\'%s\') failed: %s' % (
                            self._id, self._method, str(ex)), ex
                    )
                )

    def __enter__(self):
        """Context manager interface.

        Register the request id in the external registry.
        """
        assert self._id not in self._registry
        self._registry[self._id] = self
        return self

    def __exit__(self, *exc_info):
        """Context manager interface.

        Unregister the request id and finish the call.
        """
        self._registry.pop(self._id)
        self.close()
        return False

    def __repr__(self):
        """String representation for debug and logging."""
        return (
            utils.ReprBuilder(self)
            .add_value('id', self._id)
            .add_value('method', self._method)
            .add_iterable('args', self._args)
            .add_mapping('kwargs', self._kwargs)
            .format()
        )


class Handshake(Call):
    """Handhsake call.

    Note:
        Does not implement _accept/_invoke API as Handshake calls are
        handled by connection itself.
    """
    CALL_TYPE = constants.CallType.HANDSHAKE

    async def _send(self, on_message):
        """Call interface implementation."""
        handshake = messages.RequestHandshake(self._id, self._args)
        await on_message(handshake)

    async def cancel(self, on_message, exception=None):
        """Overriden."""
        # Sending cancel to peer is meaningless.
        async def noop(msg):
            pass
        return await super().cancel(noop, exception)


class UnaryCall(Call):
    """Unary call."""
    CALL_TYPE = constants.CallType.UNARY

    async def _send(self, on_message):
        """Call interface implementation."""
        await on_message(self._make_call_request_message(self.CALL_TYPE))

    async def _accept(self, on_message):
        """Call interface implementation."""
        pass


class StreamCall(Call):
    """Abstract stream call. Shouldn't be ever instantiated."""

    def __init__(self, *args):
        super().__init__(*args)
        self._stream = stream.Stream(self._id, self._loop)

    def close(self, exception=None):
        """Override .close to close the stream too."""
        self._stream.close_sync()
        super().close(exception)

    def _make_close_callback(self, on_message):
        """Assemble a close callback to pass to Stream object.

        Args:
            on_message: Async callable ((messages.ProtocolMessage) -> NoneType).
        """
        if self._state == constants.CallState.SENDING:
            message_cls = messages.RequestStreamClose
        elif self._state == constants.CallState.ACCEPTING:
            message_cls = messages.ResponseStreamClose
        else:
            raise exceptions.InvalidCallState(
                'cannot create stream close callback: unexpected call status'
            )

        async def on_close(request_id):
            """Stream closed callback."""
            assert self._id == request_id
            await on_message(message_cls(request_id))
        return on_close

    def _make_write_callback(self, on_message):
        """Assemble a write callback to pass to Stream object.

        Args:
            on_message: Async callable ((messages.ProtocolMessage) -> NoneType).
        """
        if self._state == constants.CallState.SENDING:
            message_cls = messages.RequestStreamMessage
        elif self._state == constants.CallState.ACCEPTING:
            message_cls = messages.ResponseStreamMessage
        else:
            raise exceptions.InvalidCallState(
                'cannot create stream write callback: unexpected call status'
            )

        async def on_write(request_id, data):
            """Stream write callback."""
            assert self._id == request_id
            await on_message(message_cls(request_id, data))
        return on_write


class IStreamCall(StreamCall):
    """IStream call (aka download)."""
    CALL_TYPE = constants.CallType.ISTREAM

    async def _send(self, on_message):
        """Call interface implementation."""
        self._stream.open(constants.StreamMode.READ)
        self._stream.on_close.append(self._make_close_callback(on_message))
        await on_message(self._make_call_request_message(self.CALL_TYPE))

    async def _accept(self, on_message):
        """Call interface implementation."""
        self._stream.open(constants.StreamMode.WRITE)
        self._stream.on_write.append(self._make_write_callback(on_message))
        self._stream.on_close.append(self._make_close_callback(on_message))


class OStreamCall(StreamCall):
    """OStream call (aka upload)."""
    CALL_TYPE = constants.CallType.OSTREAM

    async def _send(self, on_message):
        """Call interface implementation."""
        self._stream.open(constants.StreamMode.WRITE)
        self._stream.on_write.append(self._make_write_callback(on_message))
        self._stream.on_close.append(self._make_close_callback(on_message))
        await on_message(self._make_call_request_message(self.CALL_TYPE))

    async def _accept(self, on_message):
        """Call interface implementation."""
        self._stream.open(constants.StreamMode.READ)
        self._stream.on_close.append(self._make_close_callback(on_message))


class BiStreamCall(StreamCall):
    """BiStream call (aka communicate/subprotocol)."""
    CALL_TYPE = constants.CallType.BISTREAM

    async def _send(self, on_message):
        """Call interface implementation."""
        self._stream.open(
            constants.StreamMode.READ | constants.StreamMode.WRITE
        )
        self._stream.on_write.append(self._make_write_callback(on_message))
        self._stream.on_close.append(self._make_close_callback(on_message))
        await on_message(self._make_call_request_message(self.CALL_TYPE))

    async def _accept(self, on_message):
        """Call interface implementation."""
        self._stream.open(
            constants.StreamMode.READ | constants.StreamMode.WRITE
        )
        self._stream.on_write.append(self._make_write_callback(on_message))
        self._stream.on_close.append(self._make_close_callback(on_message))
