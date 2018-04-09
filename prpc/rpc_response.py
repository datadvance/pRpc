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

import asyncio

from . import exceptions
from .protocol import constants


class RpcResponse(object):
    """Top-level outgoing call representation.

    Should be used as a asynchronous context manager, e.g.::

      async with connection.call_istream("method_name") as call:
          async for msg in call.stream:
              process_message(msg)

    Note:
      Should not be instantiated directly.
      :class:`~prpc.connection.Connection` will create instances as neeeded.
    """

    def __init__(self, start_call, on_message, loop):
        """Construct a new call.

        Note:
          Shoudn't be ever created in client code.
          Use 'Connection.call_*' method family to create instances.

        Args:
            start_call: Coroutine function with args
                (started_future, finished_future).
            on_message: Async callable accepting
                a messages.ProtocolMessage instance.
            loop: asyncio event loop instance
        """
        self._start_call = start_call
        self._on_message = on_message
        self._loop = loop
        self._call = None
        self._started = loop.create_future()
        self._finished = loop.create_future()
        self._result_requested = False
        self._initialized = False

    @property
    async def result(self):
        """Get the call return value as future.

        Warning:
            Closes the call's stream (if it exists).
        """
        self._check_initialized()
        self._result_requested = True
        if (self._call.stream and
            self._call.state == constants.CallState.SENDING):
            await self._call.stream.close()
        # potential multiple await on this future is fine
        return await self._finished

    @property
    def stream(self):
        """Get stream instance (if any). Returns 'None' for unary calls."""
        self._check_initialized()
        return self._call.stream

    async def cancel(self):
        """Try to cancel remote call."""
        self._check_initialized()
        return await self._call.cancel(self._on_message)

    def _check_initialized(self):
        """Helper - check response state."""
        if not self._initialized:
            raise asyncio.InvalidStateError(
                'response must be awaited for before any operations'
            )

    async def __aenter__(self):
        """Async context manager interface."""
        await self
        return self

    async def __aexit__(self, *exc_info):
        """Async context manager interface."""
        if self._call.state == constants.CallState.SENDING:
            await self.cancel()
        # Avoid asyncio error "Future exception was never retrieved",
        # so that the following usage is valid:
        #
        # async with connection.call_<xxx>(...) as call:
        #    if whatever:
        #       call.cancel()
        #
        # Looks questionable, creates unpleasant implicit
        # dependency on exception types used in protocol code
        # (e.g. cancel may also raise RpcStreamTimeoutError), but
        # hard to avoid.
        if (not self._result_requested and
                self._call.state == constants.CallState.CANCELLED):
            try:
                await self._finished
            except (exceptions.RpcCancelledError,
                    exceptions.RpcStreamTimeoutError):
                pass
        # TODO: As an idea, if we have:
        # * no result requested
        # * active exception of type InvalidStreamStatus
        # * and call status is FINISHED
        # We can await the _finished to get the more sensible error.
        #
        # Reasoning: if stream call fails in the middle, calling code
        # will get this exception out of calls to 'stream.receive()' or
        # 'stream.send()'. It's not very helpful. Actualy cause of the stream
        # close is important.
        #
        # Different approach would be to force stream to raise
        # 'InvalidStatusErrors from xxx' to reflect the causing error.
        return False

    def __await__(self):
        """Awaitable interface."""
        if not self._initialized:
            self._loop.create_task(
                self._start_call(self._started, self._finished)
            )
            call = yield from self._started
            self._call = call
            self._initialized = True
        return self
