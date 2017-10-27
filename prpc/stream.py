#
# coding: utf-8
# Copyright (c) 2017 DATADVANCE
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
from . import utils


class Stream(object):
    """Demultiplexed data stream.

    Represents a (possibly bidirectional) data flow between RPC client
    and called method.

    Can be used as a async iterator, e.g. simple echo implementation::

        async for msg in stream:
            await stream.send(msg)
    """

    def __init__(self, id, loop):
        """Initialize new stream instance.

        Args:
            id: Request id associated with the stream.
            loop: Asyncio loop to use.
        """
        self._id = id
        self._loop = loop
        self._state = constants.StreamState.NEW
        # Store 2 plain flags instead of _mode enum.
        # IntFlag's operator & (for checks of form "_mode & StreamMode.READ")
        # is surprisingly slow.
        #
        # It was slow enough to impact the messaging on small message sizes
        # (~10-15% slowdown on small messages, completely unreasonable
        # for assert-like code).
        self._is_readable = False
        self._is_writable = False
        # Created on .open only.
        self._input_queue = None
        self._on_close = utils.Signal()
        self._on_write = utils.Signal()

    @property
    def is_writable(self):
        """Returns True if stream accepts 'send' requests."""
        return self._is_writable

    @property
    def is_readable(self):
        """Returns True if stream accepts 'receive' requests."""
        return self._is_readable

    @property
    def is_open(self):
        """Return True if stream is open."""
        return self._state == constants.StreamState.OPEN

    @property
    def is_closed(self):
        """Return True if stream is closed."""
        return self._state != constants.StreamState.OPEN

    @property
    def data_available(self):
        """Check if there are any messages immediately available."""
        if self._is_readable:
            return not self._input_queue.empty()
        return False

    @property
    def on_write(self):
        """Get write signal instance."""
        return self._on_write

    @property
    def on_close(self):
        """Get close signal instance."""
        return self._on_close

    async def receive(self, timeout=None):
        """Read a data chunk from the stream.

        Note:
            Shoudn't be ever awaited in parallel from different coroutines.

        Returns:
            Received value or None (None means that stream is closed).
        """
        self._require_readable()
        if not self._input_queue.empty():
            result = self._input_queue.get_nowait()
        else:
            # Allow reads in closing state so we can get 'remaining' messages.
            if (self._state == constants.StreamState.NEW or
                    self._state == constants.StreamState.CLOSED):
                raise exceptions.InvalidStreamState('stream is not open')
            if timeout is None:
                result = await self._input_queue.get()
            else:
                result = await asyncio.wait_for(
                    self._input_queue.get(),
                    timeout=timeout, loop=self._loop
                )
        return result

    async def send(self, data):
        """Send a message to a remote stream.

        Args:
            data: Data chunk to send.
        """
        self._require_writable()
        if self._state != constants.StreamState.OPEN:
            raise exceptions.InvalidStreamState('stream is not open')
        # None is used a close marker, so it's forbidden.
        if data is None:
            raise TypeError('NoneType cannot be sent')
        # Check that connection is subscribed for stream events.
        assert len(self._on_write)
        await self._on_write.send(self._id, data)

    async def close(self):
        """Close the stream and notify the peer."""
        if self._state != constants.StreamState.OPEN:
            return
        # Check that connection is subscribed for stream events.
        assert len(self._on_close)
        # Store _on_close as close_sync() detaches signals.
        close_signal = self._on_close
        self.close_sync()
        await close_signal.send(self._id)

    async def feed(self, data, timeout=None):
        """Feed a new 'input' message into stream.

        Warning:
            Should not be called by client code, used by library internally.

        Args:
            data: Unpacked message content.
            timeout: Timeout (in seconds).

        Returns:
           True if feed was successfull, False otherwise (message is dropped).
        """
        if self._state != constants.StreamState.OPEN:
            return False
        # Not only it makes no sense to 'feed' unreadable streams,
        # but it's also a reliable way to block a listen loop
        # when queue gets filled up.
        self._require_readable()
        if not self._input_queue.full():
            self._input_queue.put_nowait(data)
        else:
            if timeout is None:
                await self._input_queue.put(data)
            else:
                try:
                    await asyncio.wait_for(
                        self._input_queue.put(data),
                        timeout=timeout, loop=self._loop
                    )
                except asyncio.TimeoutError:
                    return False
        return True

    def open(self, mode):
        """Open a stream in a given mode.

        Warning:
            Should not be called by client code, used by library internally.

        Args:
            mode: Stream open mode (r/w/rw).
        """
        if not isinstance(mode, constants.StreamMode):
            raise TypeError('protocol.constants.StreamMode is expected')
        # Mode shoudn't be == 0.
        if not bool(mode):
            raise exceptions.InvalidStreamMode('incorrect open mode')
        if self._state != constants.StreamState.NEW:
            raise exceptions.InvalidStreamState(
                'stream is already initialized'
            )
        self._state = constants.StreamState.OPEN
        # Those checks are too slow to make on send/receive,
        # see __init__ for more details.
        self._is_readable = bool(mode & constants.StreamMode.READ)
        self._is_writable = bool(mode & constants.StreamMode.WRITE)
        if self._is_readable:
            self._input_queue = asyncio.Queue(
                constants.STREAM_BUFFER_QUEUE_SIZE, loop=self._loop
            )

    def close_sync(self):
        """Close the stream synchronously.

        Implement 'forced' closes, when there is no need to notify the peer
        (close received from peer, call finished, connection closed etc).

        Warning:
            Should not be called by client code, used by library internally.
        """
        if self._state != constants.StreamState.OPEN:
            return
        self._state = constants.StreamState.CLOSING
        self._on_close = None
        self._on_write = None
        # No need to close queue on write-only streams.
        if not self._is_readable:
            self._state = constants.StreamState.CLOSED
        else:
            if self._input_queue.full():
                # TODO:
                # Ugly solution, but almost correct.
                # Proper one seems to be impossible without
                # fixing the Queue class (or implementing our own).
                self._loop.create_task(self._close_queue())
            else:
                self._input_queue.put_nowait(None)
                self._state = constants.StreamState.CLOSED

    async def _close_queue(self):
        """Puts close marker into the input queue and wraps up stream closing"""
        await self._input_queue.put(None)
        self._state = constants.StreamState.CLOSED

    def _require_readable(self):
        """Raise if stream is not readable"""
        if not self._is_readable:
            raise exceptions.InvalidStreamMode(
                'stream mode does not allow reading'
            )

    def _require_writable(self):
        """Raise if stream is not writable"""
        if not self._is_writable:
            raise exceptions.InvalidStreamMode(
                'stream mode does not allow writing'
            )

    def __aiter__(self):
        """Async iterator interface."""
        return self

    async def __anext__(self):
        """Async iterator interface."""
        message = await self.receive()
        if message is None:
            raise StopAsyncIteration
        return message

    def __repr__(self):
        """String representation for debug and logging."""
        return (
            utils.ReprBuilder(self)
            .add_value('closed', self.is_closed)
            .add_value('readable', self.is_readable)
            .add_value('writable', self.is_writable)
            .format()
        )
