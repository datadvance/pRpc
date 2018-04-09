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

import enum


WS_PROTOCOL_NAME = 'v1.prpc.datadvance'
WS_HEARTBEAT = 10

# Taken from pre-defined WS error codes
# https://tools.ietf.org/html/rfc6455#section-7.4.1
CLOSE_CODE_SUCCESS = 1000
CLOSE_CODE_ERROR = 1002


@enum.unique
class MessageType(enum.IntEnum):
    """Transport protocol message types.

    Does not correspond to actual WS protocol message types,
    things like CONTINUATION, PING, etc should never leak
    to the library code.
    """
    TEXT = 1
    BINARY = 2
    ERROR = 3
    CLOSE = 4


class MessageSocket(object):
    """Message socket interface definition."""
    @property
    def closed(self):
        """Get socket closed flag."""
        raise NotImplementedError()

    @property
    def close_code(self):
        """Get socket close code."""
        raise NotImplementedError()

    async def receive(self):
        """Recieve single message.

        Returns:
            A tuple (message_type, payload)
        """
        raise NotImplementedError()

    async def send(self, data):
        """Send data message.

        Args:
            data: Message payload (str or bytes).
        """
        raise NotImplementedError()

    async def close(self, code=CLOSE_CODE_SUCCESS, message=b''):
        """Close the socket.

        Args:
            code: Close code to send to peer.
            message: Close reason to send to peer.
        """
        raise NotImplementedError()

    def __aiter__(self):
        """Async iterator interface."""
        raise NotImplementedError()

    async def __anext__(self):
        """Async iterator interface."""
        raise NotImplementedError()
