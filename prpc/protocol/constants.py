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

import enum


PROTOCOL_VERSION = 1


HANDSHAKE_TIMEOUT = 60.
STREAM_READ_TIMEOUT = 5.


STREAM_BUFFER_QUEUE_SIZE = 32


@enum.unique
class MessageType(enum.IntEnum):
    """Capstone enumeration of all protocol message types.

    Each message has it's own payload format, as defined in
    :mod:`prpc.protocol.messages` module.
    """
    REQUEST_HANDSHAKE = 1
    REQUEST_CALL_START = 2
    REQUEST_CALL_CANCEL = 3
    REQUEST_STREAM_MESSAGE = 4
    REQUEST_STREAM_CLOSE = 5
    RESPONSE_HANDSHAKE = 6
    RESPONSE_RESULT = 7
    RESPONSE_STREAM_MESSAGE = 8
    RESPONSE_STREAM_CLOSE = 9


@enum.unique
class CallType(enum.IntEnum):
    """Enumerates all possible call types.

    Note:
        Stream direction is considered from caller's perspective.

    Note:
        HANDSHAKE call type also uses a dedicated message type.
    """
    HANDSHAKE = 10
    UNARY = 20
    ISTREAM = 30
    OSTREAM = 40
    BISTREAM = 50


@enum.unique
class CallState(enum.IntEnum):
    """Enumerates call FSM states.

    In absence of fatal errors, transition table looks as follows:
        *  NEW -> SENDING, ACCEPTING
        *  SENDING -> FINISHED, CANCELED
        *  ACCEPTING -> ACCEPTED
        *  ACCEPTED -> FINISHED, CANCELED
        *  CANCELED -> <terminal>
        *  FINISHED -> <terminal>

    If an error occurs, any state may (and should) transition to FINISHED.
    """
    NEW = 0
    SENDING = 10
    ACCEPTING = 20
    ACCEPTED = 30
    CANCELLED = 40
    FINISHED = 50


@enum.unique
class ResponseStatus(enum.IntEnum):
    """Enumerates remote call statuses.

    Statuses other than success are mapped to exception on the calling side.
    """
    SUCCESS = 0
    METHOD_ERROR = 10
    METHOD_NOT_FOUND = 20
    SERVER_ERROR = 30


@enum.unique
class ConnectionMode(enum.IntEnum):
    """Enumerates possible connection modes.

    Protocol is mostly symmetric, but handshakes are not.
    Also, who is who may be important for reconnect implementation,
    if we get to it (reconnect should be initiated by client).
    """
    NEW = 0
    CLIENT = 10
    SERVER = 20


@enum.unique
class ConnectionState(enum.IntEnum):
    """Enumerates connection FSM states.

    Note:
        Disconnected logic is not implemented yet.

    Transition table looks as follows:
        * NEW -> PENDING
        * PENDING -> CONNECTED
        * CONNECTED -> DISCONNECTED, CLOSING
        * DISCONNECTED -> CONNECTED, CLOSING
        * CLOSING -> CLOSED
        * CLOSED -> <terminal>
    """
    NEW = 0
    PENDING = 10
    CONNECTED = 20
    DISCONNECTED = 30
    CLOSING = 40
    CLOSED = 50


@enum.unique
class StreamState(enum.IntEnum):
    """Stream FSM states.

    Transition table:
        * NEW -> OPEN, CLOSED
        * OPEN -> CLOSING
        * CLOSING -> CLOSED
        * CLOSED -> <terminal>
    """
    NEW = 0
    OPEN = 10
    CLOSING = 20
    CLOSED = 30


@enum.unique
class StreamMode(enum.IntFlag):
    """Stream open mode flags enumeration."""
    READ = 1
    WRITE = 2
