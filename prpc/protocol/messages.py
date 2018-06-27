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
Logical message types definitions, encapsulating both field access and
wire serialization format.

Note:
    Message classes are rather similar, so they could have been
    generated using some factory. However, generating 'first-class' classes
    without self-introspection would require really ugly hacks with exec
    (e.g. see collections.namedtuple).
    As a bonus, manual classes are IDE-friendly.

Note:
    namedtuple itself isn't right as there is no sane way to 'bind' the
    first tuple argument to be constant (MessageType).
"""

import collections

import msgpack

from .constants import MessageType


_message_by_type = {}


def message_class(cls):
    """Decorator to register message class for deserialization.

    Explicit mapping looks cleaner than __subclasses__ hacks and
    adds some bonus validation.

    Args:
      cls: Class with (not yet registered) class-level MESSAGE_TYPE variable.

    Return:
      Cls itself without any changes.
    """
    global _message_by_type
    assert cls.MESSAGE_TYPE in MessageType
    assert cls.MESSAGE_TYPE not in _message_by_type
    _message_by_type[cls.MESSAGE_TYPE] = cls
    return cls


def pack(data):
    """Convert 'simple' (~POD) python object to a compact byte representation.

    Args:
        data: Object to pack.

    Return:
        Byte representation of the object.
    """
    # kwargs to ensure proper encoding of unicode string
    # (so it's distinct from byte strings)
    return msgpack.packb(data, use_bin_type=True, encoding="utf-8")


def unpack(dgram):
    """Unpack byte representation of an object.

    Args:
      dgram: Serialized object bytes.

    Return:
      Unpacked object.
    """
    # kwargs to ensure proper decode of unicode strings
    return msgpack.unpackb(dgram, encoding="utf-8")


class ProtocolMessage(object):
    """Base class for protocol messages."""
    __slots__ = ("id",)

    def __init__(self):
        raise NotImplementedError()

    @classmethod
    def from_bytes(self, dgram):
        """
        Decode message from wire format.

        Args:
            dgram: Message bytes.

        Return:
            Decoded message.
        """
        message_tuple = unpack(dgram)
        message_type = message_tuple[0]
        cls = _message_by_type[message_type]
        return cls(*message_tuple[1:])

    def to_bytes(self):
        """
        Encode message to wire format.

        Return:
            Serialized message bytes.
        """
        # Theoretically, tuple is redundant there
        # as the message content can be deserialized
        # based solely on MessageType implicitly
        # encoding number of elements.
        #
        # However, overhead of array based serialization is just
        # 1 byte due to msgpack's fixarray format
        # (applies for arrays with <= 15 elements).
        #
        # And it makes serialization/deserialization code really simple.
        #
        # Worth it )
        return pack(self.to_tuple())

    def to_tuple(self):
        """
        Convert message to a tuple (with fixed field order).
        """
        raise NotImplementedError()


#: Handshake data. Very basic for now.
#:
HandshakeData = collections.namedtuple(
    "HandshakeData", ("protocol_version", "id", "user_data")
)

#: Serialized exception object.
#:
ErrorDescription = collections.namedtuple(
    "ErrorDescription", ("message", "cause_type", "cause_message", "trace")
)


@message_class
class RequestHandshake(ProtocolMessage):
    """Handshake message sent by 'connecting' side.

    Contains 1 payload field:

    * args: handshake 'arguments' (must be HandshakeData tuple)
    """

    __slots__ = ("args",)

    MESSAGE_TYPE = MessageType.REQUEST_HANDSHAKE

    def __init__(self, id, args):
        self.id = id
        self.args = HandshakeData(*args)

    def to_tuple(self):
        return (self.MESSAGE_TYPE, self.id, self.args)


@message_class
class RequestCallStart(ProtocolMessage):
    """Message requesting a new call.

    Contains 4 payload fields:

    * call_type: enum value desribing call type
    * method: method name
    * args: positional argument list
    * kwargs: keyword argument dict
    """

    __slots__ = ("call_type", "method", "args", "kwargs")

    MESSAGE_TYPE = MessageType.REQUEST_CALL_START

    def __init__(self, id, call_type, method, args, kwargs):
        self.id = id
        self.call_type = call_type
        self.method = method
        self.args = args
        self.kwargs = kwargs

    def to_tuple(self):
        return (self.MESSAGE_TYPE, self.id, self.call_type,
                self.method, self.args, self.kwargs)


@message_class
class RequestCallCancel(ProtocolMessage):
    """Request to close remote stream on the callee side.

    Has no payload.
    """
    __slots__ = ()

    MESSAGE_TYPE = MessageType.REQUEST_CALL_CANCEL

    def __init__(self, id):
        self.id = id

    def to_tuple(self):
        return (self.MESSAGE_TYPE, self.id)


@message_class
class RequestStreamMessage(ProtocolMessage):
    """Data message containing stream message from caller.

    Only 1 payload field:

    * data: unserialized data (will be packed with the message itself)
    """
    __slots__ = ("data",)

    MESSAGE_TYPE = MessageType.REQUEST_STREAM_MESSAGE

    def __init__(self, id, data):
        self.id = id
        self.data = data

    def to_tuple(self):
        return (self.MESSAGE_TYPE, self.id, self.data)


@message_class
class RequestStreamClose(ProtocolMessage):
    """Request to close remote stream on the callee side.

    Has no payload.
    """
    __slots__ = ()

    MESSAGE_TYPE = MessageType.REQUEST_STREAM_CLOSE

    def __init__(self, id):
        self.id = id

    def to_tuple(self):
        return (self.MESSAGE_TYPE, self.id)


@message_class
class ResponseHandshake(ProtocolMessage):
    """Handshake call response.

    Contains 3 payload fields:

    * result: call return value (must be a HandshakeData tuple)
    * status: call status
    * error: error as ErrorDescription tuple
    """

    __slots__ = ("result", "status", "error")

    MESSAGE_TYPE = MessageType.RESPONSE_HANDSHAKE

    def __init__(self, id, result, status, error):
        self.id = id
        self.result = HandshakeData(*result)
        self.status = status
        # There is a trade-off between 'expanded' errors and nested error tuple.
        # As most calls are expected to finish successfully, nested approach
        # looks better (so we have just 1 byte overhead on success and don't
        # really care about redundant unpacking if exception actually happened).
        self.error = ErrorDescription(*error) if error is not None else None

    def to_tuple(self):
        return (
            self.MESSAGE_TYPE, self.id, self.result, self.status, self.error
        )


@message_class
class ResponseResult(ProtocolMessage):
    """Regular call final response.

    Has similar structure to the hanshake response,
    but the result isn't limited to HandshakeData.

    Contains 3 payload fields:

    * result: call return value
    * status: call status
    * error: error as ErrorDescription tuple
    """

    __slots__ = ("result", "status", "error")

    MESSAGE_TYPE = MessageType.RESPONSE_RESULT

    def __init__(self, id, result, status, error):
        self.id = id
        self.result = result
        self.status = status
        self.error = ErrorDescription(*error) if error is not None else None

    def to_tuple(self):
        return (
            self.MESSAGE_TYPE, self.id, self.result, self.status, self.error
        )


@message_class
class ResponseStreamMessage(RequestStreamMessage):
    """Data message containing stream message from callee.

    Only 1 payload field:

    * data: unserialized data (will be packed with the message itself)
    """
    __slots__ = ("data",)

    MESSAGE_TYPE = MessageType.RESPONSE_STREAM_MESSAGE


@message_class
class ResponseStreamClose(RequestStreamClose):
    """Request to close stream on the calling side.

    Has no payload.
    """
    __slots__ = ()

    MESSAGE_TYPE = MessageType.RESPONSE_STREAM_CLOSE
