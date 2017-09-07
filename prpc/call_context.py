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

from .protocol import constants
from . import utils


class CallContext(object):
    """Context object passsed to each RPC call.

    Note:
      Should not be instantiated directly.
      :class:`~prpc.connection.Connection` will create instances as neeeded.

    """
    __slots__ = ("_call", "_connection", "_locator")

    def __init__(self, call, connection, locator):
        """Init at new CallContext instance.

        Args:
            connection: Connection instance.
            locator: Method locator instance.
            call_type: Current call type.
            args: Call positional arguments.
            kwargs: Call keyword arguments.
            stream: Optional stream instance.
        """
        assert call.CALL_TYPE in constants.CallType
        # handshake doesn't reach client API
        assert call.CALL_TYPE != constants.CallType.HANDSHAKE
        self._call = call
        self._connection = connection
        self._locator = locator

    @property
    def method(self):
        "Get method name that was used in the call."
        return self._call.method

    @property
    def args(self):
        "Get call positional arguments."
        return self._call.args

    @property
    def kwargs(self):
        "Get call keyword arguments."
        return self._call.kwargs

    @property
    def stream(self):
        """Get :class:`~prpc.stream.Stream` instance associated with the call.

        Note:
            Returns 'None' for unary calls.
        """
        return self._call.stream

    @property
    def call_type(self):
        "Get current call type (:class:`~prpc.protocol.constants.CallType`)."
        return self._call.CALL_TYPE

    @property
    def connection(self):
        """:class:`~prpc.connection.Connection` responsible for the call."""
        return self._connection

    @property
    def locator(self):
        """Get the method locator instance used to resolve the method.

        See :mod:`prpc.method_locator` for more details.
        """
        return self._locator

    @property
    def loop(self):
        "Return the asyncio event loop instance used in current connection."
        return self._connection.loop

    def __repr__(self):
        "String representation for debug and logging."
        return (
            utils.ReprBuilder(self)
            .add_value("method", self.method)
            .add_value("call_type", self.call_type)
            .add_iterable("args", self.args)
            .add_mapping("kwargs", self.kwargs)
            .format()
        )
