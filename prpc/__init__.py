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

__version__ = '1.1.0'

from .call_context import CallContext
from .connection import Connection
from .exceptions import (RpcError, RpcRemoteError, RpcLocalError,
                         RpcMethodError, RpcMethodNotFoundError,
                         RpcCancelledError, RpcServerError, RpcUnknownError,
                         RpcConnectionClosedError, RpcConnectionTimeoutError,
                         RpcConnectionRejectedError,
                         RpcCallTimeoutError, RpcStreamTimeoutError)
from .method_locator import AbstractMethodLocator, TreeMethodLocator, method
from . import platform
from . import protocol
from .protocol.constants import CallType, ConnectionState, ConnectionMode
from .rpc_response import RpcResponse
from . import utils
