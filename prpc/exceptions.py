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

import sys
import traceback


class RpcError(Exception):
    """Base library exception.

    Note:
      Has the same properties as :class:`RpcRemoteError` so that handling RPC
      exceptions using base class (i.e. 'except RpcError') is safer.
    """
    @property
    def cause_type(self):
        """Get nested exception type as a string."""
        return '<local error>'

    @property
    def cause_message(self):
        """Get nested exception message as a string."""
        return '<local error>'

    @property
    def remote_traceback(self):
        """Get full traceback as a single string."""
        return '<local error>'


class RpcLocalError(RpcError):
    """Represent local failures, e.g. unable to parse server response."""


class RpcConnectionClosedError(RpcLocalError):
    """Connection is unexpectedly closed."""


class RpcConnectionTimeoutError(RpcConnectionClosedError):
    """Connection is closed by timeout."""


class RpcConnectionRejectedError(RpcConnectionClosedError):
    """Connection is rejected at handshake stage."""


class RpcCallTimeoutError(RpcLocalError):
    """RPC execution timeout expired."""


class RpcStreamTimeoutError(RpcLocalError):
    """Stream read timeout expired (so data wasn't consumed by method)."""


class RpcRemoteError(RpcError):
    """Base class for errors on peer's side."""
    @classmethod
    def wrap_exception(cls, message, exception):
        """Create remote error wrapping existing exception."""
        result = cls(message, type(exception).__name__, str(exception), None)
        result.__cause__ = exception
        return result

    def __init__(self, message,
                 cause_type=None,
                 cause_message=None,
                 remote_traceback=None):
        super().__init__(message)
        self._cause_type = cause_type or ''
        self._cause_message = cause_message or ''
        self._remote_traceback = remote_traceback or '<no traceback available>'

    @property
    def cause_type(self):
        """Get nested exception type as a string."""
        return self._cause_type

    @property
    def cause_message(self):
        """Get nested exception message as a string."""
        return self._cause_message

    @property
    def remote_traceback(self):
        """Get full traceback as a single string."""
        return self._remote_traceback


class RpcMethodNotFoundError(RpcRemoteError):
    """Remote method is not found."""


class RpcMethodError(RpcRemoteError):
    """Remote method raised an error."""


class RpcServerError(RpcRemoteError):
    """Unspecified server error."""


class RpcCancelledError(RpcRemoteError):
    """Call is cancelled."""

    def __init__(self, message,
                 cause_type='<cancelled by client>',
                 cause_message='<cancelled by client>',
                 remote_traceback=None):
        super().__init__(message, cause_type, cause_message, remote_traceback)


class RpcUnknownError(RpcRemoteError):
    """Unexcepted failure occured on remote server."""


class InvalidCallState(RpcError):
    """Call has invalid status for requested operation."""


class InvalidStreamMode(RpcError):
    """Stream has invalid mode for requested operation."""


class InvalidStreamState(RpcError):
    """Stream has invalid status for requested operation."""


def format_traceback_string(exception):
    """Format exception traceback as a single string.

    Args:
        exception: Exception object.

    Returns:
        Full exception traceback as a string.
    """
    return '\n'.join(
        traceback.TracebackException.from_exception(exception).format()
    )


def finally_error(default_message, default_type=RpcLocalError):
    """Get a usable exception object in finally blocks.

    If there is an active exception, return it, otherwise generate a new one.

    Args:
        default_message (str): Exception message to use if there
            is no current exception
        default_type (type): Exception type to use if there
            is no current exception.

    Returns:
        Exception object.
    """
    current = sys.exc_info()[1]
    if current is not None:
        return current
    return default_type(default_message)
