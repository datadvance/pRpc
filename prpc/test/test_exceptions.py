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

import pytest

import prpc.exceptions


def helper_raise_error(*exception_types, use_cause=True):
    def create_raiser(exception_type, message, use_cause, next_raiser=None):
        def raiser(error=None):
            try:
                if error and use_cause:
                    raise exception_type(message) from error
                raise exception_type(message)
            except Exception as ex:
                if next_raiser:
                    next_raiser(ex)
                else:
                    raise
        return raiser
    assert len(exception_types) > 0
    current = None
    for idx, exception_type in enumerate(reversed(exception_types)):
        current = create_raiser(
            exception_type, 'exception %d' % (idx,), use_cause, current
        )
    current()


def test_finally_exception():
    """Test 'finally_error' logic.

    * it should return 'current' exception if it exists
    * otherwise, it returns 'default_class(default_message)'
    """
    with pytest.raises(RuntimeError):
        try:
            helper_raise_error(RuntimeError)
        finally:
            raise prpc.exceptions.finally_error(
                'using current exc', Exception)

    with pytest.raises(ValueError):
        raise prpc.exceptions.finally_error('using default exc', ValueError)


def test_format_exception():
    """Check that traceback formatting works and produces something sensible.

    As it is purely debug info, there is no goal to fix the precise format,
    so only the basic checks are run.
    """
    try:
        helper_raise_error(TypeError, RuntimeError, ValueError)
    except Exception as ex:
        formatted = prpc.exceptions.format_traceback_string(ex)
        assert 'TypeError: ' in formatted
        assert 'RuntimeError: ' in formatted
        assert 'ValueError: ' in formatted


def test_exception_properties():
    """Check that local and remote errors have the same public properties.
    """
    local = prpc.exceptions.RpcLocalError('oops')
    assert local.cause_type != ''
    assert local.cause_message != ''
    assert local.remote_traceback != ''
    remote = prpc.exceptions.RpcRemoteError('oops')
    assert remote.cause_type == ''
    assert remote.cause_message == ''
    assert 'no traceback' in remote.remote_traceback
