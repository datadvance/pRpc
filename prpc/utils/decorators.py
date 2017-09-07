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

import functools


def expand_args(function):
    """Expand arguments passed inside the CallContext object.

    Converts internal method signature 'method(ctx)' to whatever
    is appropriate to the client code.

    Args:
      function: Async function expecting CallContext as a last argument.

    Returns:
        Async function with altered signature.
    """
    @functools.wraps(function)
    async def expand_args_wrapper(*args):
        # Valid method signatures are foo(ctx) and foo(self, ctx).
        context = args[-1]
        return await function(*args, *context.args, **context.kwargs)
    return expand_args_wrapper
