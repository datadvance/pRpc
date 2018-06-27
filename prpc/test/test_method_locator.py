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

import pytest

import prpc


def test_single_object():
    "Check that method locator properly finds and names methods of an object."
    class Service(object):
        @prpc.method
        async def test_method(self, ctx):
            'whatever'
    # Plain object is handled correctly
    locator = prpc.TreeMethodLocator(Service())
    locator.resolve('test_method', prpc.CallType.UNARY, None)
    # Empty keys are correctly swallowed
    locator = prpc.TreeMethodLocator({'': Service()})
    locator.resolve('test_method', prpc.CallType.UNARY, None)
    # Nested object gets proper prefix
    locator = prpc.TreeMethodLocator({'api': Service()})
    locator.resolve('api.test_method', prpc.CallType.UNARY, None)


def test_free_function():
    "Check that method locator properly finds and names functions."
    @prpc.method
    async def foo(ctx):
        'free function'
    # Plain function works
    locator = prpc.TreeMethodLocator({'foo': foo})
    locator.resolve('foo', prpc.CallType.UNARY, None)
    # Free function in nested dict should work too
    locator = prpc.TreeMethodLocator({'api': {'foo': foo}})
    locator.resolve('api.foo', prpc.CallType.UNARY, None)
    # Empty keys are correctly swallowed
    locator = prpc.TreeMethodLocator({'bar': {'': {'foo': foo}}})
    locator.resolve('bar.foo', prpc.CallType.UNARY, None)


def test_expose():
    "Check that method locator ignores hidden and unmarked methods"
    class Service(object):
        @prpc.method
        async def exposed(self, ctx):
            'bold'

        async def hidden(self, ctx):
            'sneaky'

        async def _hidden_anyway(self):
            'ninja'

    # Without collect_all, only marked methods should be found
    #
    locator = prpc.TreeMethodLocator(Service(), collect_all=False)
    # Marked methods are always visible
    locator.resolve('exposed', prpc.CallType.UNARY, None)
    # Unmarked method is ignored without collect_all
    with pytest.raises(KeyError):
        locator.resolve('hidden', prpc.CallType.UNARY, None)
    # Methods with leading underscore are hidden unconditionally
    with pytest.raises(KeyError):
        locator.resolve('_hidden_anyway', prpc.CallType.UNARY, None)

    # With collect_all, undecorated methods should appear
    #
    locator = prpc.TreeMethodLocator(Service(), collect_all=True)
    # Marked methods are always visible
    locator.resolve('exposed', prpc.CallType.UNARY, None)
    # Unmarked method should now be visible
    locator.resolve('hidden', prpc.CallType.UNARY, None)
    # Methods with leading underscore are hidden unconditionally
    with pytest.raises(KeyError):
        locator.resolve('_hidden_anyway', prpc.CallType.UNARY, None)


def test_signature(event_loop):
    """Test that method signature doesn't break.

    The potential issue that all the decorators, wrappers and bound methods
    can miscooperate and change final callable signature in unexpected way.

    Expected behavior:
      * final method signature is exactly 'foo(ctx: CallContext)'
      * final method is still a coroutine function
      * for member methods, self is properly bound
      * expand_args works, allowing sensible declared signatures
    """
    class MockCallContext(object):
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    @prpc.method(expand_args=True)
    async def func_expand(ctx, arg, kwarg=None):
        return arg, kwarg

    @prpc.method(expand_args=False)
    async def func_noexpand(ctx):
        return ctx

    class Service(object):
        @prpc.method(expand_args=True)
        async def expand(self, ctx, arg, kwarg=None):
            assert type(self) is Service
            return arg, kwarg

        @prpc.method(expand_args=False)
        async def noexpand(self, ctx):
            assert type(self) is Service
            return ctx

    methods = {
        'obj': Service(),
        'func': {
            'expand': func_expand,
            'noexpand': func_noexpand
        }
    }

    ARG_VALUE = 'positional_arg'
    KWARG_VALUE = 'keyword_arg'

    locator = prpc.TreeMethodLocator(methods)

    expanded = [
        locator.resolve('obj.expand', None, None),
        locator.resolve('func.expand', None, None)
    ]
    collapsed = [
        locator.resolve('obj.noexpand', None, None),
        locator.resolve('func.noexpand', None, None)
    ]

    context = MockCallContext(ARG_VALUE, kwarg=KWARG_VALUE)
    for method in expanded:
        arg, kwarg = event_loop.run_until_complete(method(context))
        assert arg == ARG_VALUE
        assert kwarg == KWARG_VALUE
    for method in collapsed:
        ctx = event_loop.run_until_complete(method(context))
        assert ctx.args == (ARG_VALUE,)
        assert ctx.kwargs == {'kwarg': KWARG_VALUE}


def test_cycle_method_definition():
    """
    Check that TreeMethodLocator doesn't hang when cycle is present in 'tree'.
    """
    @prpc.method
    async def foo(ctx):
        'i am foo'

    methods = {'foo': foo}
    methods['bar'] = methods

    with pytest.raises(ValueError):
        locator = prpc.TreeMethodLocator(methods)


def test_name_clash():
    """Check that TreeMethodLocator raises on method name clash."""
    @prpc.method
    async def foo(ctx):
        """i am foo"""

    methods = {'foo.bar': foo, 'foo': {'bar': foo}}

    with pytest.raises(ValueError):
        try:
            locator = prpc.TreeMethodLocator(methods)
        except:
            import traceback
            traceback.print_exc()
            raise


def test_separator():
    "Check that TreeMethodLocator uses a proper separator."
    @prpc.method
    async def foo(ctx):
        """i am foo"""

    methods = {'foo': {'bar': foo}}

    locator = prpc.TreeMethodLocator(methods)
    locator.resolve('foo.bar', prpc.CallType.UNARY, None)
    with pytest.raises(KeyError):
        locator.resolve('foo/bar', prpc.CallType.UNARY, None)
    locator = prpc.TreeMethodLocator(methods, separator='/')
    locator.resolve('foo/bar', prpc.CallType.UNARY, None)
    with pytest.raises(KeyError):
        locator.resolve('foo.bar', prpc.CallType.UNARY, None)


def test_decorator():
    "Check that @method decorator rejects some wrong invocations."
    with pytest.raises(TypeError):
        @prpc.method('', 'too many args')
        async def foo(ctx):
            """i am foo"""
    with pytest.raises(TypeError):
        @prpc.method(None)
        async def foo(ctx):
            """i am foo too"""
