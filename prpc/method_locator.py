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

"""
Method locator interface, default implementation and support tools.

Note:
  While locator is not strictly required abstraction, it is created
  to factor this code out of already complicated
  :class:`~prpc.connection.Connection` class.
"""

import itertools

from . import utils


EXPOSE_MARKER = '_prpc_expose_'


def method(*args, expand_args=True):
    """Mark function as an public RPC method and optionally apply decorators.

    May be uses both with and without arguments, e.g.::

      @prpc.method
      async def foo(ctx, arg1, arg2):
          "Args are expanded to fit the signature."

      @prpc.method(expand_args=False)
      async def bar(ctx):
          "All args are passed in ctx object."

    Can be applied to instance/class methods::

      class Service(object):
          @prpc.method
          async def foo(self, ctx):
              "Works fine."
    """
    if len(args) > 1:
        raise TypeError(
            'only one positional argument is expected (function to decorate)'
        )

    def decorator(function):
        decorator_chain = [(expand_args, utils.decorators.expand_args)]
        for enabled, decorator in decorator_chain:
            if enabled:
                function = decorator(function)
        setattr(function, EXPOSE_MARKER, True)
        return function

    if args:
        function, = args
        if not callable(function):
            raise TypeError('decorator should be applied to a callable')
        return decorator(function)
    return decorator


class AbstractMethodLocator(object):
    """Method locator interface definition."""

    def resolve(self, method_name, call_type, connection):
        """Resolve a method given call context.

        Args:
          method_name: Method name as a string.
          call_type: Call type (unary, istream, etc).
          connection: Connection object related.

        Returns:
          method as an async callable expecting a single argument (CallContext)
        """
        raise NotImplementedError()


class TreeMethodLocator(AbstractMethodLocator):
    """Default method locator implementation.

    Allows to use multiple objects and free functions as united 'RPC service'.

    Allowed method formats (foo - function, obj - object instance):
      * foo
      * obj
      * {'func_name': foo, 'service_name': obj}
      * {'group_name': dict}

    Note:
      By default, only methods marked by :func:`method` decorator are exposed.

    .. automethod:: _decorate_method
    """

    def __init__(self, methods, collect_all=False, separator='.'):
        """Initialize a new object locator.

        Args:
          methods: Methods to expose, can be represented as an object, function,
              or a dictionary with objects and functions.
          collect_all: Allow to '.resolve' methods without 'method' decorator.
          separator: Method name separator to use.
        """
        object_tree = methods
        if not isinstance(methods, dict):
            object_tree = {'': methods}
        self._separator = separator
        self._methods = self._collect_methods(object_tree, collect_all)

    def resolve(self, method_name, call_type, connection):
        """Resolve implementation, see :class:`AbstractMethodLocator`."""
        return self._methods[method_name]

    def _decorate_method(self, method):
        """Decorate found method *(hook for subclasses)*."""
        return method

    def _collect_methods(self, root, collect_all):
        """Collect all methods from a simple tree. Non-recursive wrapper."""
        path = []
        result = {}
        visited = set()
        self._collect_methods_impl(root, path, visited, result, collect_all)
        return result

    def _collect_methods_impl(self, node, path, visited, result, collect_all):
        """Collect all methods from a simple tree. Recursive implementation."""
        if isinstance(node, dict):
            # Cycle detection is required only for dict branch.
            # (same ids for methods or services are allowed, e.g. aliases).
            if id(node) in visited:
                raise ValueError('method tree contains a cycle')
            visited.add(id(node))
            for key, value in node.items():
                new_path = (path + [key]) if key else path
                self._collect_methods_impl(
                    value, new_path, visited, result, collect_all
                )
        elif callable(node):
            full_name = self._separator.join(path)
            self._collect_method(full_name, node, result, collect_all)
        else:
            for name in dir(node):
                # Skip 'private' fields unconditionally.
                if name.startswith('_'):
                    continue
                attribute = getattr(node, name)
                # Skip properties and stuff.
                if not callable(attribute):
                    continue
                full_name = self._separator.join(
                    itertools.chain(path, (name,)))
                self._collect_method(full_name, attribute, result, collect_all)

    def _collect_method(self, name, method, result, collect_all):
        """Register single function as an RPC method if it is exposed."""
        exposed = getattr(method, EXPOSE_MARKER, False)
        if not collect_all and not exposed:
            return
        if name in result:
            raise ValueError('duplicate method name \'%s\'' % (name,))
        result[name] = self._decorate_method(method)
