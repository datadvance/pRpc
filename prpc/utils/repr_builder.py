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

import builtins
import io


class ReprBuilder(object):
    """Simple utility to build string reprs for objects.

    Motivation: __repr__ output is used directly in debug logs, so
    avoiding writing some lengthy string arguments may be important.

    Allows nice chain syntax, e.g.::

      (utils.ReprBuilder(self)
        .add_value('method', self.method)
        .add_value('call_type', self.CALL_TYPE)
        .add_iterable('args', self.args)
        .add_mapping('kwargs', self.kwargs)
        .format(module_name=False))

    Args:
        obj: Object to build repr to.
    """
    PLACEHOLDER = '[...]'

    def __init__(self, obj):
        self._object = obj
        self._elements = []

    def add_value(self, name, value, max_len=36):
        """Add key-value pair to object description."""
        self._elements.append(
            '{:s}={:s}'.format(name, self._shorten(repr(value), max_len))
        )
        return self

    def add_iterable(self, name, value, max_element_len=16, max_total_len=256):
        """Add key-iterable pair to object description."""
        elements = (
            self._shorten(repr(el), max_element_len) for el in value
        )
        formatted = '%s=(%s)' % (name, ', '.join(elements))
        self._elements.append(self._shorten(formatted, max_total_len))
        return self

    def add_mapping(self, name, value, max_key_len=16,
                    max_element_len=16, max_total_len=512):
        """Add key-mapping pair to object description."""
        elements = (
            ': '.join(
                (
                    self._shorten(repr(key), max_key_len),
                    self._shorten(repr(val), max_element_len)
                )
            ) for (key, val) in value.items()
        )
        formatted = '%s={%s}' % (name, ', '.join(elements))
        self._elements.append(self._shorten(formatted, max_total_len))
        return self

    def format(self, id=False, module_name=False):
        """Format the final result.

        Args:
            id (bool): Toggle adding object id to the repr.
            module_name (bool): Toggle appending full module name
                to the type name.

        Returns:
            Formatted repr string.
        """
        obj_type = type(self._object)
        buffer = io.StringIO()
        buffer.write('<')
        if module_name and hasattr(obj_type, '__module__'):
            buffer.write(obj_type.__module__)
            buffer.write('.')
        buffer.write(obj_type.__qualname__)
        if id:
            buffer.write(' at 0x{:x}'.format(builtins.id(self._object)))
        if self._elements:
            buffer.write(' (')
            for idx, el in enumerate(self._elements):
                buffer.write(el)
                if idx != (len(self._elements) - 1):
                    buffer.write(', ')
            buffer.write(')')
        buffer.write('>')
        return buffer.getvalue()

    def _shorten(self, value, max_len):
        """Aux - shorten the string if maxlen is given."""
        assert isinstance(value, str)
        if max_len is None or len(value) <= max_len:
            return value
        # Cannot use textwrap.shorten there, as it just replaces long words
        # with a placeholder, making things like uids unreadable.
        return self.PLACEHOLDER.join(
          (value[:max_len // 2], value[-max_len // 2:])
        )
